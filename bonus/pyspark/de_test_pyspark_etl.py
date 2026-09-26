#!/usr/bin/env python3
"""PySpark version of the assignment ETL.

This is provided as the production-scale transformation artifact. The default
Python pipeline remains the local entrypoint so reviewers can run without a
Spark cluster, but this script applies the same bronze, silver, and gold
contract with Spark DataFrames.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import uuid

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "main"))
from task_1_data_cleaner import load_state, parse_args as pipeline_args, pipeline_lock, save_state, sha256_file


CLIENT_FILE = "clientparameters.csv"
TRANSACTION_FILES = (
    "playertransactions_2026_03.csv",
    "playertransactions_2026_04.csv",
)


def read_pipe_csv(spark: SparkSession, path: Path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("sep", "|")
        .option("mode", "FAILFAST")
        .csv(str(path))
    )


def parse_timestamp(column: str) -> F.Column:
    return F.coalesce(
        F.to_timestamp(F.col(column), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col(column), "dd/MM/yyyy HH:mm:ss"),
    )


def clean_amount(column: str) -> F.Column:
    cleaned = F.regexp_replace(F.trim(F.col(column)), r"[^0-9,.\-]", "")
    has_comma = F.instr(cleaned, ",") > 0
    has_dot = F.instr(cleaned, ".") > 0
    decimal_text = (
        F.when(has_comma & has_dot, F.regexp_replace(F.regexp_replace(cleaned, r"\.", ""), ",", "."))
        .when(has_comma, F.regexp_replace(cleaned, ",", "."))
        .otherwise(cleaned)
    )
    return decimal_text.cast(T.DecimalType(38, 9))


def clean_clientparameters(frame: DataFrame) -> DataFrame:
    return frame.select(
        F.col("CODE").cast("long").alias("CODE"),
        F.lower(F.trim(F.col("CLIENTTYPE"))).alias("CLIENTTYPE"),
        F.lower(F.trim(F.col("CLIENTPLATFORM"))).alias("CLIENTPLATFORM"),
        F.col("NOTREAL").cast("long").alias("NOTREAL"),
        parse_timestamp("LASTMODIFICATIONDATE").alias("LASTMODIFICATIONDATE"),
    )


def clean_transactions(frame: DataFrame) -> DataFrame:
    cleaned = frame.select(
        F.col("CODE").cast("long").alias("CODE"),
        F.col("PLAYERCODE").cast("long").alias("PLAYERCODE"),
        parse_timestamp("REQUESTDATE").alias("REQUESTDATE"),
        parse_timestamp("ACCEPTDATE").alias("ACCEPTDATE"),
        clean_amount("AMOUNT").alias("AMOUNT"),
        F.lower(F.trim(F.col("TYPE"))).alias("TYPE"),
        F.lower(F.trim(F.col("STATUS"))).alias("STATUS"),
        F.trim(F.col("PAYMENTMETHOD")).alias("PAYMENTMETHOD"),
        F.trim(F.col("PAYMENTSUBMETHOD")).alias("PAYMENTSUBMETHOD"),
        F.trim(F.col("COUNTRY")).alias("COUNTRY"),
        F.col("CLIENTPARAMETERCODE").cast("long").alias("CLIENTPARAMETERCODE"),
        F.col("FIRSTDEPOSIT").cast("long").alias("FIRSTDEPOSIT"),
        parse_timestamp("LASTUPDATEDATE").alias("LASTUPDATEDATE"),
        parse_timestamp("INGESTION_TIMESTAMP").alias("INGESTION_TIMESTAMP"),
    )

    latest_record = Window.partitionBy("CODE").orderBy(
        F.col("LASTUPDATEDATE").desc(),
        F.col("INGESTION_TIMESTAMP").desc(),
    )
    return (
        cleaned.withColumn("row_num", F.row_number().over(latest_record))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


def write_single_parquet(frame: DataFrame, path: Path) -> None:
    print(f"[spark] Writing immutable snapshot {path}", flush=True)
    frame.coalesce(1).write.mode("errorifexists").parquet(str(path))


def latest(frame: DataFrame, columns: list[str]) -> DataFrame:
    window = Window.partitionBy("CODE").orderBy(*[F.col(column).desc() for column in columns])
    return frame.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")


def run_incremental(args) -> int:
    root = args.processing_dir.resolve() / args.env / "spark"
    state_path = root / "_state/checkpoint.json"
    state = load_state(state_path)
    sources = {path.name: path for path in args.source_dir.glob("playertransactions_*.csv")}
    sources[CLIENT_FILE] = args.source_dir / CLIENT_FILE
    for name in (CLIENT_FILE, *TRANSACTION_FILES):
        if name not in sources or not sources[name].is_file():
            raise ValueError(f"Required source is missing: {name}")
    hashes = {name: sha256_file(path) for name, path in sources.items()}
    pending = []
    for name, digest in hashes.items():
        old = state["files"].get(name, {})
        if (old.get("sha256") != digest
                or not (Path(old.get("silver", "")) / "_SUCCESS").is_file()
                or not (Path(old.get("bronze", "")) / "_SUCCESS").is_file()):
            pending.append(name)
    gold_exists = all((Path(state.get(key, "")) / "_SUCCESS").is_file()
                      for key in ("gold_dimension", "gold_fact"))
    if not pending and gold_exists:
        print("[spark] No new/changed batches; all snapshots and audit times retained.", flush=True)
        return 0

    run_root = root / "runs" / uuid.uuid4().hex
    spark = SparkSession.builder.appName("de-test-pyspark-etl").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        for name in pending:
            print(f"[spark] Processing changed batch {name}", flush=True)
            raw = read_pipe_csv(spark, sources[name])
            bronze = run_root / "bronze" / Path(name).stem
            silver = run_root / "silver" / Path(name).stem
            old = state["files"].get(name, {})
            previous_silver = Path(old.get("silver", ""))
            delta = raw
            if (name != CLIENT_FILE and (previous_silver / "_SUCCESS").is_file()
                    and (Path(old.get("bronze", "")) / "_SUCCESS").is_file()):
                previous_raw = spark.read.parquet(old["bronze"])
                # exceptAll compares nulls as equal and handles duplicate deliveries.
                delta = raw.exceptAll(previous_raw.select(raw.columns))
            cleaned = clean_clientparameters(delta) if name == CLIENT_FILE else clean_transactions(delta)
            if (previous_silver / "_SUCCESS").is_file():
                cleaned = spark.read.parquet(str(previous_silver)).unionByName(cleaned)
            cleaned = latest(cleaned, ["LASTMODIFICATIONDATE"] if name == CLIENT_FILE
                             else ["LASTUPDATEDATE", "INGESTION_TIMESTAMP"])
            write_single_parquet(raw, bronze)
            write_single_parquet(cleaned, silver)
            state["files"][name] = {"sha256": hashes[name], "bronze": str(bronze), "silver": str(silver)}

        clients = spark.read.parquet(state["files"][CLIENT_FILE]["silver"])
        frames = [spark.read.parquet(entry["silver"]) for name, entry in state["files"].items()
                  if name != CLIENT_FILE]
        transactions = frames[0]
        for frame in frames[1:]:
            transactions = transactions.unionByName(frame)
        current = latest(transactions, ["LASTUPDATEDATE", "INGESTION_TIMESTAMP"])
        old_fact = Path(state.get("gold_fact", ""))
        if (old_fact / "_SUCCESS").is_file():
            previous = spark.read.parquet(str(old_fact))
            current = latest(previous.select(current.columns).unionByName(current),
                             ["LASTUPDATEDATE", "INGESTION_TIMESTAMP"])
            unchanged = F.lit(True)
            for column in ("STATUS", "AMOUNT", "ACCEPTDATE", "LASTUPDATEDATE"):
                unchanged = unchanged & F.col(f"new.{column}").eqNullSafe(F.col(f"old.{column}"))
            current = current.alias("new").join(previous.alias("old"), "CODE", "left").select(
                "new.*",
                F.coalesce(F.col("old.DWH_CREATE_TIME"), F.current_timestamp()).alias("DWH_CREATE_TIME"),
                F.when(unchanged, F.col("old.DWH_UPDATE_TIME")).otherwise(F.current_timestamp()).alias("DWH_UPDATE_TIME"),
            )
        else:
            current = current.withColumn("DWH_CREATE_TIME", F.current_timestamp()).withColumn("DWH_UPDATE_TIME", F.current_timestamp())
        dimension_path = run_root / "gold/dim_clientparameters"
        fact_path = run_root / "gold/fact_playertransactions"
        if CLIENT_FILE in pending or not gold_exists:
            write_single_parquet(clients, dimension_path)
        else:
            dimension_path = Path(state["gold_dimension"])
        if any(name != CLIENT_FILE for name in pending) or not gold_exists:
            write_single_parquet(current, fact_path)
        else:
            fact_path = old_fact
        state.update(gold_dimension=str(dimension_path), gold_fact=str(fact_path))
        save_state(state_path, state)
        print(json.dumps({"status": "success", "files_processed": len(pending),
                          "checkpoint": str(state_path), "gold": str(run_root / "gold")}), flush=True)
        return 0
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the DE test PySpark ETL artifact.")
    parser.add_argument("--source-dir", type=Path, default=Path("main/data"))
    parser.add_argument("--processing-dir", type=Path, default=Path("main/processing"))
    parser.add_argument("--env", default="prod")
    args = parser.parse_args()

    config = pipeline_args(["--processing-dir", str(args.processing_dir), "--env", args.env])
    with pipeline_lock(config):
        return run_incremental(args)


if __name__ == "__main__":
    raise SystemExit(main())
