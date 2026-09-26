#!/usr/bin/env python3
"""Load pending Parquet batches and commit the warehouse checkpoint atomically."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "main"))
from task_1_data_cleaner import (  # noqa: E402
    PipelineError, load_env_files, load_state, output_family, parse_args,
    pipeline_lock, run_pipeline_locked, save_state, sha256_file,
)


def run_command(command: list[str], *, input_text: str | None = None) -> str:
    done = threading.Event()

    def heartbeat() -> None:
        while not done.wait(15):
            print(f"[running] Waiting for {command[0]} to finish...", flush=True)

    worker = threading.Thread(target=heartbeat, daemon=True)
    worker.start()
    try:
        result = subprocess.run(command, input=input_text, text=True, stdout=subprocess.PIPE, check=True)
        return result.stdout
    except subprocess.CalledProcessError as exc:
        if exc.stdout:
            print(exc.stdout, file=sys.stderr, flush=True)
        raise
    finally:
        done.set()
        worker.join()


def settings() -> tuple[str, str, str, str]:
    project = os.environ.get("PROJECT_ID", "")
    dataset = os.environ.get("DATASET", "de_test_dataset")
    region = os.environ.get("REGION", "EU")
    bucket = os.environ.get("GCS_BUCKET") or os.environ.get("BUCKET", "")
    for value, pattern, name in (
        (project, r"[a-z][a-z0-9-]{4,61}[a-z0-9]", "PROJECT_ID"),
        (dataset, r"[A-Za-z_][A-Za-z0-9_]*", "DATASET"),
        (bucket, r"[a-z0-9][a-z0-9._-]+[a-z0-9]", "GCS_BUCKET"),
        (region, r"[A-Za-z0-9-]+", "REGION"),
    ):
        if not re.fullmatch(pattern, value):
            raise PipelineError(f"Set a valid {name} before running the GCP pipeline")
    return project, dataset, region, bucket


def render_sql(sql: str, project: str, dataset: str, bucket: str) -> str:
    return (sql.replace("de-test-project", project)
            .replace("de_test_dataset", dataset).replace("de-test-bucket", bucket))


def assignment_merge() -> str:
    sql = (ROOT / "main/task_2_data_ddl_dml.sql").read_text()
    return sql[sql.index("MERGE `"):]


def pending_batches(files: list[dict], ledger: list[dict]) -> list[dict]:
    loaded = {(row["uri"], row["sha256"]) for row in ledger}
    return [row for row in files if (row["uri"], row["sha256"]) not in loaded]


def load_sql(pending: list[dict], project: str, dataset: str, bucket: str, token: str) -> str:
    statements = []
    cleanup = []
    for family, target, temporary in (
        ("clientparameters", "clientparameters", "client_batch"),
        ("playertransactions", "playertransactions_raw", "transaction_batch"),
    ):
        uris = [row["batch_uri"] for row in pending if row["family"] == family]
        if uris:
            external = f"`{project}.{dataset}._batch_{family}_{token}`"
            statements.append(
                f"CREATE EXTERNAL TABLE {external} OPTIONS(format='PARQUET', "
                f"uris={json.dumps(uris)}, expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY));"
            )
            statements.append(f"CREATE TEMP TABLE {temporary} AS SELECT * FROM {external};")
            cleanup.append(f"DROP TABLE IF EXISTS {external};")
        else:
            statements.append(f"CREATE TEMP TABLE {temporary} AS SELECT * FROM `{project}.{dataset}.{target}` WHERE FALSE;")

    # Automatic runs cover actual pending data; hourly replays use the MERGE stage.
    merge = assignment_merge().replace(
        "@data_interval_start", "(SELECT MIN(INGESTION_TIMESTAMP) FROM transaction_batch)"
    ).replace(
        "@data_interval_end", "(SELECT TIMESTAMP_ADD(MAX(INGESTION_TIMESTAMP), INTERVAL 1 MICROSECOND) FROM transaction_batch)"
    )
    transaction = (ROOT / "bonus/bigquery_load_from_gcs.sql").read_text().replace(
        "-- MERGE_CURATED: the driver inserts the assignment's parameterized MERGE here.", merge
    )
    statements.append(render_sql(transaction, project, dataset, bucket))
    statements.extend(cleanup)
    return "\n".join(statements)


def load() -> None:
    project, dataset, region, bucket = settings()
    config = parse_args([])
    query = ["bq", f"--project_id={project}", f"--location={region}", "query", "--use_legacy_sql=false"]
    run_command(query, input_text=f"""
CREATE TABLE IF NOT EXISTS `{project}.{dataset}._pipeline_batches` (
  uri STRING NOT NULL, sha256 STRING NOT NULL, loaded_at TIMESTAMP NOT NULL
);""")
    ledger = json.loads(run_command(
        query + ["--format=json", "--max_rows=1000000"],
        input_text=f"SELECT uri, sha256 FROM `{project}.{dataset}._pipeline_batches`",
    ))
    uploads_path = config.root / "_state" / "gcs_uploads.json"
    uploads = load_state(uploads_path)
    files = []
    for path in sorted((config.root / "silver").glob("*.parquet")):
        if path.name != "clientparameters.parquet" and not path.name.startswith("playertransactions_"):
            continue
        family = output_family(path.name)
        uri = f"gs://{bucket}/{family}/{path.name}"
        digest = sha256_file(path)
        if uploads.get(uri) != digest:
            raise PipelineError(f"Upload is missing or stale for {uri}; run the upload stage first")
        files.append({"uri": uri, "sha256": digest, "family": family, "path": str(path),
                      "batch_uri": f"gs://{bucket}/_batches/{digest}/{path.name}"})
    if not files:
        raise PipelineError("No silver files found; run the upload stage first")
    pending = pending_batches(files, ledger)
    print(f"[warehouse] {len(pending)} new/changed batches; {len(files) - len(pending)} already loaded.", flush=True)
    if not pending:
        return
    for row in pending:
        uri = row["batch_uri"]
        if uploads.get(uri) != row["sha256"]:
            print(f"[warehouse] Preserving immutable batch {uri}", flush=True)
            run_command(["gcloud", "storage", "cp", "--no-clobber", row["path"], uri])
            uploads[uri] = row["sha256"]
            save_state(uploads_path, uploads)
    manifest = json.dumps([{key: row[key] for key in ("uri", "sha256")} for row in pending])
    sql = load_sql(pending, project, dataset, bucket, uuid.uuid4().hex)
    print("[warehouse] Committing raw rows, curated upserts, and batch checkpoints together.", flush=True)
    print(run_command(query + [f"--parameter=batch_manifest:JSON:{manifest}"], input_text=sql), flush=True)


def reset() -> None:
    project, dataset, region, bucket = settings()
    print(f"[reset] Removing pipeline-owned objects from gs://{bucket} and tables in {project}.{dataset}.", flush=True)
    listing = json.loads(run_command(["gcloud", "storage", "objects", "list", f"gs://{bucket}/**", "--format=json"]))
    owned = re.compile(
        r"(?:(?:bronze/|silver/)?(?:clientparameters/clientparameters|playertransactions/playertransactions_[^/]+)\.parquet"
        r"|gold/(?:clientparameters/(?:clientparameters|dim_clientparameters)|playertransactions/(?:playertransactions|fact_playertransactions))\.parquet"
        r"|_batches/[a-f0-9]{64}/(?:clientparameters|playertransactions_[^/]+)\.parquet)$"
    )
    names = [obj["name"] for obj in listing if owned.fullmatch(obj["name"])]
    for name in names:
        print(f"[reset] Removing {name}", flush=True)
        run_command(["gcloud", "storage", "rm", f"gs://{bucket}/{name}"])
    tables = [f"{dataset}.{name}" for name in (
        "clientparameters", "playertransactions_raw", "playertransactions", "_pipeline_batches",
        "clientparameters_load_ext", "playertransactions_raw_load_ext",
    )]
    layer_prefix = "de_test" if dataset == "de_test_dataset" else dataset
    for layer in ("bronze", "silver", "gold"):
        tables.extend(f"{layer_prefix}_{layer}.{name}" for name in (
            ("dim_clientparameters", "fact_playertransactions") if layer == "gold"
            else ("clientparameters", "playertransactions")
        ))
    sql = "\n".join(f"DROP TABLE IF EXISTS `{project}.{table}`;" for table in tables)
    run_command(["bq", f"--project_id={project}", f"--location={region}", "query", "--use_legacy_sql=false"], input_text=sql)
    # Keep local checkpoints until cloud cleanup succeeds, allowing reset to retry.
    print(run_pipeline_locked(parse_args(["--reset-processing-dir", "--reset-only"])), flush=True)
    print("[reset] Complete. Source CSVs, bucket, datasets, and unrelated objects were retained.", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("load", "reset"))
    args = parser.parse_args()
    load_env_files(ROOT, Path(os.environ["ENV_FILE"]) if os.environ.get("ENV_FILE") else None)
    try:
        with pipeline_lock(parse_args([])):
            {"load": load, "reset": reset}[args.command]()
        return 0
    except (PipelineError, subprocess.CalledProcessError) as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
