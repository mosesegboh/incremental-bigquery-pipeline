#!/usr/bin/env python3
"""Clean and land the assignment CSV files as Parquet.

By default the script runs the production-style path: bronze/silver/gold file
layers are written, dimension/fact gold aliases are produced, production
artifacts are surfaced in the run report, and configured external integrations
such as GCS or Kafka are attempted. Use --no-production-layers for the faster
offline reviewer path.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DEFAULT_SOURCE_DIR = BASE_DIR / "data"
DEFAULT_PROCESSING_DIR = BASE_DIR / "processing"

CLIENT_FILE = "clientparameters.csv"
TRANSACTION_FILES = (
    "playertransactions_2026_03.csv",
    "playertransactions_2026_04.csv",
)
EXPECTED_FILES = (CLIENT_FILE, *TRANSACTION_FILES)

CLIENT_COLUMNS = (
    "CODE",
    "CLIENTTYPE",
    "CLIENTPLATFORM",
    "NOTREAL",
    "LASTMODIFICATIONDATE",
)

TRANSACTION_COLUMNS = (
    "CODE",
    "PLAYERCODE",
    "REQUESTDATE",
    "ACCEPTDATE",
    "AMOUNT",
    "TYPE",
    "STATUS",
    "PAYMENTMETHOD",
    "PAYMENTSUBMETHOD",
    "COUNTRY",
    "CLIENTPARAMETERCODE",
    "FIRSTDEPOSIT",
    "LASTUPDATEDATE",
    "INGESTION_TIMESTAMP",
)

CLIENT_REQUIRED = tuple(CLIENT_COLUMNS)
TRANSACTION_REQUIRED = tuple(
    c for c in TRANSACTION_COLUMNS if c not in {"ACCEPTDATE", "PAYMENTSUBMETHOD", "COUNTRY"}
)

INT_COLUMNS = {
    CLIENT_FILE: ("CODE", "NOTREAL"),
    "transactions": (
        "CODE",
        "PLAYERCODE",
        "CLIENTPARAMETERCODE",
        "FIRSTDEPOSIT",
    ),
}

VALID_TRANSACTION_TYPES = {"deposit", "withdraw"}
VALID_TRANSACTION_STATUSES = {"approved", "waiting", "declined", "issued"}
TRUE_VALUES = {"1", "true", "yes", "y", "on"}
FALSE_VALUES = {"0", "false", "no", "n", "off"}


class PipelineError(Exception):
    """Base exception for controlled pipeline failures."""


class DataQualityError(PipelineError):
    """Raised when source data violates the documented contract."""


@dataclass(frozen=True)
class PipelineConfig:
    source_dir: Path
    processing_dir: Path
    env: str
    force: bool
    reset_processing_dir: bool
    reset_only: bool
    schema_policy: str
    data_interval_start: pd.Timestamp | None
    data_interval_end: pd.Timestamp | None
    gcs_bucket: str | None
    no_production_layers: bool
    no_kafka: bool
    no_spark: bool
    no_s3: bool
    no_airflow: bool
    no_databricks: bool

    @property
    def root(self) -> Path:
        return self.processing_dir / self.env


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_env_file(path: Path) -> dict[str, str]:
    """Parse a small POSIX-style .env file without requiring shell execution."""
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            raise PipelineError(f"Invalid .env line in {path}:{line_number}: {raw_line!r}")

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            raise PipelineError(f"Invalid .env key in {path}:{line_number}: {key!r}")

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].strip()
        values[key] = value

    return values


def load_env_files(project_root: Path, env_file: Path | None = None) -> dict[str, str]:
    """Load .env-style config while preserving real environment precedence."""
    original_env_keys = set(os.environ)
    candidate_paths = [env_file] if env_file else [project_root / ".env", project_root / ".env.local"]
    loaded: dict[str, str] = {}

    for path in candidate_paths:
        if path is None:
            continue
        resolved_path = path if path.is_absolute() else project_root / path
        for key, value in parse_env_file(resolved_path).items():
            if key not in original_env_keys:
                os.environ[key] = value
                loaded[key] = value

    return loaded


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise PipelineError(
        f"Environment variable {name} must be one of "
        f"{sorted(TRUE_VALUES | FALSE_VALUES)}, got {value!r}"
    )


def env_path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    if value is None:
        return default

    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def utc_run_id() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%S%fZ")


def json_default(value: Any) -> str:
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def configure_logging(log_dir: Path, run_id: str) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("de_test_pipeline")
    logger.setLevel(logging.INFO)
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    formatter = logging.Formatter("%(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_dir / f"{run_id}.jsonl", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        "logged_at": utc_now().isoformat(),
        **fields,
    }
    logger.info(json.dumps(payload, default=json_default, sort_keys=True))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "files": {}}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True, default=json_default)
        handle.write("\n")
    tmp_path.replace(path)


def ensure_directories(root: Path) -> dict[str, Path]:
    directories = {
        "incoming": root / "incoming-data",
        "bronze": root / "bronze",
        "silver": root / "silver",
        "gold": root / "gold",
        "reports": root / "reports",
        "logs": root / "logs",
        "state": root / "_state",
    }
    for directory in directories.values():
        directory.mkdir(parents=True, exist_ok=True)
    return directories


def copy_source_files(source_dir: Path, incoming_dir: Path) -> dict[str, Path]:
    copied: dict[str, Path] = {}
    for file_name in EXPECTED_FILES:
        if not (source_dir / file_name).is_file():
            raise PipelineError(f"Required source file is missing: {source_dir / file_name}")
    file_names = [CLIENT_FILE, *sorted(path.name for path in source_dir.glob("playertransactions_*.csv"))]
    for file_name in file_names:
        source_path = source_dir / file_name
        if not source_path.exists():
            raise PipelineError(f"Required source file is missing: {source_path}")
        target_path = incoming_dir / file_name
        if not target_path.exists() or sha256_file(source_path) != sha256_file(target_path):
            temporary = target_path.with_suffix(".tmp")
            shutil.copy2(source_path, temporary)
            temporary.replace(target_path)
        copied[file_name] = target_path
    return copied


def read_pipe_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep="|", dtype=str, keep_default_na=False)


def validate_columns(
    frame: pd.DataFrame,
    expected_columns: tuple[str, ...],
    file_name: str,
    schema_policy: str,
) -> None:
    actual = tuple(frame.columns)
    missing = [column for column in expected_columns if column not in actual]
    extras = [column for column in actual if column not in expected_columns]

    if missing:
        raise DataQualityError(f"{file_name} is missing required columns: {missing}")
    if extras and schema_policy == "strict":
        raise DataQualityError(
            f"{file_name} has unexpected columns {extras}. "
            "Run with --schema-policy allow-additive to tolerate additive changes."
        )


def require_not_null(frame: pd.DataFrame, columns: tuple[str, ...], file_name: str) -> None:
    for column in columns:
        blank_mask = frame[column].astype(str).str.strip().eq("")
        if blank_mask.any():
            raise DataQualityError(
                f"{file_name}.{column} contains {int(blank_mask.sum())} blank values"
            )


def clean_int_column(frame: pd.DataFrame, column: str, file_name: str) -> None:
    series = frame[column].astype(str).str.strip()
    invalid = ~series.str.match(r"^-?\d+$")
    if invalid.any():
        examples = series[invalid].head(5).tolist()
        raise DataQualityError(
            f"{file_name}.{column} contains non-integer values: {examples}"
        )
    frame[column] = series.astype("int64")


def parse_timestamp_series(
    series: pd.Series,
    file_name: str,
    column: str,
    nullable: bool,
) -> pd.Series:
    text = series.astype(str).str.strip()
    empty = text.eq("")
    if empty.any() and not nullable:
        raise DataQualityError(
            f"{file_name}.{column} contains {int(empty.sum())} blank timestamps"
        )

    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    non_empty = ~empty
    # The input files contain both ISO timestamps and day-first slash dates.
    for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        unresolved = non_empty & parsed.isna()
        if not unresolved.any():
            break
        parsed.loc[unresolved] = pd.to_datetime(
            text[unresolved],
            format=fmt,
            errors="coerce",
        )

    invalid = non_empty & parsed.isna()
    if invalid.any():
        examples = text[invalid].head(5).tolist()
        raise DataQualityError(
            f"{file_name}.{column} contains unparseable timestamps: {examples}"
        )

    return parsed


def clean_amount_value(value: Any) -> Decimal:
    text = str(value).strip()
    if not text:
        raise DataQualityError("Amount is blank")

    normalized = re.sub(r"[^\d,.\-]", "", text)
    if not normalized:
        raise DataQualityError(f"Amount contains no numeric value: {value!r}")

    comma_index = normalized.rfind(",")
    dot_index = normalized.rfind(".")
    if comma_index >= 0 and dot_index >= 0:
        if comma_index > dot_index:
            normalized = normalized.replace(".", "").replace(",", ".")
        else:
            normalized = normalized.replace(",", "")
    elif comma_index >= 0:
        normalized = normalized.replace(",", ".")

    try:
        amount = Decimal(normalized)
    except InvalidOperation as exc:
        raise DataQualityError(f"Invalid amount value: {value!r}") from exc

    if amount < 0:
        raise DataQualityError(f"Negative amount is not expected: {value!r}")
    return amount


def clean_amount_column(frame: pd.DataFrame, file_name: str) -> None:
    cleaned: list[Decimal] = []
    for value in frame["AMOUNT"]:
        try:
            cleaned.append(clean_amount_value(value))
        except DataQualityError as exc:
            raise DataQualityError(f"{file_name}.AMOUNT failed validation: {exc}") from exc
    frame["AMOUNT"] = cleaned


def enforce_allowed_values(
    frame: pd.DataFrame,
    column: str,
    allowed_values: set[str],
    file_name: str,
) -> None:
    invalid = ~frame[column].isin(allowed_values)
    if invalid.any():
        examples = frame.loc[invalid, column].head(10).tolist()
        raise DataQualityError(
            f"{file_name}.{column} contains unsupported values: {examples}"
        )


def write_parquet_atomic(frame: pd.DataFrame, output_path: Path) -> None:
    if output_path.exists() and frames_equal(frame, load_existing_parquet(output_path)):
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(f".{output_path.name}.tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    frame.to_parquet(
        tmp_path,
        index=False,
        engine="pyarrow",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    tmp_path.replace(output_path)


def frames_equal(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    try:
        pd.testing.assert_frame_equal(
            left.reset_index(drop=True), right.reset_index(drop=True),
            check_dtype=False, check_exact=True,
        )
        return True
    except AssertionError:
        return False


def merge_clients(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    return (pd.concat([existing, incoming], ignore_index=True)
            .sort_values(["CODE", "LASTMODIFICATIONDATE"], kind="stable")
            .drop_duplicates("CODE", keep="last")
            .sort_values("CODE").reset_index(drop=True))


def should_process(
    state: dict[str, Any],
    file_name: str,
    source_hash: str,
    output_path: Path,
    force: bool,
) -> bool:
    if force or not output_path.exists():
        return True
    file_state = state.get("files", {}).get(file_name, {})
    return (file_state.get("sha256") != source_hash
            or file_state.get("silver_sha256") != sha256_file(output_path))


def clean_clientparameters(
    raw_frame: pd.DataFrame,
    file_name: str,
    schema_policy: str,
) -> pd.DataFrame:
    validate_columns(raw_frame, CLIENT_COLUMNS, file_name, schema_policy)
    frame = raw_frame.loc[:, CLIENT_COLUMNS].copy()
    require_not_null(frame, CLIENT_REQUIRED, file_name)

    for column in INT_COLUMNS[CLIENT_FILE]:
        clean_int_column(frame, column, file_name)

    frame["CLIENTTYPE"] = frame["CLIENTTYPE"].astype(str).str.strip().str.lower()
    frame["CLIENTPLATFORM"] = (
        frame["CLIENTPLATFORM"].astype(str).str.strip().str.lower()
    )
    frame["LASTMODIFICATIONDATE"] = parse_timestamp_series(
        frame["LASTMODIFICATIONDATE"],
        file_name,
        "LASTMODIFICATIONDATE",
        nullable=False,
    )

    if frame["CODE"].duplicated().any():
        duplicates = frame.loc[frame["CODE"].duplicated(), "CODE"].head(10).tolist()
        raise DataQualityError(f"{file_name}.CODE is not unique: {duplicates}")

    if not frame["NOTREAL"].isin({0, 1}).all():
        raise DataQualityError(f"{file_name}.NOTREAL must contain only 0 or 1")

    return frame


def clean_transactions(
    raw_frame: pd.DataFrame,
    file_name: str,
    schema_policy: str,
    data_interval_start: pd.Timestamp | None,
    data_interval_end: pd.Timestamp | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    validate_columns(raw_frame, TRANSACTION_COLUMNS, file_name, schema_policy)
    frame = raw_frame.loc[:, TRANSACTION_COLUMNS].copy()
    require_not_null(frame, TRANSACTION_REQUIRED, file_name)

    for column in INT_COLUMNS["transactions"]:
        clean_int_column(frame, column, file_name)

    for column, nullable in (
        ("REQUESTDATE", False),
        ("ACCEPTDATE", True),
        ("LASTUPDATEDATE", False),
        ("INGESTION_TIMESTAMP", False),
    ):
        frame[column] = parse_timestamp_series(frame[column], file_name, column, nullable)

    clean_amount_column(frame, file_name)

    frame["TYPE"] = frame["TYPE"].astype(str).str.strip().str.lower()
    frame["STATUS"] = frame["STATUS"].astype(str).str.strip().str.lower()
    frame["PAYMENTMETHOD"] = frame["PAYMENTMETHOD"].astype(str).str.strip()
    frame["PAYMENTSUBMETHOD"] = frame["PAYMENTSUBMETHOD"].astype(str).str.strip()
    frame["COUNTRY"] = frame["COUNTRY"].astype(str).str.strip()

    enforce_allowed_values(frame, "TYPE", VALID_TRANSACTION_TYPES, file_name)
    enforce_allowed_values(frame, "STATUS", VALID_TRANSACTION_STATUSES, file_name)

    if not frame["FIRSTDEPOSIT"].isin({0, 1}).all():
        raise DataQualityError(f"{file_name}.FIRSTDEPOSIT must contain only 0 or 1")

    before_interval = len(frame)
    if data_interval_start is not None:
        frame = frame.loc[frame["INGESTION_TIMESTAMP"] >= data_interval_start].copy()
    if data_interval_end is not None:
        frame = frame.loc[frame["INGESTION_TIMESTAMP"] < data_interval_end].copy()

    before_dedupe = len(frame)
    frame["_source_order"] = range(len(frame))
    frame = frame.sort_values(
        ["CODE", "LASTUPDATEDATE", "INGESTION_TIMESTAMP", "_source_order"],
        ascending=[True, False, False, False],
        kind="mergesort",
    )
    frame = frame.drop_duplicates(subset=["CODE"], keep="first")
    frame = frame.drop(columns=["_source_order"]).sort_values(["REQUESTDATE", "CODE"])
    frame = frame.reset_index(drop=True)

    metrics = {
        "rows_before_interval": before_interval,
        "rows_after_interval": before_dedupe,
        "duplicates_removed": before_dedupe - len(frame),
        "rows_written": len(frame),
    }
    return frame, metrics


def validate_referential_integrity(
    clientparameters: pd.DataFrame,
    transactions: list[pd.DataFrame],
) -> None:
    valid_codes = set(clientparameters["CODE"].tolist())
    referenced_codes: set[int] = set()
    for frame in transactions:
        referenced_codes.update(frame["CLIENTPARAMETERCODE"].dropna().tolist())

    missing = sorted(referenced_codes - valid_codes)
    if missing:
        raise DataQualityError(
            "Transactions reference unknown CLIENTPARAMETERCODE values: "
            f"{missing[:20]}"
        )


def build_gold_transactions(
    transactions: list[pd.DataFrame], existing: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if not transactions:
        return pd.DataFrame(columns=TRANSACTION_COLUMNS)

    frames = [frame.loc[:, TRANSACTION_COLUMNS] for frame in transactions]
    if existing is not None:
        frames.insert(0, existing.loc[:, TRANSACTION_COLUMNS])
    frames = [frame for frame in frames if not frame.empty] or frames[:1]
    combined = pd.concat(frames, ignore_index=True)

    combined["_source_order"] = range(len(combined))
    combined = combined.sort_values(
        ["CODE", "LASTUPDATEDATE", "INGESTION_TIMESTAMP", "_source_order"],
        ascending=[True, False, False, False],
        kind="mergesort",
    )
    combined = combined.drop_duplicates(subset=["CODE"], keep="first")
    combined = combined.drop(columns=["_source_order"]).sort_values(["REQUESTDATE", "CODE"])
    combined = combined.reset_index(drop=True)

    load_time = pd.Timestamp.utcnow().tz_localize(None)
    combined["DWH_CREATE_TIME"] = load_time
    combined["DWH_UPDATE_TIME"] = load_time
    if existing is not None and not existing.empty:
        previous = existing.set_index("CODE").reindex(combined["CODE"]).reset_index()
        matched = previous["DWH_CREATE_TIME"].notna()
        same = pd.Series(True, index=combined.index)
        for column in TRANSACTION_COLUMNS:
            same &= (combined[column].eq(previous[column])
                     | (combined[column].isna() & previous[column].isna())).fillna(False)
        combined.loc[matched, "DWH_CREATE_TIME"] = previous.loc[matched, "DWH_CREATE_TIME"]
        combined.loc[matched & same, "DWH_UPDATE_TIME"] = previous.loc[matched & same, "DWH_UPDATE_TIME"]
    return combined


def load_existing_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise PipelineError(f"Expected incremental output does not exist: {path}")
    return pd.read_parquet(path, engine="pyarrow")


def update_file_state(
    state: dict[str, Any],
    file_name: str,
    source_hash: str,
    source_path: Path,
    output_path: Path,
    rows_written: int,
) -> None:
    state.setdefault("files", {})[file_name] = {
        "sha256": source_hash,
        "source_path": str(source_path),
        "output_path": str(output_path),
        "rows_written": rows_written,
        "processed_at": utc_now().isoformat(),
    }


def output_family(file_name: str) -> str:
    return "clientparameters" if "clientparameters" in file_name else "playertransactions"


def maybe_upload_to_gcs(
    bucket_name: str | None,
    no_s3: bool,
    no_production_layers: bool,
    bronze_outputs: dict[str, Path],
    silver_outputs: dict[str, Path],
    gold_outputs: dict[str, Path],
    logger: logging.Logger,
    state_path: Path | None = None,
) -> dict[str, Any]:
    if no_s3:
        reason = (
            "--no-production-layers was provided"
            if no_production_layers
            else "--no-s3 was provided"
        )
        log_event(logger, "object_store_upload_skipped", reason=reason)
        return {"enabled": False, "reason": reason}

    if not bucket_name:
        log_event(logger, "object_store_upload_skipped", reason="no GCS bucket configured")
        return {"enabled": False, "reason": "no GCS bucket configured"}

    upload_plan: list[tuple[Path, str, str]] = []

    # Assignment-compatible paths used by bonus/bigquery_load_from_gcs.sql.
    for file_name, output_path in silver_outputs.items():
        folder = output_family(file_name)
        upload_plan.append((output_path, f"{folder}/{output_path.name}", "assignment"))

    if not no_production_layers:
        for layer_name, outputs in (("bronze", bronze_outputs), ("silver", silver_outputs)):
            for file_name, output_path in outputs.items():
                folder = output_family(file_name)
                blob_name = f"{layer_name}/{folder}/{output_path.name}"
                upload_plan.append((output_path, blob_name, layer_name))

        for output_name, output_path in gold_outputs.items():
            folder = output_family(output_name)
            blob_name = f"gold/{folder}/{output_path.name}"
            upload_plan.append((output_path, blob_name, "gold"))

    upload_state = load_state(state_path) if state_path else {}
    fingerprints = {blob: sha256_file(path) for path, blob, _ in upload_plan}
    total_planned = len(upload_plan)
    upload_plan = [item for item in upload_plan
                   if upload_state.get(f"gs://{bucket_name}/{item[1]}") != fingerprints[item[1]]]
    skipped = total_planned - len(upload_plan)

    def checkpoint(blob_name: str) -> None:
        upload_state[f"gs://{bucket_name}/{blob_name}"] = fingerprints[blob_name]
        if state_path:
            save_state(state_path, upload_state)

    if not upload_plan:
        log_event(logger, "gcs_upload_skipped", reason="all objects already uploaded", files_skipped=skipped)
        return {"enabled": True, "bucket": bucket_name, "uploaded": [], "files_skipped": skipped}

    log_event(
        logger,
        "gcs_upload_start",
        bucket=bucket_name,
        files_planned=len(upload_plan),
        production_layers=not no_production_layers,
    )

    def upload_with_gcloud() -> dict[str, Any]:
        if shutil.which("gcloud") is None:
            raise PipelineError(
                "GCS upload requires either Google Application Default Credentials "
                "or an authenticated gcloud CLI. Run `gcloud auth application-default login`, "
                "or use `--no-production-layers` for a local-only run."
            )

        log_event(logger, "gcs_upload_fallback", mode="gcloud storage cp")
        uploaded: list[dict[str, str]] = []
        seen_blobs: set[str] = set()
        for output_path, blob_name, layer_name in upload_plan:
            if blob_name in seen_blobs:
                continue
            seen_blobs.add(blob_name)
            target = f"gs://{bucket_name}/{blob_name}"
            try:
                subprocess.run(
                    ["gcloud", "storage", "cp", str(output_path), target],
                    check=True,
                )
            except subprocess.CalledProcessError as exc:
                raise PipelineError(f"gcloud upload failed for {target}") from exc
            checkpoint(blob_name)
            uploaded.append({"layer": layer_name, "source": str(output_path), "target": target})
            log_event(
                logger,
                "gcs_upload_complete",
                layer=layer_name,
                source=str(output_path),
                target=target,
            )

        return {
            "enabled": True,
            "bucket": bucket_name,
            "mode": "gcloud",
            "production_layers": not no_production_layers,
            "uploaded": uploaded,
            "files_skipped": skipped,
        }

    try:
        from google.cloud import storage  # type: ignore

        client = storage.Client()
        bucket = client.bucket(bucket_name)
    except Exception as exc:
        log_event(
            logger,
            "gcs_upload_python_client_unavailable",
            fallback="gcloud",
            reason=str(exc),
        )
        result = upload_with_gcloud()
        log_event(logger, "gcs_upload_success", bucket=bucket_name, files_uploaded=len(result["uploaded"]))
        return result

    uploaded: list[dict[str, str]] = []
    seen_blobs: set[str] = set()
    for output_path, blob_name, layer_name in upload_plan:
        if blob_name in seen_blobs:
            continue
        seen_blobs.add(blob_name)
        bucket.blob(blob_name).upload_from_filename(str(output_path))
        checkpoint(blob_name)
        target = f"gs://{bucket_name}/{blob_name}"
        uploaded.append({"layer": layer_name, "source": str(output_path), "target": target})
        log_event(logger, "gcs_upload_complete", layer=layer_name, source=str(output_path), target=target)

    result = {
        "enabled": True,
        "bucket": bucket_name,
        "mode": "python-client",
        "production_layers": not no_production_layers,
        "uploaded": uploaded,
        "files_skipped": skipped,
    }
    log_event(logger, "gcs_upload_success", bucket=bucket_name, files_uploaded=len(uploaded))
    return result


def maybe_publish_kafka_event(
    no_kafka: bool,
    no_production_layers: bool,
    report: dict[str, Any],
    logger: logging.Logger,
) -> dict[str, Any]:
    if no_kafka:
        reason = (
            "--no-production-layers was provided"
            if no_production_layers
            else "--no-kafka was provided"
        )
        log_event(logger, "kafka_publish_skipped", reason=reason)
        return {"enabled": False, "reason": reason}

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.getenv("KAFKA_TOPIC")
    if not bootstrap_servers or not topic:
        log_event(logger, "kafka_publish_skipped", reason="Kafka environment not configured")
        return {"enabled": False, "reason": "Kafka environment not configured"}

    try:
        from confluent_kafka import Producer  # type: ignore
    except ImportError as exc:
        raise PipelineError("confluent-kafka is required for Kafka publishing") from exc

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    producer.produce(topic, json.dumps(report, default=json_default).encode("utf-8"))
    producer.flush(10)
    log_event(logger, "kafka_publish_complete", topic=topic)
    return {"enabled": True, "topic": topic}


def describe_external_runtime_flags(config: PipelineConfig, logger: logging.Logger) -> dict[str, Any]:
    if config.no_production_layers:
        reason = "--no-production-layers was provided"
        runtimes = {
            "spark": {"enabled": False, "mode": reason},
            "airflow": {"enabled": False, "mode": reason},
            "databricks": {"enabled": False, "mode": reason},
        }
        log_event(logger, "external_runtime_flags", runtimes=runtimes)
        return runtimes

    runtimes = {
        "spark": {
            "enabled": not config.no_spark,
            "mode": "production transformation artifact is available in bonus/pyspark",
        },
        "airflow": {
            "enabled": not config.no_airflow,
            "mode": "hourly orchestration DAG is provided in bonus/airflow",
        },
        "databricks": {
            "enabled": not config.no_databricks,
            "mode": "Delta Lake DDL/DML artifact is provided in bonus/warehouse_alternatives",
        },
    }
    log_event(logger, "external_runtime_flags", runtimes=runtimes)
    return runtimes


def describe_production_layers(
    config: PipelineConfig,
    bronze_outputs: dict[str, Path],
    silver_outputs: dict[str, Path],
    gold_outputs: dict[str, Path],
) -> dict[str, Any]:
    if config.no_production_layers:
        return {
            "enabled": False,
            "reason": "--no-production-layers was provided",
        }

    return {
        "enabled": True,
        "file_layers": {
            "bronze": bronze_outputs,
            "silver": silver_outputs,
            "gold": gold_outputs,
        },
        "warehouse_model": {
            "dimensions": {
                "dim_clientparameters": gold_outputs["dim_clientparameters"],
            },
            "facts": {
                "fact_playertransactions": gold_outputs["fact_playertransactions"],
            },
        },
        "provisioning_artifacts": {
            "bigquery_core": PROJECT_ROOT / "main" / "task_2_data_ddl_dml.sql",
            "bigquery_bronze_silver_gold": PROJECT_ROOT / "bonus" / "bigquery_production_layers.sql",
            "bigquery_load_from_gcs": PROJECT_ROOT / "bonus" / "bigquery_load_from_gcs.sql",
            "airflow_dag": PROJECT_ROOT / "bonus" / "airflow" / "de_test_incremental_pipeline.py",
            "pyspark_etl": PROJECT_ROOT / "bonus" / "pyspark" / "de_test_pyspark_etl.py",
            "databricks_delta": PROJECT_ROOT
            / "bonus"
            / "warehouse_alternatives"
            / "databricks_delta_ddl_dml.sql",
        },
    }


@contextmanager
def pipeline_lock(config: PipelineConfig):
    config.processing_dir.mkdir(parents=True, exist_ok=True)
    lock_path = config.processing_dir / f".{config.env}.lock"
    inherited = os.environ.get("PIPELINE_LOCK_FD")
    inherited_matches = False
    if inherited:
        try:
            stat = os.fstat(int(inherited))
            expected = lock_path.stat()
            inherited_matches = (stat.st_dev, stat.st_ino) == (expected.st_dev, expected.st_ino)
        except (OSError, ValueError):
            pass
    if inherited_matches:
        yield
        return
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PipelineError(f"Another pipeline or reset is running for {config.env}") from exc
        yield


def run_pipeline(config: PipelineConfig) -> dict[str, Any]:
    with pipeline_lock(config):
        stopped = threading.Event()

        def heartbeat() -> None:
            while not stopped.wait(15):
                print(json.dumps({"event": "pipeline_running", "env": config.env,
                                  "logged_at": utc_now().isoformat()}), flush=True)

        worker = threading.Thread(target=heartbeat, daemon=True)
        worker.start()
        try:
            return run_pipeline_locked(config)
        finally:
            stopped.set()
            worker.join()


def run_pipeline_locked(config: PipelineConfig) -> dict[str, Any]:
    if config.reset_only and not config.reset_processing_dir:
        raise PipelineError("--reset-only requires --reset-processing-dir")
    if config.source_dir.resolve().is_relative_to(config.root.resolve()):
        raise PipelineError("The source directory must be outside the generated processing root")
    if config.reset_processing_dir and config.root.exists():
        shutil.rmtree(config.root)

    if config.reset_only:
        return {
            "status": "reset",
            "processing_root": str(config.root),
            "message": "Generated processing directory removed. Original source data was untouched.",
        }

    run_id = utc_run_id()
    directories = ensure_directories(config.root)
    logger = configure_logging(directories["logs"], run_id)
    log_event(
        logger,
        "pipeline_start",
        run_id=run_id,
        env=config.env,
        source_dir=config.source_dir,
        processing_root=config.root,
    )

    log_event(logger, "pipeline_stage_start", stage="copy_sources")
    copied_sources = copy_source_files(config.source_dir, directories["incoming"])
    log_event(logger, "pipeline_stage_complete", stage="copy_sources", files=len(copied_sources))
    state_path = directories["state"] / "watermarks.json"
    state = load_state(state_path)
    signature = json.dumps({
        "version": 2, "schema_policy": config.schema_policy,
        "start": config.data_interval_start, "end": config.data_interval_end,
    }, default=json_default, sort_keys=True)
    context_changed = state.get("processing_signature") != signature
    pending_writes: dict[Path, pd.DataFrame] = {}

    silver_outputs: dict[str, Path] = {}
    bronze_outputs: dict[str, Path] = {}
    file_results: list[dict[str, Any]] = []

    client_source = copied_sources[CLIENT_FILE]
    client_hash = sha256_file(client_source)
    client_bronze_path = directories["bronze"] / CLIENT_FILE.replace(".csv", ".parquet")
    client_silver_path = directories["silver"] / CLIENT_FILE.replace(".csv", ".parquet")
    bronze_outputs[CLIENT_FILE] = client_bronze_path
    silver_outputs[CLIENT_FILE] = client_silver_path

    if should_process(state, CLIENT_FILE, client_hash, client_silver_path,
                      config.force or context_changed or not client_bronze_path.exists()):
        log_event(logger, "pipeline_stage_start", stage="process_file", file_name=CLIENT_FILE)
        raw_client = read_pipe_csv(client_source)
        pending_writes[client_bronze_path] = raw_client
        client_clean = clean_clientparameters(raw_client, CLIENT_FILE, config.schema_policy)
        if client_silver_path.exists():
            client_clean = merge_clients(load_existing_parquet(client_silver_path), client_clean)
        pending_writes[client_silver_path] = client_clean
        update_file_state(
            state,
            CLIENT_FILE,
            client_hash,
            client_source,
            client_silver_path,
            len(client_clean),
        )
        file_results.append(
            {
                "file_name": CLIENT_FILE,
                "processed": True,
                "source_hash": client_hash,
                "rows_read": len(raw_client),
                "rows_written": len(client_clean),
                "bronze_output": client_bronze_path,
                "silver_output": client_silver_path,
            }
        )
        log_event(logger, "file_processed", file_name=CLIENT_FILE, rows_written=len(client_clean))
    else:
        log_event(logger, "pipeline_stage_start", stage="load_existing_silver", file_name=CLIENT_FILE)
        client_clean = load_existing_parquet(client_silver_path)
        file_results.append(
            {
                "file_name": CLIENT_FILE,
                "processed": False,
                "source_hash": client_hash,
                "rows_written": len(client_clean),
                "silver_output": client_silver_path,
            }
        )
        log_event(logger, "file_skipped", file_name=CLIENT_FILE, reason="unchanged")

    transaction_frames: list[pd.DataFrame] = []
    for file_name in (name for name in copied_sources if name != CLIENT_FILE):
        source_path = copied_sources[file_name]
        source_hash = sha256_file(source_path)
        bronze_path = directories["bronze"] / file_name.replace(".csv", ".parquet")
        silver_path = directories["silver"] / file_name.replace(".csv", ".parquet")
        bronze_outputs[file_name] = bronze_path
        silver_outputs[file_name] = silver_path

        if should_process(state, file_name, source_hash, silver_path,
                          config.force or context_changed or not bronze_path.exists()):
            log_event(logger, "pipeline_stage_start", stage="process_file", file_name=file_name)
            raw_transactions = read_pipe_csv(source_path)
            pending_writes[bronze_path] = raw_transactions
            new_rows = raw_transactions
            committed = state.get("files", {}).get(file_name, {})
            if (bronze_path.exists() and silver_path.exists() and not context_changed and not config.force
                    and committed.get("bronze_sha256") == sha256_file(bronze_path)
                    and committed.get("silver_sha256") == sha256_file(silver_path)):
                previous_raw = load_existing_parquet(bronze_path)
                validate_columns(raw_transactions, TRANSACTION_COLUMNS, file_name, config.schema_policy)
                old_keys = pd.MultiIndex.from_frame(previous_raw.loc[:, TRANSACTION_COLUMNS])
                new_keys = pd.MultiIndex.from_frame(raw_transactions.loc[:, TRANSACTION_COLUMNS])
                new_rows = raw_transactions.loc[~new_keys.isin(old_keys)]
            cleaned_transactions, metrics = clean_transactions(
                new_rows,
                file_name,
                config.schema_policy,
                config.data_interval_start,
                config.data_interval_end,
            )
            if silver_path.exists():
                cleaned_transactions = build_gold_transactions([
                    load_existing_parquet(silver_path), cleaned_transactions,
                ]).loc[:, TRANSACTION_COLUMNS]
            metrics["rows_transformed"] = len(new_rows)
            metrics["rows_written"] = len(cleaned_transactions)
            pending_writes[silver_path] = cleaned_transactions
            update_file_state(
                state,
                file_name,
                source_hash,
                source_path,
                silver_path,
                len(cleaned_transactions),
            )
            file_results.append(
                {
                    "file_name": file_name,
                    "processed": True,
                    "source_hash": source_hash,
                    "rows_read": len(raw_transactions),
                    "bronze_output": bronze_path,
                    "silver_output": silver_path,
                    **metrics,
                }
            )
            log_event(
                logger,
                "file_processed",
                file_name=file_name,
                rows_written=len(cleaned_transactions),
                duplicates_removed=metrics["duplicates_removed"],
            )
        else:
            log_event(logger, "pipeline_stage_start", stage="load_existing_silver", file_name=file_name)
            cleaned_transactions = load_existing_parquet(silver_path)
            file_results.append(
                {
                    "file_name": file_name,
                    "processed": False,
                    "source_hash": source_hash,
                    "rows_written": len(cleaned_transactions),
                    "silver_output": silver_path,
                }
            )
            log_event(logger, "file_skipped", file_name=file_name, reason="unchanged")

        transaction_frames.append(cleaned_transactions)

    # Archived source batches remain part of the dataset until an explicit reset.
    for file_name in state.get("files", {}):
        if file_name != CLIENT_FILE and file_name not in silver_outputs:
            silver_path = directories["silver"] / Path(file_name).with_suffix(".parquet")
            transaction_frames.append(load_existing_parquet(silver_path))
            silver_outputs[file_name] = silver_path
            bronze_outputs[file_name] = directories["bronze"] / silver_path.name

    log_event(logger, "pipeline_stage_start", stage="quality_checks")
    validate_referential_integrity(client_clean, transaction_frames)
    log_event(logger, "pipeline_stage_complete", stage="quality_checks")

    log_event(logger, "pipeline_stage_start", stage="build_gold_outputs")
    gold_client_path = directories["gold"] / "clientparameters.parquet"
    gold_transactions_path = directories["gold"] / "playertransactions.parquet"
    gold_dim_client_path = directories["gold"] / "dim_clientparameters.parquet"
    gold_fact_transactions_path = directories["gold"] / "fact_playertransactions.parquet"
    existing_gold = load_existing_parquet(gold_transactions_path) if gold_transactions_path.exists() else None
    if existing_gold is not None and not any(result["processed"] for result in file_results):
        gold_transactions = existing_gold
        log_event(logger, "gold_build_skipped", reason="no source changes")
    else:
        gold_transactions = build_gold_transactions(transaction_frames, existing_gold)
    pending_writes.update({
        gold_client_path: client_clean, gold_transactions_path: gold_transactions,
        gold_dim_client_path: client_clean, gold_fact_transactions_path: gold_transactions,
    })
    for output_path, frame in pending_writes.items():
        write_parquet_atomic(frame, output_path)
    log_event(
        logger,
        "pipeline_stage_complete",
        stage="build_gold_outputs",
        gold_clientparameters_rows=len(client_clean),
        gold_transactions_rows=len(gold_transactions),
    )

    gold_outputs = {
        "clientparameters": gold_client_path,
        "playertransactions": gold_transactions_path,
        "dim_clientparameters": gold_dim_client_path,
        "fact_playertransactions": gold_fact_transactions_path,
    }

    total_source_rows = sum(
        result.get("rows_read", result.get("rows_written", 0)) for result in file_results
    )
    total_silver_rows = sum(result["rows_written"] for result in file_results)

    report: dict[str, Any] = {
        "run_id": run_id,
        "status": "success",
        "env": config.env,
        "source_dir": config.source_dir,
        "processing_root": config.root,
        "data_interval_start": config.data_interval_start,
        "data_interval_end": config.data_interval_end,
        "files": file_results,
        "incremental": {
            "files_processed": sum(result["processed"] for result in file_results),
            "files_skipped": sum(not result["processed"] for result in file_results),
            "transaction_rows_transformed": sum(result.get("rows_transformed", 0) for result in file_results),
        },
        "row_counts": {
            "source_rows_observed": total_source_rows,
            "silver_rows_written": total_silver_rows,
            "gold_transactions_rows": len(gold_transactions),
            "gold_clientparameters_rows": len(client_clean),
        },
        "outputs": {
            "bronze": bronze_outputs,
            "silver": silver_outputs,
            "gold": gold_outputs,
        },
        "quality_checks": {
            "source_contract": "passed",
            "required_not_null": "passed",
            "primary_key_uniqueness": "passed",
            "referential_integrity": "passed",
            "valid_amounts": "passed",
            "valid_status_and_type": "passed",
        },
        "external_runtimes": describe_external_runtime_flags(config, logger),
        "production_layers": describe_production_layers(
            config,
            bronze_outputs,
            silver_outputs,
            gold_outputs,
        ),
    }

    report["object_store_upload"] = maybe_upload_to_gcs(
        config.gcs_bucket,
        config.no_s3,
        config.no_production_layers,
        bronze_outputs,
        silver_outputs,
        gold_outputs,
        logger,
        directories["state"] / "gcs_uploads.json",
    )
    report["kafka"] = maybe_publish_kafka_event(
        config.no_kafka,
        config.no_production_layers,
        report,
        logger,
    )

    report_path = directories["reports"] / f"{run_id}_data_quality_report.json"
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, default=json_default, sort_keys=True)
        handle.write("\n")
    report["report_path"] = report_path

    state["last_successful_run_id"] = run_id
    state["processing_signature"] = signature
    for file_name, silver_path in silver_outputs.items():
        state["files"][file_name]["silver_sha256"] = sha256_file(silver_path)
        state["files"][file_name]["bronze_sha256"] = sha256_file(bronze_outputs[file_name])
    state["last_successful_run_at"] = utc_now().isoformat()
    save_state(state_path, state)

    log_event(
        logger,
        "pipeline_success",
        run_id=run_id,
        report_path=report_path,
        gold_transactions_rows=len(gold_transactions),
    )
    return report


def parse_optional_timestamp(value: str | None, argument_name: str) -> pd.Timestamp | None:
    if not value:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise argparse.ArgumentTypeError(f"{argument_name} must be a valid timestamp")
    return parsed.tz_localize(None) if parsed.tzinfo else parsed


def parse_args(argv: list[str] | None = None) -> PipelineConfig:
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--env-file", type=Path, default=None)
    pre_args, _ = pre_parser.parse_known_args(argv)
    load_env_files(PROJECT_ROOT, pre_args.env_file)
    no_production_layers_default = env_bool("NO_PRODUCTION_LAYERS", False)

    parser = argparse.ArgumentParser(
        description="Incrementally clean the assignment source files and land Parquet outputs.",
        parents=[pre_parser],
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=env_path("SOURCE_DIR", DEFAULT_SOURCE_DIR),
    )
    parser.add_argument(
        "--processing-dir",
        type=Path,
        default=env_path("PROCESSING_DIR", DEFAULT_PROCESSING_DIR),
    )
    parser.add_argument(
        "--env",
        choices=("dev", "staging", "prod"),
        default=os.getenv("PIPELINE_ENV", os.getenv("ENV", "dev")),
        help="Environment-specific processing namespace.",
    )
    parser.add_argument("--force", action="store_true", help="Reprocess unchanged files.")
    parser.add_argument(
        "--reset-processing-dir",
        action="store_true",
        help="Remove only the generated processing directory for the selected environment.",
    )
    parser.add_argument(
        "--reset-only",
        action="store_true",
        help="Use with --reset-processing-dir to reset generated data and exit.",
    )
    parser.add_argument(
        "--schema-policy",
        choices=("strict", "allow-additive"),
        default="strict",
        help="How to handle source columns outside the documented contract.",
    )
    parser.add_argument("--data-interval-start", help="Optional explicit local ingestion slice start.")
    parser.add_argument("--data-interval-end", help="Optional explicit local ingestion slice end.")
    parser.add_argument(
        "--gcs-bucket",
        default=os.getenv("GCS_BUCKET", os.getenv("BUCKET")),
        help="Optional GCS bucket for assignment bonus uploads.",
    )
    parser.add_argument(
        "--no-production-layers",
        action="store_true",
        default=no_production_layers_default,
        help=(
            "Fast local mode: skip configured external integrations and "
            "production artifact publication."
        ),
    )
    parser.set_defaults(
        no_kafka=env_bool("NO_KAFKA", no_production_layers_default),
        no_spark=env_bool("NO_SPARK", no_production_layers_default),
        no_s3=env_bool("NO_S3", no_production_layers_default),
        no_airflow=env_bool("NO_AIRFLOW", no_production_layers_default),
        no_databricks=env_bool("NO_DATABRICKS", no_production_layers_default),
    )
    parser.add_argument("--no-kafka", dest="no_kafka", action="store_true")
    parser.add_argument("--with-kafka", dest="no_kafka", action="store_false")
    parser.add_argument("--no-spark", dest="no_spark", action="store_true")
    parser.add_argument("--with-spark", dest="no_spark", action="store_false")
    parser.add_argument("--no-s3", dest="no_s3", action="store_true")
    parser.add_argument("--with-s3", dest="no_s3", action="store_false")
    parser.add_argument("--no-airflow", dest="no_airflow", action="store_true")
    parser.add_argument("--with-airflow", dest="no_airflow", action="store_false")
    parser.add_argument(
        "--no-databricks",
        dest="no_databricks",
        action="store_true",
    )
    parser.add_argument("--with-databricks", dest="no_databricks", action="store_false")
    args = parser.parse_args(argv)
    if args.no_production_layers:
        args.no_kafka = True
        args.no_spark = True
        args.no_s3 = True
        args.no_airflow = True
        args.no_databricks = True

    return PipelineConfig(
        source_dir=args.source_dir,
        processing_dir=args.processing_dir,
        env=args.env,
        force=args.force,
        reset_processing_dir=args.reset_processing_dir,
        reset_only=args.reset_only,
        schema_policy=args.schema_policy,
        data_interval_start=parse_optional_timestamp(
            args.data_interval_start,
            "--data-interval-start",
        ),
        data_interval_end=parse_optional_timestamp(args.data_interval_end, "--data-interval-end"),
        gcs_bucket=args.gcs_bucket,
        no_production_layers=args.no_production_layers,
        no_kafka=args.no_kafka,
        no_spark=args.no_spark,
        no_s3=args.no_s3,
        no_airflow=args.no_airflow,
        no_databricks=args.no_databricks,
    )


def main(argv: list[str] | None = None) -> int:
    try:
        config = parse_args(argv)
        report = run_pipeline(config)
        print(json.dumps(report, indent=2, default=json_default, sort_keys=True))
        return 0
    except PipelineError as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
