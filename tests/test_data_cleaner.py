from pathlib import Path
from dataclasses import replace
import json
import logging
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "main"))

from task_1_data_cleaner import (  # noqa: E402
    PipelineConfig,
    DataQualityError,
    PipelineError,
    clean_amount_value,
    load_env_files,
    parse_args,
    parse_timestamp_series,
    run_pipeline,
    load_state,
    maybe_upload_to_gcs,
    pipeline_lock,
    sha256_file,
)


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_clean_amount_value_handles_currency_and_comma_decimal() -> None:
    assert clean_amount_value("50,00€") == clean_amount_value("50.00")


def test_parse_timestamp_series_handles_supported_formats() -> None:
    parsed = parse_timestamp_series(
        pd.Series(["2026-03-02 09:02:16", "02/03/2026 09:02:22"]),
        "sample.csv",
        "REQUESTDATE",
        nullable=False,
    )

    assert parsed.dt.strftime("%Y-%m-%d %H:%M:%S").tolist() == [
        "2026-03-02 09:02:16",
        "2026-03-02 09:02:22",
    ]


def test_load_env_files_preserves_real_environment_precedence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    write_text(
        tmp_path / ".env",
        "\n".join(
            [
                "PROJECT_ID=from-dotenv",
                "GCS_BUCKET=from-dotenv-bucket",
            ]
        )
        + "\n",
    )
    write_text(tmp_path / ".env.local", "PROJECT_ID=from-dotenv-local\n")
    monkeypatch.setenv("PROJECT_ID", "from-real-env")
    monkeypatch.delenv("GCS_BUCKET", raising=False)

    loaded = load_env_files(tmp_path)

    assert loaded["GCS_BUCKET"] == "from-dotenv-bucket"
    assert "PROJECT_ID" not in loaded
    assert __import__("os").environ["PROJECT_ID"] == "from-real-env"


def test_default_cli_enables_production_layers(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    for name in (
        "NO_PRODUCTION_LAYERS",
        "NO_KAFKA",
        "NO_SPARK",
        "NO_S3",
        "NO_AIRFLOW",
        "NO_DATABRICKS",
    ):
        monkeypatch.delenv(name, raising=False)

    config = parse_args(
        [
            "--env-file",
            str(env_file),
            "--source-dir",
            str(tmp_path / "data"),
            "--processing-dir",
            str(tmp_path / "processing"),
        ]
    )

    assert config.no_production_layers is False
    assert config.no_kafka is False
    assert config.no_spark is False
    assert config.no_s3 is False
    assert config.no_airflow is False
    assert config.no_databricks is False


def test_no_production_layers_disables_optional_runtimes(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    for name in (
        "NO_PRODUCTION_LAYERS",
        "NO_KAFKA",
        "NO_SPARK",
        "NO_S3",
        "NO_AIRFLOW",
        "NO_DATABRICKS",
    ):
        monkeypatch.delenv(name, raising=False)

    config = parse_args(
        [
            "--env-file",
            str(env_file),
            "--source-dir",
            str(tmp_path / "data"),
            "--processing-dir",
            str(tmp_path / "processing"),
            "--no-production-layers",
        ]
    )

    assert config.no_production_layers is True
    assert config.no_kafka is True
    assert config.no_spark is True
    assert config.no_s3 is True
    assert config.no_airflow is True
    assert config.no_databricks is True


@pytest.fixture
def pipeline_config(tmp_path: Path) -> PipelineConfig:
    source_dir = tmp_path / "data"
    processing_dir = tmp_path / "processing"
    source_dir.mkdir()

    write_text(
        source_dir / "clientparameters.csv",
        "\n".join(
            [
                "CODE|CLIENTTYPE|CLIENTPLATFORM|NOTREAL|LASTMODIFICATIONDATE",
                "1|casino|download|0|2025-08-25 06:03:44",
            ]
        )
        + "\n",
    )

    transactions_header = (
        "CODE|PLAYERCODE|REQUESTDATE|ACCEPTDATE|AMOUNT|TYPE|STATUS|"
        "PAYMENTMETHOD|PAYMENTSUBMETHOD|COUNTRY|CLIENTPARAMETERCODE|"
        "FIRSTDEPOSIT|LASTUPDATEDATE|INGESTION_TIMESTAMP"
    )
    march_rows = [
        transactions_header,
        (
            "100|200|02/03/2026 09:02:22|02/03/2026 09:02:29|50,00€|"
            "withdraw|APPROVED|ShopWithdraw||Netherlands|1|0|"
            "02/03/2026 09:03:40|2026-05-18 16:03:08"
        ),
    ]
    april_rows = [
        transactions_header,
        (
            "101|200|2026-04-01 04:01:06||10.01|deposit|waiting|"
            "VISA||United Kingdom|1|0|2026-04-01 04:01:09|2026-05-18 17:05:11"
        ),
    ]
    write_text(source_dir / "playertransactions_2026_03.csv", "\n".join(march_rows) + "\n")
    write_text(source_dir / "playertransactions_2026_04.csv", "\n".join(april_rows) + "\n")

    config = PipelineConfig(
        source_dir=source_dir,
        processing_dir=processing_dir,
        env="dev",
        force=False,
        reset_processing_dir=False,
        reset_only=False,
        schema_policy="strict",
        data_interval_start=None,
        data_interval_end=None,
        gcs_bucket=None,
        no_production_layers=True,
        no_kafka=True,
        no_spark=True,
        no_s3=True,
        no_airflow=True,
        no_databricks=True,
    )

    return config


def test_run_pipeline_writes_incremental_outputs(pipeline_config: PipelineConfig) -> None:
    config = pipeline_config
    processing_dir = config.processing_dir
    report = run_pipeline(config)

    assert report["status"] == "success"
    assert (processing_dir / "dev" / "incoming-data" / "clientparameters.csv").exists()
    assert (processing_dir / "dev" / "silver" / "playertransactions_2026_03.parquet").exists()
    assert (processing_dir / "dev" / "gold" / "playertransactions.parquet").exists()
    assert (processing_dir / "dev" / "gold" / "dim_clientparameters.parquet").exists()
    assert (processing_dir / "dev" / "gold" / "fact_playertransactions.parquet").exists()
    assert report["row_counts"]["gold_transactions_rows"] == 2
    assert report["production_layers"]["enabled"] is False


def snapshot_outputs(root: Path) -> dict:
    return {str(path.relative_to(root)): (path.stat().st_mtime_ns, sha256_file(path))
            for folder in ("incoming-data", "bronze", "silver", "gold")
            for path in (root / folder).iterdir() if path.is_file()}


def test_repeat_run_preserves_files_and_audit_times(pipeline_config) -> None:
    run_pipeline(pipeline_config)
    before = snapshot_outputs(pipeline_config.root)
    report = run_pipeline(pipeline_config)
    assert report["incremental"]["files_processed"] == 0
    assert report["incremental"]["transaction_rows_transformed"] == 0
    assert snapshot_outputs(pipeline_config.root) == before


def test_append_transforms_only_new_rows_and_preserves_other_files(pipeline_config) -> None:
    run_pipeline(pipeline_config)
    before = snapshot_outputs(pipeline_config.root)
    gold_path = pipeline_config.root / "gold/playertransactions.parquet"
    old = pd.read_parquet(gold_path).set_index("CODE")
    source = pipeline_config.source_dir / "playertransactions_2026_04.csv"
    row = source.read_text().splitlines()[1]
    with source.open("a") as handle:
        handle.write(row.replace("101|", "102|", 1) + "\n")
    report = run_pipeline(pipeline_config)
    assert report["incremental"]["files_processed"] == 1
    assert report["incremental"]["transaction_rows_transformed"] == 1
    assert report["row_counts"]["gold_transactions_rows"] == 3
    new = pd.read_parquet(gold_path).set_index("CODE")
    pd.testing.assert_frame_equal(new.loc[old.index], old)
    after = snapshot_outputs(pipeline_config.root)
    for name in before:
        if "clientparameters" in name or "2026_03" in name:
            assert before[name] == after[name]


def test_new_batch_updates_latest_and_does_not_delete_archived_data(pipeline_config) -> None:
    run_pipeline(pipeline_config)
    gold_path = pipeline_config.root / "gold/playertransactions.parquet"
    old = pd.read_parquet(gold_path).set_index("CODE")
    april = pipeline_config.source_dir / "playertransactions_2026_04.csv"
    batch = pipeline_config.source_dir / "playertransactions_2026_05.csv"
    batch.write_text(april.read_text().replace("waiting", "approved").replace(
        "2026-04-01 04:01:09", "2026-05-01 04:01:09"))
    report = run_pipeline(pipeline_config)
    assert report["incremental"]["files_processed"] == 1
    new = pd.read_parquet(gold_path).set_index("CODE")
    assert new.loc[101, "STATUS"] == "approved"
    assert new.loc[101, "DWH_CREATE_TIME"] == old.loc[101, "DWH_CREATE_TIME"]
    assert new.loc[101, "DWH_UPDATE_TIME"] > old.loc[101, "DWH_UPDATE_TIME"]
    batch.unlink()
    run_pipeline(pipeline_config)
    pd.testing.assert_frame_equal(pd.read_parquet(gold_path).set_index("CODE"), new)
    batch.write_text(april.read_text())
    run_pipeline(pipeline_config)
    pd.testing.assert_frame_equal(pd.read_parquet(gold_path).set_index("CODE"), new)


def test_interval_changes_do_not_skip_unprocessed_rows(pipeline_config) -> None:
    first = replace(pipeline_config, data_interval_start=pd.Timestamp("2026-05-18 16:00:00"),
                    data_interval_end=pd.Timestamp("2026-05-18 17:00:00"))
    second = replace(first, data_interval_start=pd.Timestamp("2026-05-18 17:00:00"),
                     data_interval_end=pd.Timestamp("2026-05-18 18:00:00"))
    assert run_pipeline(first)["row_counts"]["gold_transactions_rows"] == 1
    assert run_pipeline(second)["row_counts"]["gold_transactions_rows"] == 2
    assert run_pipeline(second)["incremental"]["files_processed"] == 0


def test_quality_failure_does_not_commit_outputs_or_checkpoint(pipeline_config) -> None:
    run_pipeline(pipeline_config)
    before = snapshot_outputs(pipeline_config.root)
    state_path = pipeline_config.root / "_state/watermarks.json"
    state = state_path.read_bytes()
    source = pipeline_config.source_dir / "playertransactions_2026_04.csv"
    source.write_text(source.read_text().replace("Kingdom|1|", "Kingdom|999|"))
    with pytest.raises(DataQualityError):
        run_pipeline(pipeline_config)
    assert state_path.read_bytes() == state
    after = snapshot_outputs(pipeline_config.root)
    for name in before:
        if not name.startswith("incoming-data/"):
            assert after[name] == before[name]


def test_missing_output_is_repaired(pipeline_config) -> None:
    run_pipeline(pipeline_config)
    missing = pipeline_config.root / "bronze/playertransactions_2026_03.parquet"
    missing.unlink()
    report = run_pipeline(pipeline_config)
    assert missing.exists()
    assert report["incremental"]["files_processed"] == 1


def test_retry_after_partial_parquet_write_does_not_lose_appended_rows(pipeline_config, monkeypatch) -> None:
    import task_1_data_cleaner as cleaner
    run_pipeline(pipeline_config)
    source = pipeline_config.source_dir / "playertransactions_2026_04.csv"
    with source.open("a") as handle:
        handle.write(source.read_text().splitlines()[1].replace("101|", "102|", 1) + "\n")
    original_write = cleaner.write_parquet_atomic

    def fail_silver(frame, path):
        if path.parent.name == "silver" and "2026_04" in path.name:
            raise OSError("injected disk failure after bronze write")
        original_write(frame, path)

    monkeypatch.setattr(cleaner, "write_parquet_atomic", fail_silver)
    with pytest.raises(OSError, match="injected"):
        run_pipeline(pipeline_config)
    monkeypatch.setattr(cleaner, "write_parquet_atomic", original_write)
    assert run_pipeline(pipeline_config)["row_counts"]["gold_transactions_rows"] == 3


def test_reset_only_removes_generated_data(pipeline_config) -> None:
    run_pipeline(pipeline_config)
    original = {path.name: sha256_file(path) for path in pipeline_config.source_dir.iterdir()}
    reset = replace(pipeline_config, reset_processing_dir=True, reset_only=True)
    assert run_pipeline(reset)["status"] == "reset"
    assert not pipeline_config.root.exists()
    assert {path.name: sha256_file(path) for path in pipeline_config.source_dir.iterdir()} == original
    assert run_pipeline(pipeline_config)["incremental"]["files_processed"] == 3


def test_overlapping_run_is_rejected(pipeline_config) -> None:
    with pipeline_lock(pipeline_config):
        with pytest.raises(PipelineError, match="Another pipeline"):
            run_pipeline(pipeline_config)


def test_gcs_upload_checkpoints_partial_success_and_bucket_switch(tmp_path, monkeypatch) -> None:
    import google.cloud.storage
    uploaded = []
    failing = {"second.parquet"}

    class Blob:
        def __init__(self, bucket, name):
            self.bucket, self.name = bucket, name

        def upload_from_filename(self, path):
            if Path(path).name in failing:
                raise RuntimeError("injected upload failure")
            uploaded.append((self.bucket, self.name))

    class Bucket:
        def __init__(self, name):
            self.name = name

        def blob(self, name):
            return Blob(self.name, name)

    class Client:
        def bucket(self, name):
            return Bucket(name)

    monkeypatch.setattr(google.cloud.storage, "Client", Client)
    files = {}
    for name in ("first", "second"):
        path = tmp_path / f"{name}.parquet"
        path.write_bytes(name.encode())
        files[name] = path
    state_path = tmp_path / "uploads.json"

    def upload(bucket="test-bucket"):
        return maybe_upload_to_gcs(bucket, False, True, {}, files, {},
                                   logging.getLogger("test-upload"), state_path)

    with pytest.raises(RuntimeError, match="injected"):
        upload()
    assert len(uploaded) == 1
    failing.clear()
    assert len(upload()["uploaded"]) == 1
    assert len(upload()["uploaded"]) == 0
    assert len(upload("different-bucket")["uploaded"]) == 2
    assert len(uploaded) == 4
