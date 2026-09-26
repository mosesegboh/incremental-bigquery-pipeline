import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import bigquery_incremental as warehouse
from task_1_data_cleaner import save_state, sha256_file


@pytest.fixture
def cloud_config(tmp_path, monkeypatch):
    from types import SimpleNamespace
    root = tmp_path / "processing/dev"
    (root / "silver").mkdir(parents=True)
    for key, value in {"PROJECT_ID": "test-project", "DATASET": "test_dataset",
                       "REGION": "EU", "GCS_BUCKET": "test-bucket"}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(warehouse, "parse_args", lambda _: SimpleNamespace(root=root))
    return root


def prepare_batch(root):
    path = root / "silver/playertransactions_2026_05.parquet"
    path.write_bytes(b"example-parquet")
    digest = sha256_file(path)
    uri = "gs://test-bucket/playertransactions/" + path.name
    save_state(root / "_state/gcs_uploads.json", {uri: digest})
    return {"uri": uri, "sha256": digest}


def test_loaded_batch_causes_no_upload_or_data_mutation(cloud_config, monkeypatch):
    row = prepare_batch(cloud_config)
    calls = []

    def command(args, input_text=None):
        calls.append((args, input_text))
        return json.dumps([row]) if "SELECT uri, sha256" in (input_text or "") else ""

    monkeypatch.setattr(warehouse, "run_command", command)
    warehouse.load()
    assert len(calls) == 2
    assert all(args[0] == "bq" for args, _ in calls)
    assert not any("BEGIN TRANSACTION" in (sql or "") for _, sql in calls)


def test_failed_warehouse_commit_retries_pending_batch(cloud_config, monkeypatch):
    prepare_batch(cloud_config)
    calls = []
    fail = True

    def command(args, input_text=None):
        nonlocal fail
        calls.append((args, input_text))
        if "SELECT uri, sha256" in (input_text or ""):
            return "[]"
        if "BEGIN TRANSACTION" in (input_text or "") and fail:
            fail = False
            raise subprocess.CalledProcessError(1, args)
        return ""

    monkeypatch.setattr(warehouse, "run_command", command)
    with pytest.raises(subprocess.CalledProcessError):
        warehouse.load()
    warehouse.load()
    assert sum(args[0] == "gcloud" for args, _ in calls) == 1
    assert sum("BEGIN TRANSACTION" in (sql or "") for _, sql in calls) == 2
    assert not (cloud_config / "_state/warehouse.json").exists()


def test_generated_load_is_atomic_and_reads_only_pending_files():
    pending = [{"family": "playertransactions", "batch_uri": "gs://test-bucket/_batches/hash/new.parquet"}]
    sql = warehouse.load_sql(pending, "test-project", "test_dataset", "test-bucket", "test")
    assert "uris=[\"gs://test-bucket/_batches/hash/new.parquet\"]" in sql
    assert "playertransactions_2026_03" not in sql
    assert "TRUNCATE" not in sql
    assert "CREATE OR REPLACE" not in sql
    begin = sql.index("BEGIN TRANSACTION;")
    commit = sql.index("COMMIT TRANSACTION;")
    for table in ("clientparameters", "playertransactions_raw", "playertransactions", "_pipeline_batches"):
        assert begin < sql.index(f"MERGE `test-project.test_dataset.{table}`") < commit
    assert "MIN(INGESTION_TIMESTAMP) FROM transaction_batch" in sql
    assert "source.LASTUPDATEDATE >= target.LASTUPDATEDATE" in sql
    assert "@data_interval_start" not in sql


def test_reset_deletes_only_owned_objects_and_clears_local_last(cloud_config, monkeypatch):
    calls = []
    objects = [
        {"name": "playertransactions/playertransactions_2026_05.parquet"},
        {"name": "gold/playertransactions/fact_playertransactions.parquet"},
        {"name": "personal/photo.png"},
        {"name": "playertransactions/README.txt"},
    ]

    def command(args, input_text=None):
        calls.append((args, input_text))
        if args[:4] == ["gcloud", "storage", "objects", "list"]:
            assert args[4] == "gs://test-bucket/**"
            return json.dumps(objects)
        return ""

    monkeypatch.setattr(warehouse, "run_command", command)
    monkeypatch.setattr(warehouse, "run_pipeline_locked", lambda _: calls.append((["local-reset"], None)))
    warehouse.reset()
    deletes = [args[-1] for args, _ in calls if args[:3] == ["gcloud", "storage", "rm"]]
    assert len(deletes) == 2
    assert not any("personal" in uri or "README" in uri for uri in deletes)
    assert calls[-1][0] == ["local-reset"]
    sql = next(sql for args, sql in calls if args[0] == "bq")
    assert "DROP TABLE IF EXISTS `test-project.test_dataset._pipeline_batches`" in sql
    assert "DROP SCHEMA" not in sql


def test_failed_cloud_reset_retains_local_state(cloud_config, monkeypatch):
    prepare_batch(cloud_config)
    state = cloud_config / "_state/gcs_uploads.json"
    before = state.read_bytes()

    def command(args, input_text=None):
        if args[0] == "bq":
            raise subprocess.CalledProcessError(1, args)
        return "[]"

    monkeypatch.setattr(warehouse, "run_command", command)
    monkeypatch.setattr(warehouse, "run_pipeline_locked", lambda _: pytest.fail("Must not clear state after cloud failure"))
    with pytest.raises(subprocess.CalledProcessError):
        warehouse.reset()
    assert state.read_bytes() == before
