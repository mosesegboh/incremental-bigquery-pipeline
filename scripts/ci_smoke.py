#!/usr/bin/env python3
"""Exercise the real CLI twice without cloud access or persistent output changes."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def snapshots(root: Path) -> dict:
    return {
        str(path.relative_to(root)): (
            hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns
        )
        for layer in ("incoming-data", "bronze", "silver", "gold")
        for path in (root / layer).iterdir() if path.is_file()
    }


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="pipeline-smoke-") as temporary:
        directory = Path(temporary)
        env_file = directory / "empty.env"
        env_file.touch()
        command = [
            sys.executable, str(ROOT / "main/task_1_data_cleaner.py"),
            "--env-file", str(env_file), "--no-production-layers", "--env", "dev",
            "--source-dir", str(ROOT / "main/data"),
            "--processing-dir", str(directory / "processing"),
        ]
        root = directory / "processing/dev"
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, timeout=180)
        gold = pd.read_parquet(root / "gold/playertransactions.parquet")
        clients = pd.read_parquet(root / "gold/clientparameters.parquet")
        assert not gold.empty and not clients.empty, "Gold outputs must contain records"
        assert gold["CODE"].is_unique, "Gold transaction keys must be unique"
        assert gold["CLIENTPARAMETERCODE"].isin(clients["CODE"]).all()
        before = snapshots(root)
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, timeout=180)
        assert snapshots(root) == before, "Unchanged input must preserve outputs and mtimes"
        reports = sorted((root / "reports").glob("*.json"))
        report = json.loads(reports[-1].read_text())
        assert report["status"] == "success"
        assert report["incremental"]["files_processed"] == 0
        assert report["incremental"]["transaction_rows_transformed"] == 0
        print(f"CLI smoke passed: {len(gold)} transactions; unchanged rerun is idempotent")


if __name__ == "__main__":
    main()
