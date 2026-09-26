# Incremental Verification: 2026-09-10

Verified against project `de-test-project-moses`, dataset `de_test_dataset`, and bucket `de-test-project-moses-de-test-bucket`.

- `make test`: 21 passed. The installed pandas/NumPy versions emit dependency deprecation warnings during concatenation.
- Live `make run`: completed successfully. Baseline registration affected zero existing rows in each of the three core BigQuery tables and added three batch-ledger entries.
- Unchanged live run `20260910T085644918491Z`: zero files processed, three files skipped, zero transaction rows transformed, 13 GCS uploads skipped, and zero pending warehouse batches. Gold rebuilding was skipped.
- BigQuery row counts remained 41 client configurations, 19,842 raw transactions, and 19,161 curated transactions.
- `scripts/verify_bigquery_incremental.py`: live assertions passed using temporary BigQuery tables. Covered insert, unchanged replay, newer update with preserved creation time, an older-ingestion replay, and a late arrival of an older source version.
- Reset: local deletion, cloud ownership filtering, and failure/retry boundaries were tested with temporary files/mocked cloud commands. Recursive and empty GCS listings were verified against the CLI. No destructive reset of the actual assignment data was performed.
- Python compilation and shell syntax checks passed. The optional Spark artifact was syntax-checked only because PySpark is not installed; Airflow/Databricks runtimes were not executed.

Supporting files:

- `20260910T085717Z_bigquery_counts.txt`
- `20260910T085717Z_bigquery_tables.txt`
- `20260910T085717Z_gcs_objects.txt`
- `incremental_merge_verification.txt`
- Local JSON report: `main/processing/dev/reports/20260910T085644918491Z_data_quality_report.json`

Review commands and screenshot queries are documented in `bonus/incremental_pipeline_review.md`.
