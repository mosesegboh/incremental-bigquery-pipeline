# Incremental Pipeline Review

## Commands And Layers

| Command/layer | Responsibility | Implementation |
| --- | --- | --- |
| `make run` | Complete incremental GCP run; progress messages during processing and cloud waits | `Makefile`, `scripts/gcp_bigquery_test.sh` |
| Source/incoming | Discover batches and copy only changed source content | `main/task_1_data_cleaner.py`: `copy_source_files` |
| Bronze | Raw per-file Parquet snapshot used to identify previously observed rows | `run_pipeline_locked` |
| Silver | Clean dates, amounts, statuses; retain previously processed records | `clean_transactions`, `merge_clients` |
| Gold | Latest transaction per `CODE`; preserve create/update times for unchanged records | `build_gold_transactions` |
| GCS | Upload changed outputs; checkpoint each success; preserve immutable load batches | `maybe_upload_to_gcs`, `scripts/bigquery_incremental.py` |
| BigQuery DDL | Create missing tables, retain existing rows and partitions | `main/task_2_data_ddl_dml.sql` |
| BigQuery load | Select pending file hashes from `_pipeline_batches`; append unseen raw rows and upsert dimensions | `scripts/bigquery_incremental.py`, `bonus/bigquery_load_from_gcs.sql` |
| Curated merge | Assignment's hourly source slice and guarded upsert; audit timestamps change only with mutable fields | `main/task_2_data_ddl_dml.sql` |
| Layer tables | Non-destructive external bronze/silver/gold tables | `bonus/bigquery_production_layers.sql` |
| Analytics/evidence | Assignment queries, GCS/table lists, row counts | `main/task_3_analytic_queries.sql`, script `evidence` stage |
| `make reset` | Remove pipeline-owned cloud objects/tables, then local generated state | `scripts/bigquery_incremental.py`: `reset` |
| `make reset-local` | Local rebuild only, leaving GCP data in place | `main/task_1_data_cleaner.py`: `run_pipeline_locked` |

## Repeat-Run Test

Run from the repository root with dependencies and GCP credentials configured:

```bash
make test
make run
make run
```

The second run should report zero files processed and zero transaction rows transformed, skip GCS uploads, and print `0 new/changed batches`. It still executes resource checks, analytics, and evidence queries. Existing BigQuery rows and their audit timestamps should remain the same.

Inspect local checkpoints and logs:

```bash
ls main/processing/dev/_state
rg 'file_skipped|gold_build_skipped|gcs_upload_skipped' main/processing/dev/logs
```

The tests use temporary source data to exercise appends, newly discovered batches, late updates, interval changes, validation failures, partial uploads, warehouse retries, and reset boundaries. They do not reset your actual GCP resources.

Run the live warehouse assertions separately after provisioning the assignment tables:

```bash
.venv/bin/python scripts/verify_bigquery_incremental.py
```

This reads the real table schemas, creates temporary test tables, and checks insertion, replay stability, newer updates, and older late arrivals. It does not mutate the assignment tables. Results are saved in `bonus/gcp_evidence/incremental_merge_verification.txt`.

For a local-only experiment, copy the supplied CSVs into a temporary directory and point `--source-dir` and `--processing-dir` at temporary paths. Add a `playertransactions_2026_05.csv` with the same schema and a new transaction `CODE`, then rerun. Only that batch should be processed. Do not edit the supplied assignment CSVs for the test.

## Screenshot Evidence

Refresh the saved evidence:

```bash
bash scripts/gcp_bigquery_test.sh evidence
gcloud storage ls -r 'gs://de-test-project-moses-de-test-bucket/**'
```

In the BigQuery Console, run the following before and after a repeated `make run` (replace project/dataset for another environment):

```sql
SELECT 'clientparameters' AS table_name, COUNT(*) AS row_count
FROM `de-test-project-moses.de_test_dataset.clientparameters`
UNION ALL
SELECT 'playertransactions_raw', COUNT(*)
FROM `de-test-project-moses.de_test_dataset.playertransactions_raw`
UNION ALL
SELECT 'playertransactions', COUNT(*)
FROM `de-test-project-moses.de_test_dataset.playertransactions`;

SELECT CODE, STATUS, AMOUNT, LASTUPDATEDATE, DWH_CREATE_TIME, DWH_UPDATE_TIME
FROM `de-test-project-moses.de_test_dataset.playertransactions`
ORDER BY CODE LIMIT 20;

SELECT uri, sha256, loaded_at
FROM `de-test-project-moses.de_test_dataset._pipeline_batches`
ORDER BY loaded_at, uri;
```

Capture the query results, the bucket's `clientparameters`/`playertransactions` folders, and the second-run terminal messages. These demonstrate the required landing/load bonus steps and incremental behavior. The batch ledger has one entry per successfully loaded URI/content hash; a normal repeat run should add no entries. Existing assignment data should remain at 41 client configurations, 19,842 raw rows and 19,161 curated transactions unless new input has been added.

## Reset And Recovery

Use `make reset` only for an intentional full rebuild, followed by `make run`. It deletes generated pipeline data across GCS, BigQuery, and the selected local environment. `make reset-local` leaves remote data and its batch ledger intact. A failed reset retains local checkpoints until cloud cleanup succeeds, so the same reset command can be retried.

For an ordinary failed run, fix the reported issue and rerun `make run`; do not reset. Successful object uploads remain checkpointed. BigQuery commits the data and its ledger together, so a failed warehouse transaction remains pending. Temporary load tables expire after a day if a query fails before cleanup.
