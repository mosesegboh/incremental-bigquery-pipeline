# DE Test Assignment Solution

This solution keeps the assignment deliverables simple while adding production-style controls around them. BigQuery is the selected warehouse because the brief explicitly prefers it and `config.txt` is already set to `dbengine=bigquery`.

There is no `logs` directory or Power BI file in the supplied project. The pipeline treats `main/data` as the immutable source and copies new or changed files into `main/processing/<env>/incoming-data`. Normal runs retain existing data. `make reset` explicitly clears pipeline-owned GCS objects, BigQuery tables, and local generated state; `make reset-local` clears only local generated data. Neither command deletes the source CSVs.

## System Requirements

- Python 3.10 or newer. Tested target: Python 3.12.
- `make` for the documented commands.
- Docker and Docker Compose, optional.
- Google Cloud SDK/BigQuery access for the default production validation path.
- Airflow, Kafka, PySpark, and Databricks are optional production deployment targets/artifacts. BigQuery remains the selected assignment warehouse target.

## Python Dependencies

Install from `requirements.txt`:

- `pandas`: CSV parsing, validation, and transformations.
- `pyarrow`: Parquet read/write.
- `google-cloud-storage`: optional GCS upload for Bonus Task 1.
- `pytest`: local tests.

Optional integrations:

- `confluent-kafka`: only needed if publishing run reports to Kafka using `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC`.
- Airflow: only needed if deploying the DAG in `bonus/airflow`.
- PySpark/Databricks clients are not needed for the BigQuery submission; the production-scale Spark/Delta artifacts are supplied under `bonus`.

## Source Contracts

The script validates the documented schemas before writing outputs.

- `clientparameters.CODE` is the primary key.
- `playertransactions.CODE` is the transaction key.
- Transaction updates are resolved by latest `LASTUPDATEDATE`, then latest `INGESTION_TIMESTAMP`.
- Required fields must not be blank.
- `AMOUNT` must be a non-negative decimal.
- `TYPE` must be `deposit` or `withdraw`.
- `STATUS` must be `approved`, `waiting`, `declined`, or `issued`.
- `CLIENTPARAMETERCODE` must exist in `clientparameters.CODE`.
- Schema policy is strict by default. Use `--schema-policy allow-additive` only when intentionally tolerating new source columns.

## Local Setup Without Docker

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` with your real `PROJECT_ID`, `BUCKET`, and `GCS_BUCKET` before running the default production path.

```bash
make run
make test
```

`make run` is the incremental GCP/BigQuery command. It checks source and upload checkpoints, processes new or changed data, creates missing tables, and commits pending raw rows, curated upserts, and the warehouse batch ledger in one transaction. It then runs analytics and saves evidence. It never truncates or recreates existing data tables. A repeated run with unchanged inputs skips cleaning, uploads, and warehouse data mutations; provisioning checks, analytics, and evidence queries still run.

For a fast local-only run without GCP or other production integrations:

```bash
make run-fast
```

The local pipeline copies the source files into a processing directory, skips unchanged files on later runs, and writes:

- `main/processing/dev/incoming-data`: copied source files.
- `main/processing/dev/bronze`: raw Parquet snapshots.
- `main/processing/dev/silver`: cleaned Parquet files with the same base names as the CSVs.
- `main/processing/dev/gold`: business-ready `clientparameters.parquet` and deduplicated `playertransactions.parquet`.
- `main/processing/dev/gold/dim_clientparameters.parquet`: dimension alias for the client parameter model.
- `main/processing/dev/gold/fact_playertransactions.parquet`: fact alias for the transaction model.
- `main/processing/dev/_state/watermarks.json`: file hash checkpoint state.
- `main/processing/dev/_state/gcs_uploads.json`: successful uploads, keyed by bucket/object URI and content hash.
- `main/processing/dev/logs`: JSONL execution logs.
- `main/processing/dev/reports`: data quality reports.

To reprocess from a clean generated area while leaving `main/data` untouched:

```bash
make reset-local
make run-fast
```

## Local Setup With Docker

```bash
docker compose run --rm pipeline
```

Docker runs the fast local-only path and writes generated files back to the mounted project directory. The full `make run` production path should be run on the host where `gcloud` and `bq` are authenticated.

## Runtime Options

The default command runs the full GCP/BigQuery validation path:

```bash
make run
```

Equivalent explicit command:

```bash
bash scripts/gcp_bigquery_test.sh all
```

Fast local-only command:

```bash
make run-fast
```

Equivalent explicit command:

```bash
python3 main/task_1_data_cleaner.py --no-production-layers
```

Useful variants:

```bash
python3 main/task_1_data_cleaner.py --force
python3 main/task_1_data_cleaner.py --env staging
python3 main/task_1_data_cleaner.py --schema-policy allow-additive
python3 main/task_1_data_cleaner.py --data-interval-start "2026-05-18 16:00:00" --data-interval-end "2026-05-18 17:00:00"
python3 main/task_1_data_cleaner.py --with-s3 --gcs-bucket de-test-bucket
```

`--no-production-layers` disables GCS/Kafka production integration attempts in one switch. `--no-s3` disables object-store upload behavior only. The assignment asks for GCS rather than AWS S3, but the flag is kept because reviewers often use `--no-s3` to mean "no object-store integration."

## Credentials And Env Vars

Use environment variables for non-secret runtime configuration such as project ID, dataset, region, bucket name, and optional feature flags. Do not put private key JSON, refresh tokens, or passwords in shell history, committed files, or copied assignment artifacts.

The Python and shell entrypoints load `.env` and `.env.local` automatically. Existing environment variables take precedence, so CI/CD and Airflow can inject values without being overridden by local files. Python command-line arguments take precedence over both. `DATA_INTERVAL_START`/`DATA_INTERVAL_END` configure the explicit BigQuery `merge` replay stage; local slicing requires the corresponding CLI flags. Normal runs discover pending data without a hardcoded date window.

Local setup:

```bash
cp .env.example .env
```

Then edit `.env`:

```bash
PROJECT_ID=your-real-google-project-id
DATASET=de_test_dataset
REGION=EU
BUCKET=your-real-google-project-id-de-test-bucket
GCS_BUCKET=your-real-google-project-id-de-test-bucket
NO_PRODUCTION_LAYERS=false
NO_KAFKA=false
NO_SPARK=false
NO_S3=false
NO_AIRFLOW=false
NO_DATABRICKS=false
```

For local-only testing, run `make run-fast`. `NO_PRODUCTION_LAYERS=true` controls direct Python execution; `make run` always runs the GCP stages.

For local GCP testing, prefer Google Application Default Credentials:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project "$PROJECT_ID"
```

These credentials are stored by the Google Cloud SDK outside this repository. Closing the terminal does not remove them. Short-lived terminal variables such as `PROJECT_ID` and `BUCKET` do disappear when the shell closes, so keep non-secret values in the local ignored `.env` file or in your shell profile. Use `.env.example` as the template.

In deployed data pipelines, the better pattern is managed identity or secret-backed configuration:

- GCP/Cloud Composer: attach a least-privilege service account to the runtime.
- Airflow: store connections and sensitive variables in a secrets backend, not DAG code.
- CI/CD: use encrypted repository secrets or workload identity federation.
- Databricks: use secret scopes, OAuth, or external integrations managed outside the repo.

Before submission, verify that no private material is present:

```bash
rg -n "BEGIN PRIVATE KEY|private_key|client_email|refresh_token|GOOGLE_APPLICATION_CREDENTIALS|AIza|ya29\\." .
find . -maxdepth 4 -type f \( -name ".env*" -o -name "*credentials*.json" -o -name "*secret*.json" -o -name "service-account*.json" \)
```

## BigQuery Setup

1. Create or select a GCP project. The SQL uses `de-test-project.de_test_dataset`; replace that prefix if needed.
2. Run `main/task_2_data_ddl_dml.sql` to create the dataset and tables.
3. Run the cleaner locally.
4. Optional bonus upload:

```bash
python3 main/task_1_data_cleaner.py --with-s3 --gcs-bucket de-test-bucket
```

5. Run `bash scripts/gcp_bigquery_test.sh load`. The driver in `scripts/bigquery_incremental.py` supplies pending batch URIs and renders `bonus/bigquery_load_from_gcs.sql`. The SQL template requires the driver's temporary tables and batch manifest; it is not a standalone SQL script.
6. Run the `MERGE` in `main/task_2_data_ddl_dml.sql` with runtime query parameters:

- `data_interval_start`
- `data_interval_end`

The merge matches on `CODE`, deduplicates the raw hourly slice, rejects older `LASTUPDATEDATE` values, and updates audit timestamps only when one of the assignment's update fields changes. The normal load stage derives its interval from pending data and merges in the same transaction as the raw load. The standalone `merge` stage is available for explicit hourly replay.

You can also run the full GCP validation path from `.env` with one command:

```bash
make gcp-test
```

That command follows the same incremental path as `make run`. It requires `gcloud`, `bq`, and an authenticated GCP account. GCS upload uses Application Default Credentials when available and otherwise falls back to the authenticated `gcloud` CLI.

The same script can run one stage at a time:

```bash
bash scripts/gcp_bigquery_test.sh bootstrap
bash scripts/gcp_bigquery_test.sh upload
bash scripts/gcp_bigquery_test.sh ddl
bash scripts/gcp_bigquery_test.sh layers
bash scripts/gcp_bigquery_test.sh load
DATA_INTERVAL_START=2026-05-18T16:00:00Z DATA_INTERVAL_END=2026-05-18T17:00:00Z bash scripts/gcp_bigquery_test.sh merge
bash scripts/gcp_bigquery_test.sh analytics
bash scripts/gcp_bigquery_test.sh evidence
```

Use the `evidence` command output, the saved files under `bonus/gcp_evidence/`, the GCS bucket object list, and the BigQuery table preview pages for submission screenshots.

## Analytical Queries

Run `main/task_3_analytic_queries.sql` after the curated BigQuery tables are loaded. It contains:

- March 2026 volume by status.
- Withdrawal approval rate by client type.
- March first-deposit retention into March/April activity.

## Production Controls Implemented

- Bronze/Silver/Gold layout.
- Immutable source handling through copied incoming data.
- Source hashes, transformation-context checkpoints, and output checksums for incremental local processing.
- Row comparisons against committed bronze data, so an append transforms only new or changed rows.
- GCS upload checkpoints and immutable warehouse batch copies under `_batches/<sha256>/<original-name>.parquet`.
- Durable BigQuery `_pipeline_batches` ledger committed atomically with raw and curated data.
- Idempotent BigQuery `MERGE` by `CODE`.
- Duplicate handling with deterministic latest-record logic.
- Data quality checks for nulls, uniqueness, allowed values, decimal parsing, and referential integrity.
- JSONL logs and JSON data-quality reports.
- Atomic Parquet writes.
- Environment-separated processing directories with `--env`.
- Optional GCS upload.
- Optional BigQuery bronze/silver/gold external table provisioning in `bonus/bigquery_production_layers.sql`.
- Optional PySpark production-scale ETL artifact in `bonus/pyspark/de_test_pyspark_etl.py`.
- Optional Kafka run-report publishing when Kafka environment variables are configured.
- Airflow DAG and Databricks Delta SQL artifacts under `bonus`.
- Shared local run/reset lock and 15-second progress heartbeats during long processing/cloud commands.

## Incremental Behavior And Reset

| Situation | Behavior |
| --- | --- |
| Same inputs on the next run | Existing CSV copies, Parquet files, and row audit timestamps are preserved. No repeated object uploads or warehouse loads. |
| New `playertransactions_*.csv` | Automatically discovered and processed; assignment filenames are preserved in silver/GCS. The three supplied files remain required. |
| Rows appended to an existing CSV | Only rows absent from committed bronze data are transformed. The affected per-file Parquet snapshot is updated; unrelated files remain untouched. |
| Updated transaction | Latest `LASTUPDATEDATE`, then `INGESTION_TIMESTAMP`, wins locally. BigQuery upserts the assignment's mutable fields, retaining `DWH_CREATE_TIME`. |
| Archived extra batch or rows removed from a CSV | Previously processed records remain. Source disappearance is not a delete event. |
| Local validation failure | Curated outputs and source checkpoints are not advanced. |
| Interrupted write/upload/load | Output checksums detect partial writes. Successful uploads are remembered individually. Failed BigQuery transactions do not mark batches loaded. Rerun the same command. |
| Explicit interval changes | The local transformation context changes, so unchanged file hashes cannot suppress the new interval. Previously loaded rows remain. |

Parquet files are immutable file formats: updating one record requires replacing the affected snapshot. Gold fact/dimension snapshots are refreshed only when their contents change. Normal runs do not rewrite every file. Immutable GCS batch copies preserve the warehouse input for retries; the required assignment paths remain available for review.

`make reset` is the full rebuild boundary. It removes only the pipeline's Parquet objects (including `_batches`), drops its three core tables, batch ledger, and bronze/silver/gold external tables, then removes local generated data/checkpoints. The bucket, datasets, original CSVs, unrelated objects, and saved submission evidence remain. The next `make run` provisions missing tables and loads all source data again. Reset is explicit and destructive for the pipeline's generated data; it is never part of a normal run. Cloud retention/soft-delete policies may retain noncurrent object versions.

```bash
make reset          # Clear generated GCS, BigQuery, and local pipeline data
make run            # Rebuild from source after the explicit reset
make reset-local    # Clear local output only; retain all warehouse data
```

Keep `_state` on persistent storage and run one orchestrator per bucket/dataset. The Airflow DAG uses `max_active_runs=1`; the host command also rejects overlapping runs/resets for the same local environment. Separate deployed environments must use separate buckets/datasets as well as `PIPELINE_ENV`. Host file locks are not distributed locks. The BigQuery batch ledger survives local state loss and prevents a local rebuild from appending already-loaded batches again.

The first run after upgrading from the earlier implementation validates existing output checksums, establishes upload checkpoints, and registers the warehouse baseline. This may upload existing snapshots once; existing BigQuery rows are merged without truncation. Later unchanged runs skip those operations. Manually deleting remote data bypasses checkpoints: use `make reset` for a coordinated rebuild.

The default executable transformation remains pandas, with GCS and BigQuery as the production path. The Airflow DAG orchestrates this same complete path. PySpark and Databricks are optional deployment artifacts; the assignment does not require a Spark cluster. This repository does not claim that merely enabling a feature flag starts a Spark or Airflow service.

The optional PySpark script keeps its own checkpoint at `main/processing/<env>/spark/_state/checkpoint.json`. Changed batches create immutable snapshots under `spark/runs/<run-id>`; unchanged runs return without starting a Spark session. The checkpoint identifies the current dimension/fact paths, and older snapshots remain until reset. Spark output is separate from the assignment's single-file pandas outputs. For a custom `DATASET`, external layer datasets are named `<DATASET>_bronze`, `<DATASET>_silver`, and `<DATASET>_gold`; the default keeps `de_test_bronze`, `de_test_silver`, and `de_test_gold`.

BigQuery's [multi-statement transactions](https://docs.cloud.google.com/bigquery/docs/transactions) provide the atomic warehouse commit used here. The driver materializes immutable external batches into temporary native tables before starting that transaction.

## Failure Scenarios Covered

- Duplicate transaction delivery: latest record wins before local gold output and before BigQuery merge.
- Source column removed or renamed: strict schema validation fails the run.
- Source column added: strict mode fails; additive mode can be used intentionally.
- Bad dates or amounts: the run fails before writing curated outputs.
- Unknown client parameter references: the run fails referential integrity.
- Partial reruns: unchanged files are skipped using hash state; `--force` reprocesses all.
- Late-arriving updates: raw BigQuery data is merged by ingestion interval while preserving original `REQUESTDATE` partitioning.
- Low or suspicious row counts: reports include input/output counts for monitoring thresholds.
- Bad dashboard numbers: Power BI should point to curated/gold models only, with refresh filters on `REQUESTDATE`.

## How To Test

1. Install dependencies.
2. Use `make reset-local` only when you need a clean local test. A reset is not needed between ordinary runs.
3. Run `make run-fast` for local-only validation, or `make run` for full GCS/BigQuery validation.
4. Confirm these files exist:

```bash
ls main/processing/dev/silver
ls main/processing/dev/gold
ls main/processing/dev/reports
```

5. Run the same command again. Logs should show `file_skipped`, `gold_build_skipped`, and (for GCP) `gcs_upload_skipped` plus `0 new/changed batches`.
6. Run `make test`.
7. For BigQuery, run the DDL, load the Parquet files, run the merge with an interval covering the source `INGESTION_TIMESTAMP`, then run the analytics SQL.

For a detailed review/test walkthrough with file references and screenshot queries, see [the incremental pipeline guide](bonus/incremental_pipeline_review.md).

## CI/CD And Feature Branches

Run `make check` for syntax checks, regression tests, and an isolated CLI smoke
run that verifies incremental rerun behavior. GitHub Actions also validates Python
3.10/3.12 and the Docker image on feature branches, develop, master and pull requests.
After a validated master push, it publishes a commit-tagged image to GHCR.
See [CI/CD and release instructions](docs/ci-cd.md) for checks, branch promotion,
required-check configuration, and the boundary between image delivery and deployment.
