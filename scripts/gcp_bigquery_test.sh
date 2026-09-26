#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
COMMAND="${1:-all}"

if [[ -n "${PYTHON:-}" ]]; then
  PYTHON_BIN="$PYTHON"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

export ENV_FILE
ENV_EXPORTS="$("$PYTHON_BIN" -c '
import shlex, sys
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[1]) / "main"))
from task_1_data_cleaner import load_env_files
for name, value in load_env_files(Path(sys.argv[1]), Path(sys.argv[2])).items():
    print("export " + name + "=" + shlex.quote(value))
' "$ROOT_DIR" "$ENV_FILE")"
eval "$ENV_EXPORTS"

PROJECT_ID="${PROJECT_ID:?PROJECT_ID must be set in .env or the environment}"
DATASET="${DATASET:-de_test_dataset}"
LAYER_PREFIX="$DATASET"
[[ "$DATASET" != de_test_dataset ]] || LAYER_PREFIX=de_test
REGION="${REGION:-EU}"
BUCKET="${GCS_BUCKET:-${BUCKET:-$PROJECT_ID-de-test-bucket}}"
export PROJECT_ID DATASET REGION BUCKET
export GCS_BUCKET="$BUCKET"
DATA_INTERVAL_START="${DATA_INTERVAL_START:-}"
DATA_INTERVAL_END="${DATA_INTERVAL_END:-}"
EVIDENCE_DIR="${EVIDENCE_DIR:-$ROOT_DIR/bonus/gcp_evidence}"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# The same environment lock is inherited by the cleaner and warehouse helper.
PROCESSING_DIR="${PROCESSING_DIR:-$ROOT_DIR/main/processing}"
[[ "$PROCESSING_DIR" = /* ]] || PROCESSING_DIR="$ROOT_DIR/$PROCESSING_DIR"
PIPELINE_ENV="${PIPELINE_ENV:-${ENV:-dev}}"
mkdir -p "$PROCESSING_DIR"
exec 9>"$PROCESSING_DIR/.$PIPELINE_ENV.lock"
flock -n 9 || { echo "Another pipeline/reset is running for $PIPELINE_ENV." >&2; exit 1; }
export PIPELINE_LOCK_FD=9

progress() {
  "$PYTHON_BIN" -c '
import sys
sys.path.insert(0, sys.argv[1])
from bigquery_incremental import run_command
print(run_command(sys.argv[2:]), end="", flush=True)
' "$ROOT_DIR/scripts" "$@"
}

log_step() {
  printf '\n[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command is missing: $1" >&2
    exit 1
  fi
}

render_sql() {
  local input_file="$1"
  local output_file="$2"
  sed \
    -e "s/de-test-project/$PROJECT_ID/g" \
    -e "s/de_test_dataset/$DATASET/g" \
    -e "s/de_test_bronze/${LAYER_PREFIX}_bronze/g" \
    -e "s/de_test_silver/${LAYER_PREFIX}_silver/g" \
    -e "s/de_test_gold/${LAYER_PREFIX}_gold/g" \
    -e "s/location = 'EU'/location = '$REGION'/g" \
    -e "s/de-test-bucket/$BUCKET/g" \
    "$input_file" > "$output_file"
}

bootstrap() {
  log_step "Bootstrap: validating gcloud, selecting project, enabling APIs, and checking bucket."
  require_command gcloud
  gcloud config set project "$PROJECT_ID"
  progress gcloud services enable bigquery.googleapis.com storage.googleapis.com --project="$PROJECT_ID"
  if ! gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1; then
    log_step "Bootstrap: creating GCS bucket gs://$BUCKET in $REGION."
    gcloud storage buckets create "gs://$BUCKET" \
      --location="$REGION" \
      --uniform-bucket-level-access
  fi
  log_step "Bootstrap complete."
}

run_cleaner_with_gcs_upload() {
  log_step "Upload: running cleaner and uploading Parquet outputs to gs://$BUCKET."
  NO_PRODUCTION_LAYERS=false DATA_INTERVAL_START= DATA_INTERVAL_END= "$PYTHON_BIN" "$ROOT_DIR/main/task_1_data_cleaner.py" \
    --with-s3 \
    --gcs-bucket "$BUCKET"
  log_step "Upload complete."
}

run_ddl() {
  log_step "BigQuery DDL: creating required assignment dataset and tables."
  require_command bq
  sed '/^-- Runtime parameters/,$d' "$ROOT_DIR/main/task_2_data_ddl_dml.sql" > "$TMP_DIR/ddl_template.sql"
  render_sql "$TMP_DIR/ddl_template.sql" "$TMP_DIR/ddl.sql"
  progress bq --project_id="$PROJECT_ID" --location="$REGION" query --use_legacy_sql=false < "$TMP_DIR/ddl.sql"
  log_step "BigQuery DDL complete."
}

run_load() {
  log_step "BigQuery load: checking the batch ledger, loading pending data, and merging atomically."
  require_command bq
  "$PYTHON_BIN" "$ROOT_DIR/scripts/bigquery_incremental.py" load
  log_step "BigQuery load complete."
}

run_layers() {
  log_step "BigQuery production layers: provisioning optional bronze/silver/gold external tables."
  require_command bq
  render_sql "$ROOT_DIR/bonus/bigquery_production_layers.sql" "$TMP_DIR/layers.sql"
  progress bq --project_id="$PROJECT_ID" --location="$REGION" query --use_legacy_sql=false < "$TMP_DIR/layers.sql"
  log_step "BigQuery production layers complete."
}

run_merge() {
  log_step "BigQuery merge: upserting raw transactions into the curated table."
  require_command bq
  : "${DATA_INTERVAL_START:?Set DATA_INTERVAL_START for an explicit hourly replay}"
  : "${DATA_INTERVAL_END:?Set DATA_INTERVAL_END for an explicit hourly replay}"
  sed -n '/^MERGE /,$p' "$ROOT_DIR/main/task_2_data_ddl_dml.sql" > "$TMP_DIR/merge_template.sql"
  render_sql "$TMP_DIR/merge_template.sql" "$TMP_DIR/merge.sql"
  progress bq --project_id="$PROJECT_ID" --location="$REGION" query --use_legacy_sql=false \
    --parameter=data_interval_start:TIMESTAMP:"$DATA_INTERVAL_START" \
    --parameter=data_interval_end:TIMESTAMP:"$DATA_INTERVAL_END" \
    < "$TMP_DIR/merge.sql"
  log_step "BigQuery merge complete."
}

run_analytics() {
  log_step "Analytics: running the required Task 3 analytical queries."
  require_command bq
  render_sql "$ROOT_DIR/main/task_3_analytic_queries.sql" "$TMP_DIR/analytics.sql"
  progress bq --project_id="$PROJECT_ID" --location="$REGION" query --use_legacy_sql=false < "$TMP_DIR/analytics.sql"
  log_step "Analytics complete."
}

run_evidence() {
  log_step "Evidence: printing GCS objects, BigQuery tables, and row counts for screenshots."
  require_command gcloud
  require_command bq
  mkdir -p "$EVIDENCE_DIR"
  local evidence_run_id
  local gcs_file
  local tables_file
  local counts_file
  evidence_run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
  gcs_file="$EVIDENCE_DIR/${evidence_run_id}_gcs_objects.txt"
  tables_file="$EVIDENCE_DIR/${evidence_run_id}_bigquery_tables.txt"
  counts_file="$EVIDENCE_DIR/${evidence_run_id}_bigquery_counts.txt"

  if ! gcloud storage ls -r "gs://$BUCKET/**" > "$gcs_file" 2>&1; then
    cat "$gcs_file"
  else
    cat "$gcs_file"
  fi

  bq --project_id="$PROJECT_ID" ls "$DATASET" > "$tables_file"
  cat "$tables_file"

  bq --project_id="$PROJECT_ID" --location="$REGION" query --use_legacy_sql=false \
    "SELECT 'clientparameters' AS table_name, COUNT(*) AS row_count FROM \`$PROJECT_ID.$DATASET.clientparameters\`
     UNION ALL
     SELECT 'playertransactions_raw', COUNT(*) FROM \`$PROJECT_ID.$DATASET.playertransactions_raw\`
     UNION ALL
     SELECT 'playertransactions', COUNT(*) FROM \`$PROJECT_ID.$DATASET.playertransactions\`" > "$counts_file"
  cat "$counts_file"
  log_step "Evidence complete. Files saved under $EVIDENCE_DIR."
}

case "$COMMAND" in
  reset)
    "$PYTHON_BIN" "$ROOT_DIR/scripts/bigquery_incremental.py" reset
    ;;
  bootstrap)
    bootstrap
    ;;
  upload)
    run_cleaner_with_gcs_upload
    ;;
  ddl)
    run_ddl
    ;;
  load)
    run_load
    ;;
  layers)
    run_layers
    ;;
  merge)
    run_merge
    ;;
  analytics)
    run_analytics
    ;;
  evidence)
    run_evidence
    ;;
  all)
    bootstrap
    run_cleaner_with_gcs_upload
    run_ddl
    run_layers
    run_load
    run_analytics
    run_evidence
    ;;
  *)
    echo "Usage: $0 [bootstrap|upload|ddl|layers|load|merge|analytics|evidence|reset|all]" >&2
    exit 1
    ;;
esac
