#!/usr/bin/env python3
"""Exercise the assignment MERGE against temporary BigQuery tables only."""

import os
from pathlib import Path

from bigquery_incremental import ROOT, assignment_merge, run_command, settings
from task_1_data_cleaner import load_env_files


def main() -> None:
    load_env_files(ROOT, Path(os.environ["ENV_FILE"]) if os.environ.get("ENV_FILE") else None)
    project, dataset, region, _ = settings()
    merge = (assignment_merge()
             .replace("`de-test-project.de_test_dataset.playertransactions_raw`", "raw")
             .replace("`de-test-project.de_test_dataset.playertransactions`", "curated")
             .replace("@data_interval_start", "interval_start")
             .replace("@data_interval_end", "interval_end"))
    sql = f"""
DECLARE interval_start TIMESTAMP DEFAULT TIMESTAMP '2026-05-18 16:00:00+00';
DECLARE interval_end TIMESTAMP DEFAULT TIMESTAMP '2026-05-18 17:00:00+00';
CREATE TEMP TABLE raw AS SELECT * FROM `{project}.{dataset}.playertransactions_raw` WHERE FALSE;
CREATE TEMP TABLE curated AS SELECT * FROM `{project}.{dataset}.playertransactions` WHERE FALSE;
INSERT INTO raw VALUES (
  1, 100, TIMESTAMP '2026-03-01 00:00:00+00', NULL, NUMERIC '10',
  'deposit', 'waiting', 'VISA', NULL, 'GB', 1, 1,
  TIMESTAMP '2026-03-01 00:00:01+00', TIMESTAMP '2026-05-18 16:10:00+00'
);
{merge}
ASSERT (SELECT COUNT(*) = 1 FROM curated) AS 'New transaction inserted';
CREATE TEMP TABLE before_replay AS SELECT * FROM curated;
{merge}
ASSERT (SELECT TO_JSON_STRING(c) = TO_JSON_STRING(b) FROM curated c CROSS JOIN before_replay b)
  AS 'Replay preserves all fields and audit timestamps';

INSERT INTO raw SELECT * REPLACE (
  'approved' AS STATUS, NUMERIC '20' AS AMOUNT,
  TIMESTAMP '2026-03-02 00:00:00+00' AS LASTUPDATEDATE,
  TIMESTAMP '2026-05-18 17:10:00+00' AS INGESTION_TIMESTAMP
) FROM raw;
SET interval_start = TIMESTAMP '2026-05-18 17:00:00+00';
SET interval_end = TIMESTAMP '2026-05-18 18:00:00+00';
{merge}
ASSERT (SELECT STATUS = 'approved' AND AMOUNT = 20 FROM curated) AS 'New version updates fields';
ASSERT (SELECT c.DWH_CREATE_TIME = b.DWH_CREATE_TIME AND c.DWH_UPDATE_TIME > b.DWH_UPDATE_TIME
        FROM curated c CROSS JOIN before_replay b) AS 'Creation retained; update time advanced';
CREATE TEMP TABLE after_update AS SELECT * FROM curated;

-- Same source update time but older ingestion, delivered in a backfill file.
INSERT INTO raw SELECT * REPLACE (
  'declined' AS STATUS, TIMESTAMP '2026-05-18 16:30:00+00' AS INGESTION_TIMESTAMP
) FROM raw WHERE STATUS = 'approved';
SET interval_start = TIMESTAMP '2026-05-18 16:00:00+00';
SET interval_end = TIMESTAMP '2026-05-18 17:00:00+00';
{merge}
ASSERT (SELECT TO_JSON_STRING(c) = TO_JSON_STRING(b) FROM curated c CROSS JOIN after_update b)
  AS 'Older ingestion replay cannot undo the newer version';

INSERT INTO raw SELECT * REPLACE (
  TIMESTAMP '2026-05-18 18:10:00+00' AS INGESTION_TIMESTAMP
) FROM raw WHERE STATUS = 'waiting';
SET interval_start = TIMESTAMP '2026-05-18 18:00:00+00';
SET interval_end = TIMESTAMP '2026-05-18 19:00:00+00';
{merge}
ASSERT (SELECT TO_JSON_STRING(c) = TO_JSON_STRING(b) FROM curated c CROSS JOIN after_update b)
  AS 'Late delivery of an older source version cannot regress the target';
SELECT 'passed' AS incremental_merge_checks;
"""
    result = run_command(["bq", f"--project_id={project}", f"--location={region}",
                          "query", "--use_legacy_sql=false"], input_text=sql)
    evidence = ROOT / "bonus/gcp_evidence/incremental_merge_verification.txt"
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(result)
    print(f"Temporary-table BigQuery assertions passed. Evidence: {evidence}", flush=True)


if __name__ == "__main__":
    main()
