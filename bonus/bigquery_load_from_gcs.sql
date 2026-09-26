-- Executed by scripts/bigquery_incremental.py. The driver supplies only pending,
-- immutable GCS batch URIs and creates temporary client_batch/transaction_batch.
-- Raw data, curated data and the batch ledger commit together, or all roll back.
BEGIN TRANSACTION;

MERGE `de-test-project.de_test_dataset.clientparameters` AS target
USING (
  SELECT * FROM client_batch
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CODE ORDER BY LASTMODIFICATIONDATE DESC
  ) = 1
) AS source
ON target.CODE = source.CODE
WHEN MATCHED AND source.LASTMODIFICATIONDATE >= target.LASTMODIFICATIONDATE
  AND TO_JSON_STRING(source) != TO_JSON_STRING(target)
THEN UPDATE SET
  CLIENTTYPE = source.CLIENTTYPE,
  CLIENTPLATFORM = source.CLIENTPLATFORM,
  NOTREAL = source.NOTREAL,
  LASTMODIFICATIONDATE = source.LASTMODIFICATIONDATE
WHEN NOT MATCHED THEN INSERT ROW;

MERGE `de-test-project.de_test_dataset.playertransactions_raw` AS target
USING (SELECT DISTINCT * FROM transaction_batch) AS source
ON TO_JSON_STRING(target) = TO_JSON_STRING(source)
WHEN NOT MATCHED THEN INSERT ROW;

-- MERGE_CURATED: the driver inserts the assignment's parameterized MERGE here.

MERGE `de-test-project.de_test_dataset._pipeline_batches` AS target
USING (
  SELECT JSON_VALUE(item, '$.uri') AS uri, JSON_VALUE(item, '$.sha256') AS sha256
  FROM UNNEST(JSON_QUERY_ARRAY(@batch_manifest)) AS item
) AS source
ON target.uri = source.uri AND target.sha256 = source.sha256
WHEN NOT MATCHED THEN
  INSERT (uri, sha256, loaded_at) VALUES (source.uri, source.sha256, CURRENT_TIMESTAMP());

COMMIT TRANSACTION;
