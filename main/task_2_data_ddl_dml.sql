-- BigQuery implementation for the de-test assignment.
-- Replace `de-test-project.de_test_dataset` with the target project and dataset
-- if the reviewer uses a different sandbox.

CREATE SCHEMA IF NOT EXISTS `de-test-project.de_test_dataset`
OPTIONS(location = 'EU');

CREATE TABLE IF NOT EXISTS `de-test-project.de_test_dataset.clientparameters` (
  CODE INT64 NOT NULL,
  CLIENTTYPE STRING NOT NULL,
  CLIENTPLATFORM STRING NOT NULL,
  NOTREAL INT64 NOT NULL,
  LASTMODIFICATIONDATE TIMESTAMP NOT NULL
)
OPTIONS(
  description = 'Curated client parameter configurations.'
);

CREATE TABLE IF NOT EXISTS `de-test-project.de_test_dataset.playertransactions_raw` (
  CODE INT64 NOT NULL,
  PLAYERCODE INT64 NOT NULL,
  REQUESTDATE TIMESTAMP NOT NULL,
  ACCEPTDATE TIMESTAMP,
  AMOUNT NUMERIC NOT NULL,
  TYPE STRING NOT NULL,
  STATUS STRING NOT NULL,
  PAYMENTMETHOD STRING NOT NULL,
  PAYMENTSUBMETHOD STRING,
  COUNTRY STRING,
  CLIENTPARAMETERCODE INT64 NOT NULL,
  FIRSTDEPOSIT INT64 NOT NULL,
  LASTUPDATEDATE TIMESTAMP NOT NULL,
  INGESTION_TIMESTAMP TIMESTAMP NOT NULL
)
PARTITION BY DATE(INGESTION_TIMESTAMP)
CLUSTER BY CODE, PLAYERCODE, CLIENTPARAMETERCODE
OPTIONS(
  description = 'Raw transaction staging table loaded from cleaned Parquet batches.'
);

CREATE TABLE IF NOT EXISTS `de-test-project.de_test_dataset.playertransactions` (
  CODE INT64 NOT NULL,
  PLAYERCODE INT64 NOT NULL,
  REQUESTDATE TIMESTAMP NOT NULL,
  ACCEPTDATE TIMESTAMP,
  AMOUNT NUMERIC NOT NULL,
  TYPE STRING NOT NULL,
  STATUS STRING NOT NULL,
  PAYMENTMETHOD STRING NOT NULL,
  PAYMENTSUBMETHOD STRING,
  COUNTRY STRING,
  CLIENTPARAMETERCODE INT64 NOT NULL,
  FIRSTDEPOSIT INT64 NOT NULL,
  LASTUPDATEDATE TIMESTAMP NOT NULL,
  DWH_CREATE_TIME TIMESTAMP NOT NULL,
  DWH_UPDATE_TIME TIMESTAMP NOT NULL
)
PARTITION BY DATE(REQUESTDATE)
CLUSTER BY CODE, PLAYERCODE, CLIENTPARAMETERCODE
OPTIONS(
  description = 'Curated player transactions. One latest record per transaction CODE.'
);

-- Runtime parameters expected from the orchestrator:
--   @data_interval_start TIMESTAMP inclusive
--   @data_interval_end   TIMESTAMP exclusive
--
-- The source query deduplicates the hourly raw slice so the merge is idempotent
-- if the same batch is replayed or if Kafka/GCS/load jobs deliver duplicates.
MERGE `de-test-project.de_test_dataset.playertransactions` AS target
USING (
  SELECT
    CODE,
    PLAYERCODE,
    REQUESTDATE,
    ACCEPTDATE,
    AMOUNT,
    TYPE,
    STATUS,
    PAYMENTMETHOD,
    PAYMENTSUBMETHOD,
    COUNTRY,
    CLIENTPARAMETERCODE,
    FIRSTDEPOSIT,
    LASTUPDATEDATE
  FROM (
    SELECT
      raw.*,
      ROW_NUMBER() OVER (
        PARTITION BY CODE
        ORDER BY LASTUPDATEDATE DESC, INGESTION_TIMESTAMP DESC
      ) AS row_num
    FROM `de-test-project.de_test_dataset.playertransactions_raw` AS raw
    WHERE raw.INGESTION_TIMESTAMP >= @data_interval_start
      AND raw.INGESTION_TIMESTAMP < @data_interval_end
      -- An older replay must not undo a newer version already landed in raw.
      AND NOT EXISTS (
        SELECT 1
        FROM `de-test-project.de_test_dataset.playertransactions_raw` AS newer
        WHERE newer.CODE = raw.CODE
          AND (
            newer.LASTUPDATEDATE > raw.LASTUPDATEDATE
            OR (newer.LASTUPDATEDATE = raw.LASTUPDATEDATE
                AND newer.INGESTION_TIMESTAMP > raw.INGESTION_TIMESTAMP)
          )
      )
  )
  WHERE row_num = 1
) AS source
ON target.CODE = source.CODE
WHEN MATCHED AND source.LASTUPDATEDATE >= target.LASTUPDATEDATE
  AND (
    source.STATUS IS DISTINCT FROM target.STATUS
    OR source.AMOUNT IS DISTINCT FROM target.AMOUNT
    OR source.ACCEPTDATE IS DISTINCT FROM target.ACCEPTDATE
    OR source.LASTUPDATEDATE IS DISTINCT FROM target.LASTUPDATEDATE
  ) THEN UPDATE SET
  STATUS = source.STATUS,
  AMOUNT = source.AMOUNT,
  ACCEPTDATE = source.ACCEPTDATE,
  LASTUPDATEDATE = source.LASTUPDATEDATE,
  DWH_UPDATE_TIME = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  CODE,
  PLAYERCODE,
  REQUESTDATE,
  ACCEPTDATE,
  AMOUNT,
  TYPE,
  STATUS,
  PAYMENTMETHOD,
  PAYMENTSUBMETHOD,
  COUNTRY,
  CLIENTPARAMETERCODE,
  FIRSTDEPOSIT,
  LASTUPDATEDATE,
  DWH_CREATE_TIME,
  DWH_UPDATE_TIME
) VALUES (
  source.CODE,
  source.PLAYERCODE,
  source.REQUESTDATE,
  source.ACCEPTDATE,
  source.AMOUNT,
  source.TYPE,
  source.STATUS,
  source.PAYMENTMETHOD,
  source.PAYMENTSUBMETHOD,
  source.COUNTRY,
  source.CLIENTPARAMETERCODE,
  source.FIRSTDEPOSIT,
  source.LASTUPDATEDATE,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);
