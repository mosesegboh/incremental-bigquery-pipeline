-- Optional production lakehouse layer provisioning for BigQuery.
-- Render de-test-project/de-test-bucket to the real project and bucket before running.

CREATE SCHEMA IF NOT EXISTS `de-test-project.de_test_bronze`
OPTIONS(location = 'EU');

CREATE SCHEMA IF NOT EXISTS `de-test-project.de_test_silver`
OPTIONS(location = 'EU');

CREATE SCHEMA IF NOT EXISTS `de-test-project.de_test_gold`
OPTIONS(location = 'EU');

CREATE EXTERNAL TABLE IF NOT EXISTS `de-test-project.de_test_bronze.clientparameters`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-test-bucket/bronze/clientparameters/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `de-test-project.de_test_bronze.playertransactions`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-test-bucket/bronze/playertransactions/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `de-test-project.de_test_silver.clientparameters`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-test-bucket/silver/clientparameters/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `de-test-project.de_test_silver.playertransactions`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-test-bucket/silver/playertransactions/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `de-test-project.de_test_gold.dim_clientparameters`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-test-bucket/gold/clientparameters/dim_clientparameters.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `de-test-project.de_test_gold.fact_playertransactions`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-test-bucket/gold/playertransactions/fact_playertransactions.parquet']
);
