# Preamble

The goal of this test assignment is to assess your knowledge of the main steps in an ETL (or an ELT) flow: processing data files, landing them in a data storage system, implementing a database layer and additionally writing analytic queries based on the loaded data. Although the preferred database engine for these tasks is BigQuery, you can use PostgreSQL as an alternative. If using PostgreSQL, adapt the syntax accordingly.

The project has been structured for you:
  - In the `main` folder there are files we expect you to fill based on the scripts and queries you write. Read the description of each task carefully to understand what to add to these files.
  - In the `main/data` folder, there are three CSV files:
    - Dimensional data: `clientparameters.csv` contains information about the supported operator client type and platform combinations.
    - Incremental data: `playertransactions_2026_03.csv`, `playertransactions_2026_04.csv` contain transactional data about deposits and withdrawals.
  - The solution to Bonus Task 2 can be added to the `bonus` folder (see task description for more details).
  - The file `config.txt` states the database engine used for any submitted SQL script. If you prefer using PostgreSQL instead of BigQuery, change the contents of this file to `dbengine=postgresql`.

Please note that Bonus Task 1 is directly related to the Google Cloud Platform (GCP). If possible, we highly recommend setting up a free Google Cloud trial to validate your work (in case you do choose the BigQuery engine for your assignment). Read more about the trial [here](https://docs.cloud.google.com/free/docs/free-cloud-features).

After you have added the required content, compress the project back into a ZIP file which begins with your name (`FirstnameLastname_de_test_assignment.zip`) and submit it as instructed by the recruiter.

**We don't expect perfection – we want to see your approach and reasoning. Be prepared to explain and make minor changes in your solutions during the interview.**


# Task 1: Data Cleaning & Landing (Python)

**Write a Python script that:**

1. Reads all three of the provided CSV files.
2. Cleans the incremental data:
  - Standardizes all dates to the format `yyyy-MM-dd HH:mm:ss`.
  - Strips currency symbols from the amount and ensures it is a valid decimal number (with `.` used as the decimal separator).
  - Standardizes transaction status to lowercase (e.g., `waiting`, `approved`, `declined`).
3. Saves the files in Parquet format (same file names as were provided).

**[Bonus Task 1] You get extra points if:**

- The script uploads the cleaned files to their corresponding folder in Google Cloud Storage (GCS).
  - Dimensional data must be uploaded to the `de-test-bucket/clientparameters` folder.
  - Incremental data must be uploaded to the `de-test-bucket/playertransactions` folder.
- We suggest naming your GCP project `de-test-project` and the GCS bucket as `de-test-bucket`. PS! Do not share your personal Google credentials with us!

**Add your code to the file `main/task_1_data_cleaner.py`.**
Make sure to include a `requirements.txt` file as well so we are aware of the dependencies when running your script.


# Task 2: Schema Design & Incremental Load (BigQuery/SQL)

**Implement the database layer in BigQuery:**

1. DDL: Write the SQL to create 3 tables:
  - `clientparameters`: The curated table for client parameters.
  - `playertransactions_raw`: A staging table for transactions.
  - `playertransactions`: The curated table for transactions. Partitioned by `REQUESTDATE` (daily partitions).
2. DML: Write a SQL MERGE statement that:
  - Upserts the latest data from `playertransactions_raw` into `playertransactions`, matching on `CODE`. You can assume that this merge runs hourly, with the source query taking only what has been ingested into the raw table within the last hour. For setting the bounds, you can define timestamp variables `data_interval_start` and `data_interval_end` as placeholders that will be substituted at runtime.
    - If a record exists, update its `STATUS`, `AMOUNT`, `ACCEPTDATE`, `LASTUPDATEDATE` and `DWH_UPDATE_TIME`.
    - If a record is new, insert it and set a `DWH_CREATE_TIME` timestamp (`DWH_UPDATE_TIME` = `DWH_CREATE_TIME`).

**[Bonus Task 2] You get extra points if:**

* You are also able to perform the intermediate step of inserting data into the `playertransactions_raw` and `clientparameters` tables. Based on the database engine you chose, there are multiple ways to achieve this. We also accept screenshots of your activity in Google Cloud UIs or any PostgreSQL client.

**Add your DDL/DML scripts from steps 1-2 to the file `main/task_2_data_ddl_dml.sql`.** 
If you choose to perform the bonus step as well, add the related pictures and/or scripts to the `bonus` folder.


# Task 3: Analytical Queries (SQL)

**Write SQL queries based on the created curated tables:**

1. **Volume Analysis.** Calculate the total transactions count (`transaction_count`) and amount (`total_requested_amount`) per status for March 2026. Return the result in descending order, by the count of transactions.
2. **Approval Rate.** Calculate the approval rate of withdrawals for each client type (`withdraw_approval_rate`).
3. **First-deposit Retention.** Of the players who made their very first deposit in March 2026, what percentage made at least one additional transaction in either March or April of 2026? Return the count of retained players and total first depositors, plus the retention rate (`retained_players`, `first_depositors`, `retention_rate`).

**Add your queries to the file `main/task_3_analytic_queries.sql`.**


# Appendix

## Destination table schemas

### clientparameters

| Field name | Type | Mode | Description |
| :--- | :--- | :--- | :--- |
| CODE |  INTEGER | REQUIRED | Unique identifier for the client parameter configuration. |
| CLIENTTYPE | STRING | REQUIRED | The category of the gaming client (e.g., casino, poker, bingo). |
| CLIENTPLATFORM | STRING | REQUIRED | The platform used by the client (e.g., download, flash, mobile). |
| NOTREAL | INTEGER | REQUIRED | Indicator for test or non-real money transactions (0 = Real, 1 = Not Real). |
| LASTMODIFICATIONDATE | TIMESTAMP | REQUIRED | The date and time when the record was last modified in the source system. |

### playertransactions_raw

| Field name | Type | Mode | Description |
| :--- | :--- | :--- | :--- |
| CODE |  INTEGER | REQUIRED | Unique identifier for the transaction. |
| PLAYERCODE | INTEGER | REQUIRED | Unique identifier for the player. |
| REQUESTDATE | TIMESTAMP | REQUIRED | The date and time the transaction was requested. |
| ACCEPTDATE | TIMESTAMP | NULLABLE | The date and time the transaction was processed or accepted. |
| AMOUNT | NUMERIC | REQUIRED | The monetary value of the transaction. |
| TYPE | STRING | REQUIRED | The type of transaction (e.g., deposit, withdraw). |
| STATUS | STRING | REQUIRED | The current state of the transaction (e.g., approved, waiting, declined). |
| PAYMENTMETHOD | STRING | REQUIRED | The primary payment method used. |
| PAYMENTSUBMETHOD | STRING | NULLABLE | The specific provider or sub-method used (e.g., PayPal). |
| COUNTRY | STRING | NULLABLE | The country associated with the player or transaction. |
| CLIENTPARAMETERCODE | INTEGER | REQUIRED | Reference to the client configuration. |
| FIRSTDEPOSIT | INTEGER | REQUIRED | Indicator if this is the player's first deposit (1 = Yes, 0 = No). |
| LASTUPDATEDATE | TIMESTAMP | REQUIRED | The date and time the record was last updated in the source system. |
| INGESTION_TIMESTAMP | TIMESTAMP | REQUIRED | The timestamp when the record was loaded into the raw table. |

### playertransactions

| Field name | Type | Mode | Description |
| :--- | :--- | :--- | :--- |
| CODE |  INTEGER | REQUIRED | Unique identifier for the transaction. |
| PLAYERCODE | INTEGER | REQUIRED | Unique identifier for the player. |
| REQUESTDATE | TIMESTAMP | REQUIRED | The date and time the transaction was requested. |
| ACCEPTDATE | TIMESTAMP | NULLABLE | The date and time the transaction was processed or accepted. |
| AMOUNT | NUMERIC | REQUIRED | The monetary value of the transaction. |
| TYPE | STRING | REQUIRED | The type of transaction (e.g., deposit, withdraw). |
| STATUS | STRING | REQUIRED | The current state of the transaction (e.g., approved, waiting, declined). |
| PAYMENTMETHOD | STRING | REQUIRED | The primary payment method used. |
| PAYMENTSUBMETHOD | STRING | NULLABLE | The specific provider or sub-method used (e.g., PayPal). |
| COUNTRY | STRING | NULLABLE | The country associated with the player or transaction. |
| CLIENTPARAMETERCODE | INTEGER | REQUIRED | Reference to the client configuration (FK to clientparameters). |
| FIRSTDEPOSIT | INTEGER | REQUIRED | Indicator if this is the player's first deposit (1 = Yes, 0 = No). |
| LASTUPDATEDATE | TIMESTAMP | REQUIRED | The date and time the record was last updated in the source system. |
| DWH_CREATE_TIME | TIMESTAMP | REQUIRED | The timestamp when the record was first loaded into the data warehouse. |
| DWH_UPDATE_TIME | TIMESTAMP | REQUIRED | The timestamp when the record was last updated in the data warehouse. |