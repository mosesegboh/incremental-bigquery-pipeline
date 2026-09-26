# Power BI Integration Notes

No `.pbix` file was supplied, so this project cannot directly update a Power BI report artifact. The intended Power BI source should be the curated/gold model, not raw CSV files.

## Recommended Source

Preferred production source:

- BigQuery table `de-test-project.de_test_dataset.playertransactions`
- BigQuery table `de-test-project.de_test_dataset.clientparameters`

Local review source:

- `main/processing/dev/gold/playertransactions.parquet`
- `main/processing/dev/gold/clientparameters.parquet`

## Incremental Refresh

Configure Power BI incremental refresh on `REQUESTDATE`:

- `RangeStart`: datetime parameter
- `RangeEnd`: datetime parameter
- Filter: `REQUESTDATE >= RangeStart and REQUESTDATE < RangeEnd`

The warehouse table is partitioned by `REQUESTDATE`, so this keeps refreshes aligned with the BigQuery partitioning strategy.

## Suggested Measures

```DAX
Transaction Count = COUNTROWS(playertransactions)

Total Requested Amount = SUM(playertransactions[AMOUNT])

Withdrawal Approval Rate =
DIVIDE(
    CALCULATE(COUNTROWS(playertransactions), playertransactions[STATUS] = "approved"),
    COUNTROWS(playertransactions)
)
```

Apply a report/page filter where `TYPE = "withdraw"` for the withdrawal approval-rate visual, or embed that predicate in a dedicated measure.

## Validation Workflow

1. Refresh Power BI after `make run` or after the BigQuery merge.
2. Compare March status counts with Query 1 in `main/task_3_analytic_queries.sql`.
3. Compare withdrawal approval rates with Query 2.
4. Confirm report filters use `REQUESTDATE`, not `INGESTION_TIMESTAMP`, for business-period analysis.
