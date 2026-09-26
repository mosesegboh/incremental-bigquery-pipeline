-- Query 1: Volume Analysis
-- March 2026 transaction count and requested amount per status.
SELECT
  STATUS AS status,
  COUNT(*) AS transaction_count,
  SUM(AMOUNT) AS total_requested_amount
FROM `de-test-project.de_test_dataset.playertransactions`
WHERE REQUESTDATE >= TIMESTAMP('2026-03-01 00:00:00 UTC')
  AND REQUESTDATE < TIMESTAMP('2026-04-01 00:00:00 UTC')
GROUP BY status
ORDER BY transaction_count DESC;


-- Query 2: Approval Rate
-- Withdrawal approval rate for each client type.
SELECT
  cp.CLIENTTYPE AS client_type,
  SAFE_DIVIDE(
    COUNTIF(pt.STATUS = 'approved'),
    COUNT(*)
  ) AS withdraw_approval_rate
FROM `de-test-project.de_test_dataset.playertransactions` AS pt
JOIN `de-test-project.de_test_dataset.clientparameters` AS cp
  ON pt.CLIENTPARAMETERCODE = cp.CODE
WHERE pt.TYPE = 'withdraw'
GROUP BY client_type
ORDER BY client_type;


-- Query 3: First-deposit Retention
-- Players whose source-designated first deposit was in March 2026 and who made
-- at least one additional transaction in March or April 2026.
WITH first_march_depositors AS (
  SELECT
    CODE AS first_deposit_code,
    PLAYERCODE,
    REQUESTDATE AS first_deposit_requestdate
  FROM `de-test-project.de_test_dataset.playertransactions`
  WHERE TYPE = 'deposit'
    AND FIRSTDEPOSIT = 1
    AND REQUESTDATE >= TIMESTAMP('2026-03-01 00:00:00 UTC')
    AND REQUESTDATE < TIMESTAMP('2026-04-01 00:00:00 UTC')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY PLAYERCODE
    ORDER BY REQUESTDATE ASC, CODE ASC
  ) = 1
),
retained_players AS (
  SELECT DISTINCT
    fd.PLAYERCODE
  FROM first_march_depositors AS fd
  JOIN `de-test-project.de_test_dataset.playertransactions` AS pt
    ON pt.PLAYERCODE = fd.PLAYERCODE
   AND pt.CODE != fd.first_deposit_code
   AND pt.REQUESTDATE >= TIMESTAMP('2026-03-01 00:00:00 UTC')
   AND pt.REQUESTDATE < TIMESTAMP('2026-05-01 00:00:00 UTC')
)
SELECT
  COUNT(rp.PLAYERCODE) AS retained_players,
  COUNT(fd.PLAYERCODE) AS first_depositors,
  SAFE_DIVIDE(COUNT(rp.PLAYERCODE), COUNT(fd.PLAYERCODE)) AS retention_rate
FROM first_march_depositors AS fd
LEFT JOIN retained_players AS rp
  ON fd.PLAYERCODE = rp.PLAYERCODE;
