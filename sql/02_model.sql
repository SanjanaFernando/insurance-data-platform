-- Build the core star schema and staging views after load_raw.py has loaded RAW.
-- Run this entire file in one Snowsight worksheet as a role with access to INSURANCE_DW.
USE DATABASE insurance_dw;
USE SCHEMA core;

-- ---------- dimensions ----------
CREATE TABLE IF NOT EXISTS insurance_dw.core.dim_customer (
  customer_key INT AUTOINCREMENT START 1 INCREMENT 1,
  customer_id  INT,
  full_name    VARCHAR(100),
  city         VARCHAR(50),
  valid_from   DATE,
  valid_to     DATE,
  is_current   BOOLEAN
);

CREATE OR REPLACE TABLE insurance_dw.core.dim_policy AS
SELECT
  ROW_NUMBER() OVER (ORDER BY policy_id) AS policy_key,
  policy_id::INT AS policy_id,
  customer_id::INT AS customer_id,
  policy_type,
  annual_premium::DECIMAL(10,2) AS annual_premium,
  start_date::DATE AS start_date,
  end_date::DATE AS end_date
FROM insurance_dw.raw.policies;

-- ---------- staging: customers (deduplicate and standardize) ----------
CREATE OR REPLACE VIEW insurance_dw.staging.customers_incoming AS
SELECT
  customer_id::INT AS customer_id,
  TRIM(full_name) AS full_name,
  INITCAP(TRIM(city)) AS city
FROM insurance_dw.raw.customers
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY TRY_TO_DATE(signup_date) DESC NULLS LAST
) = 1;

-- ---------- SCD Type 2 for dim_customer ----------
-- The first load backdates the initial version to cover historical claims.
-- On later loads, change the expression to CURRENT_DATE().
SET effective_date = '1900-01-01';

-- Close changed current versions on the day before the new version starts.
UPDATE insurance_dw.core.dim_customer AS d
SET
  valid_to = DATEADD(day, -1, TO_DATE($effective_date)),
  is_current = FALSE
FROM insurance_dw.staging.customers_incoming AS s
WHERE d.customer_id = s.customer_id
  AND d.is_current = TRUE
  AND (
    d.city IS DISTINCT FROM s.city
    OR d.full_name IS DISTINCT FROM s.full_name
  );

-- Insert first versions and replacement versions for changed customers.
INSERT INTO insurance_dw.core.dim_customer
  (customer_id, full_name, city, valid_from, valid_to, is_current)
SELECT
  s.customer_id,
  s.full_name,
  s.city,
  TO_DATE($effective_date),
  '9999-12-31'::DATE,
  TRUE
FROM insurance_dw.staging.customers_incoming AS s
LEFT JOIN insurance_dw.core.dim_customer AS d
  ON d.customer_id = s.customer_id
  AND d.is_current = TRUE
WHERE d.customer_id IS NULL;

-- ---------- staging: claims (clean, deduplicate, attach dimension keys) ----------
CREATE OR REPLACE VIEW insurance_dw.staging.claims_clean AS
WITH base AS (
  SELECT
    TRY_TO_NUMBER(claim_id)::INT AS claim_id,
    TRY_TO_NUMBER(policy_id)::INT AS policy_id,
    TRY_TO_DATE(claim_date) AS claim_date,
    TRY_TO_DECIMAL(claim_amount, 12, 2) AS claim_amount,
    COALESCE(UPPER(TRIM(status)), 'UNKNOWN') AS status
  FROM insurance_dw.raw.claims
),
valid AS (
  SELECT *
  FROM base
  WHERE claim_id IS NOT NULL
    AND policy_id IS NOT NULL
    AND claim_date IS NOT NULL
    AND claim_amount > 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY claim_id
    ORDER BY claim_date DESC
  ) = 1
)
SELECT
  v.claim_id,
  p.policy_key,
  c.customer_key,
  YEAR(v.claim_date) * 10000
    + MONTH(v.claim_date) * 100
    + DAY(v.claim_date) AS date_key,
  v.claim_amount,
  v.status
FROM valid AS v
JOIN insurance_dw.core.dim_policy AS p
  ON p.policy_id = v.policy_id
JOIN insurance_dw.core.dim_customer AS c
  ON c.customer_id = p.customer_id
  AND v.claim_date BETWEEN c.valid_from AND c.valid_to;

-- ---------- fact table and idempotent incremental load ----------
CREATE TABLE IF NOT EXISTS insurance_dw.core.fact_claims (
  claim_id     INT,
  policy_key   INT,
  customer_key INT,
  date_key     INT,
  claim_amount DECIMAL(12,2),
  status       VARCHAR(15)
);

MERGE INTO insurance_dw.core.fact_claims AS f
USING insurance_dw.staging.claims_clean AS s
  ON f.claim_id = s.claim_id
WHEN MATCHED THEN UPDATE SET
  policy_key = s.policy_key,
  customer_key = s.customer_key,
  date_key = s.date_key,
  claim_amount = s.claim_amount,
  status = s.status
WHEN NOT MATCHED THEN INSERT (
  claim_id, policy_key, customer_key, date_key, claim_amount, status
) VALUES (
  s.claim_id, s.policy_key, s.customer_key, s.date_key, s.claim_amount, s.status
);
