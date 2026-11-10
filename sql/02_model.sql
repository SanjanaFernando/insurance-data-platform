-- Star schema + cleaning views. Run AFTER the raw tables exist (load_raw.py).
USE SCHEMA insurance_dw.core;

-- ---------- dimensions ----------
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_key INT AUTOINCREMENT START 1 INCREMENT 1,
  customer_id  INT,
  full_name    VARCHAR(100),
  city         VARCHAR(50),
  valid_from   DATE,
  valid_to     DATE,
  is_current   BOOLEAN
);

CREATE OR REPLACE TABLE dim_policy AS
SELECT ROW_NUMBER() OVER (ORDER BY policy_id) AS policy_key,
       policy_id::INT AS policy_id, customer_id::INT AS customer_id, policy_type,
       annual_premium::DECIMAL(10,2) AS annual_premium,
       start_date::DATE AS start_date, end_date::DATE AS end_date
FROM insurance_dw.raw.policies;

-- ---------- staging: customers (dedupe + standardise) ----------
CREATE OR REPLACE VIEW insurance_dw.staging.customers_incoming AS
SELECT customer_id::INT AS customer_id, TRIM(full_name) AS full_name, INITCAP(TRIM(city)) AS city
FROM insurance_dw.raw.customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC) = 1;

-- ---------- SCD Type 2 for dim_customer ----------
-- First run: effective date far in the past so every historical claim finds a customer version.
-- Later runs: SET effective_date = CURRENT_DATE();
SET effective_date = '1900-01-01';

-- step 1: close the current version of customers whose attributes changed
UPDATE dim_customer d
SET valid_to = DATEADD(day, -1, TO_DATE($effective_date)), is_current = FALSE
FROM insurance_dw.staging.customers_incoming s
WHERE d.customer_id = s.customer_id AND d.is_current
  AND (d.city IS DISTINCT FROM s.city OR d.full_name IS DISTINCT FROM s.full_name);

-- step 2: insert a new current version for new customers AND for the ones just closed
INSERT INTO dim_customer (customer_id, full_name, city, valid_from, valid_to, is_current)
SELECT s.customer_id, s.full_name, s.city, TO_DATE($effective_date), '9999-12-31'::DATE, TRUE
FROM insurance_dw.staging.customers_incoming s
LEFT JOIN dim_customer d ON d.customer_id = s.customer_id AND d.is_current
WHERE d.customer_id IS NULL;

-- ---------- staging: claims (clean + dedupe + attach dimension keys) ----------
CREATE OR REPLACE VIEW insurance_dw.staging.claims_clean AS
WITH base AS (
  SELECT claim_id::INT AS claim_id, policy_id::INT AS policy_id,
         TRY_CAST(claim_date AS DATE) AS claim_date,
         TRY_CAST(claim_amount AS DECIMAL(12,2)) AS claim_amount,
         COALESCE(UPPER(TRIM(status)), 'UNKNOWN') AS status
  FROM insurance_dw.raw.claims
),
valid AS (
  SELECT * FROM base
  WHERE claim_id IS NOT NULL AND claim_date IS NOT NULL AND claim_amount > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY claim_date DESC) = 1
)
SELECT v.claim_id, p.policy_key, c.customer_key,
       YEAR(v.claim_date) * 10000 + MONTH(v.claim_date) * 100 + DAY(v.claim_date) AS date_key,
       v.claim_amount, v.status
FROM valid v
JOIN core.dim_policy p   ON p.policy_id = v.policy_id
JOIN core.dim_customer c ON c.customer_id = p.customer_id
                        AND v.claim_date BETWEEN c.valid_from AND c.valid_to;

-- ---------- fact table + idempotent incremental load ----------
CREATE TABLE IF NOT EXISTS fact_claims (
  claim_id     INT,
  policy_key   INT,
  customer_key INT,
  date_key     INT,
  claim_amount DECIMAL(12,2),
  status       VARCHAR(15)
);

MERGE INTO fact_claims f
USING insurance_dw.staging.claims_clean s ON f.claim_id = s.claim_id
WHEN MATCHED THEN UPDATE SET
  policy_key = s.policy_key, customer_key = s.customer_key, date_key = s.date_key,
  claim_amount = s.claim_amount, status = s.status
WHEN NOT MATCHED THEN INSERT (claim_id, policy_key, customer_key, date_key, claim_amount, status)
  VALUES (s.claim_id, s.policy_key, s.customer_key, s.date_key, s.claim_amount, s.status);
