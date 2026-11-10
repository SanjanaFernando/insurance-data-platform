-- Idempotent incremental load of fact_claims (used by the Airflow DAG and by Project 1).
MERGE INTO insurance_dw.core.fact_claims f
USING insurance_dw.staging.claims_clean s ON f.claim_id = s.claim_id
WHEN MATCHED THEN UPDATE SET
  policy_key = s.policy_key, customer_key = s.customer_key, date_key = s.date_key,
  claim_amount = s.claim_amount, status = s.status
WHEN NOT MATCHED THEN INSERT (claim_id, policy_key, customer_key, date_key, claim_amount, status)
  VALUES (s.claim_id, s.policy_key, s.customer_key, s.date_key, s.claim_amount, s.status);
