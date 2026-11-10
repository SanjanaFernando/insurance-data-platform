USE SCHEMA insurance_dw.core;

-- Q1 Claims by policy type and quarter
SELECT p.policy_type, d.year, d.quarter, COUNT(*) AS claims, SUM(f.claim_amount) AS total_amount
FROM fact_claims f
JOIN dim_policy p ON p.policy_key = f.policy_key
JOIN dim_date d   ON d.date_key = f.date_key
GROUP BY p.policy_type, d.year, d.quarter
ORDER BY p.policy_type, d.year, d.quarter;

-- Q2 Running total of paid claims by month
WITH monthly AS (
  SELECT d.year, d.month, SUM(f.claim_amount) AS paid_amount
  FROM fact_claims f JOIN dim_date d ON d.date_key = f.date_key
  WHERE f.status = 'PAID'
  GROUP BY d.year, d.month
)
SELECT year, month, paid_amount,
       SUM(paid_amount) OVER (ORDER BY year, month
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM monthly ORDER BY year, month;

-- Q3 Month-over-month change in claim amount (LAG)
WITH monthly AS (
  SELECT d.year, d.month, SUM(f.claim_amount) AS total_amount
  FROM fact_claims f JOIN dim_date d ON d.date_key = f.date_key
  GROUP BY d.year, d.month
)
SELECT year, month, total_amount,
       LAG(total_amount) OVER (ORDER BY year, month) AS prev_month,
       ROUND((total_amount - LAG(total_amount) OVER (ORDER BY year, month))
             * 100.0 / LAG(total_amount) OVER (ORDER BY year, month), 1) AS pct_change
FROM monthly ORDER BY year, month;

-- Q4 Top 3 customers by total claim amount in each city (DENSE_RANK)
WITH totals AS (
  SELECT c.city, c.customer_id, c.full_name, SUM(f.claim_amount) AS total_amount
  FROM fact_claims f JOIN dim_customer c ON c.customer_key = f.customer_key
  GROUP BY c.city, c.customer_id, c.full_name
),
ranked AS (
  SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY total_amount DESC) AS rnk FROM totals
)
SELECT city, rnk, full_name, total_amount FROM ranked WHERE rnk <= 3 ORDER BY city, rnk;

-- Q5 Approval rate by policy type (conditional aggregation)
SELECT p.policy_type, COUNT(*) AS claims,
       ROUND(100.0 * SUM(CASE WHEN f.status IN ('APPROVED','PAID') THEN 1 ELSE 0 END) / COUNT(*), 1) AS approval_pct
FROM fact_claims f JOIN dim_policy p ON p.policy_key = f.policy_key
GROUP BY p.policy_type ORDER BY approval_pct DESC;

-- Q6 Claims more than 3x the average for their policy type (window average)
WITH x AS (
  SELECT f.claim_id, p.policy_type, f.claim_amount,
         AVG(f.claim_amount) OVER (PARTITION BY p.policy_type) AS type_avg
  FROM fact_claims f JOIN dim_policy p ON p.policy_key = f.policy_key
)
SELECT claim_id, policy_type, claim_amount, ROUND(type_avg, 2) AS type_avg
FROM x WHERE claim_amount > 3 * type_avg ORDER BY claim_amount DESC LIMIT 10;

-- Q7 Share of total claim amount by city (percent of total)
SELECT c.city, SUM(f.claim_amount) AS total_amount,
       ROUND(100.0 * SUM(f.claim_amount) / SUM(SUM(f.claim_amount)) OVER (), 1) AS pct_of_total
FROM fact_claims f JOIN dim_customer c ON c.customer_key = f.customer_key
GROUP BY c.city ORDER BY total_amount DESC;

-- Q8 SCD2 in action: customers who moved city, with claims under each address
SELECT c.customer_id, c.city, c.valid_from, c.valid_to, c.is_current,
       COUNT(f.claim_id) AS claims
FROM dim_customer c
LEFT JOIN fact_claims f ON f.customer_key = c.customer_key
WHERE c.customer_id IN (SELECT customer_id FROM dim_customer GROUP BY customer_id HAVING COUNT(*) > 1)
GROUP BY c.customer_id, c.city, c.valid_from, c.valid_to, c.is_current
ORDER BY c.customer_id, c.valid_from LIMIT 12;
