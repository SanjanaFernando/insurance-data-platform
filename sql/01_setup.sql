-- Run once in a Snowsight worksheet (trial account, role ACCOUNTADMIN)
CREATE WAREHOUSE IF NOT EXISTS wh_xs
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE wh_xs;

CREATE DATABASE IF NOT EXISTS insurance_dw;
CREATE SCHEMA IF NOT EXISTS insurance_dw.raw;       -- landed data, all VARCHAR
CREATE SCHEMA IF NOT EXISTS insurance_dw.staging;   -- cleaning views
CREATE SCHEMA IF NOT EXISTS insurance_dw.core;      -- star schema
USE SCHEMA insurance_dw.core;

-- Date dimension (Snowflake-specific generator; 2024-01-01 .. 2026-12-31 = 1096 days)
CREATE OR REPLACE TABLE dim_date AS
SELECT
  YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d) AS date_key,
  d AS full_date, YEAR(d) AS year, QUARTER(d) AS quarter, MONTH(d) AS month,
  MONTHNAME(d) AS month_name, DAYOFWEEK(d) AS day_of_week
FROM (SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2024-01-01'::DATE) AS d
      FROM TABLE(GENERATOR(ROWCOUNT => 1096)));
