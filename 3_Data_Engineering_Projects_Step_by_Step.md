# 3 Data Engineering Projects in 3 Days

*Gallagher Data Engineer (L1) preparation*

A step-by-step build guide: Snowflake warehouse, an Airflow pipeline with PySpark (no Azure needed), and an API pipeline with dbt and CI.

| Day    | Date       | What you finish                                               | Time      |
|--------|------------|---------------------------------------------------------------|-----------|
| 1      | Mon Nov 9  | Setup + **Project 1**: Snowflake claims warehouse             | 7 to 8 h  |
| 2      | Tue Nov 10 | **Project 2**: Airflow (Docker) + PySpark + Snowflake         | 5 to 6 h  |
| 3      | Wed Nov 11 | **Project 3**: API + dbt + GitHub Actions, then update the CV | 6 to 7 h  |
| Buffer | Thu Nov 12 | Fix problems, polish READMEs, upload the CV                   | as needed |

## Read this first

- **One repository, three projects.** Everything lives in one GitHub repo called `insurance-data-platform` that uses the same synthetic insurance data throughout. The starter code is in the zip file that came with this guide.
- **What was tested.** I ran the data generator, the SQL logic (in DuckDB, not Snowflake), the PySpark cleaning script, the extractor and pipeline unit tests, the Airflow DAG in a real Airflow 2.11 (outside Docker), and the whole dbt project (17 of 17 steps passed, in DuckDB). The checkpoint numbers in this guide come from those runs. If yours differ by a little, check for a mistake, but do not panic.
- **What was NOT tested.** I cannot reach Snowflake from here, and there is no Docker engine in my environment. So the Snowflake-only SQL (such as the date dimension), the Snowflake steps of the DAG (tested only against fake connections), the Docker image build and the CI workflow are written from my knowledge of the products and have not been run for real. Screen names in Snowsight and Docker change, so use the descriptions as a guide, and fix small errors using the troubleshooting page at the end.
- **Claim only what you build.** The CV bullets at the end are written for the finished projects. Delete any bullet for a part you did not complete. A smaller honest project beats a large one you cannot explain.
- **Ask me.** If any step fails, copy the error message to me and I will help you fix it.

## Open these accounts now (20 minutes)

- **GitHub**: create a **public** repo named `insurance-data-platform`, so recruiters can open it from your CV.
- **Snowflake trial** (needed for all three projects): sign up and choose the Standard edition on Azure or AWS, whichever region is closest. The trial gives about \$400 of credit for 30 days. Signing in to Snowsight will ask you to set up a second factor (an authenticator app).
- **Docker Desktop** (Project 2): free for personal use. Install it now and make sure it starts (Windows needs WSL2 turned on). You do not need an Azure account for anything in this guide.

> **Watch out**
>
> Snowflake is phasing out password-only sign-in for many accounts. Its documentation says trial accounts are not covered, so your scripts should work with a password. If a script fails with an authentication or MFA error, switch to key-pair authentication (see the troubleshooting page).

# Setup (about 45 minutes)

### Step 1: Install the tools

- Python 3.11 or newer, Git, and VS Code.
- Docker Desktop for Project 2 (about 6 GB of free memory is comfortable). Java is only needed if you run PySpark outside Docker.

### Step 2: Create the project folder and the Python environment

Unzip `insurance-data-platform-starter.zip`, open a terminal in the unzipped folder, and run:

```bash
python -m venv .venv
# Windows PowerShell:  .venv\Scripts\Activate.ps1
# Mac/Linux:           source .venv/bin/activate
pip install -r requirements.txt
```

Installing `pyspark` and `dbt-snowflake` takes a few minutes. Start it now and carry on.

### Step 3: Generate the dataset

```bash
python generate_data.py
```

> **Checkpoint: you should see**
>
>     {'claims_part1.csv': 2100, 'claims_part2.csv': 920, 'customers.csv': 510, 'customers_changes.csv': 40, 'policies.csv': 800}

The data has deliberate problems for you to fix: duplicate customers, negative claim amounts, missing and mixed-case statuses, and 20 claims that are sent twice (they appear in both claim files). The files are already in the `data/` folder of the zip too.

### Step 4: Create the GitHub repo and push

```bash
git init
git add .
git commit -m "Starter: data generator and project scaffolding"
git branch -M main
git remote add origin https://github.com/<your-username>/insurance-data-platform.git
git push -u origin main
```

Commit after every project so the history shows steady progress. Add a `.gitignore` containing `.venv/`, `out/` and `target/`.

### Step 5: Run the Snowflake setup

In Snowsight open **Worksheets**, create a new SQL worksheet, paste `sql/01_setup.sql` and run all of it (the arrow next to the Run button has a **Run All** option).

```sql
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
```

> **Checkpoint: you should see**
>
> The last statement creates `core.dim_date`. Run `SELECT COUNT(*) FROM insurance_dw.core.dim_date;` and expect 1096 rows.

> **Watch out**
>
> The warehouse suspends itself after 60 seconds. That is what keeps the credit from draining. Do not create a larger warehouse.

### Step 6: Set your Snowflake login in the terminal

Your account identifier is in Snowsight under your name at the bottom left, then **Account**, then **View account details** (format `orgname-accountname`).

```bash
# Windows PowerShell
$env:SNOWFLAKE_ACCOUNT="orgname-accountname"
$env:SNOWFLAKE_USER="YOUR_USER"
$env:SNOWFLAKE_PASSWORD="your-password"

# Mac/Linux
export SNOWFLAKE_ACCOUNT=orgname-accountname
export SNOWFLAKE_USER=YOUR_USER
export SNOWFLAKE_PASSWORD='your-password'
```

Never write these into a file that you commit.

# Project 1: Insurance claims warehouse in Snowflake

**Goal:** turn messy raw files into a clean star schema with an incremental, re-runnable load and a customer history table (SCD Type 2). **Skills it proves:** SQL, dimensional modeling, MERGE, window functions, Snowflake basics. **Time:** 4 to 5 hours.

```diagram
raw CSV files --(load_raw.py)--> RAW schema (all VARCHAR)
                                   |
                    STAGING views (clean, dedupe, cast)
                                   |
   dim_customer (SCD2)   dim_policy   dim_date
                    \         |         /
                      fact_claims  (MERGE load)
```

### Step 1: Load the raw files into Snowflake

```bash
python load_raw.py
```

`load_raw.py` reads each CSV as text and writes it to a table in the `RAW` schema with `write_pandas`. Raw tables are all `VARCHAR` on purpose: a bad value can never make the load fail, and cleaning happens later in views.

```sql
SELECT COUNT(*) AS rows, COUNT(DISTINCT customer_id) AS customers FROM insurance_dw.raw.customers;
SELECT COUNT(*) FROM insurance_dw.raw.claims;
```

> **Checkpoint: you should see**
>
> 510 rows and 500 distinct customers (10 are duplicates), and 2100 claims.

### Step 2: Create the dimensions

Paste and run this part of `sql/02_model.sql`. `dim_customer` is empty for now. `dim_policy` is built straight from the raw policies.

```sql
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
```

### Step 3: Create the cleaning view for customers

```sql
-- ---------- staging: customers (dedupe + standardise) ----------
CREATE OR REPLACE VIEW insurance_dw.staging.customers_incoming AS
SELECT customer_id::INT AS customer_id, TRIM(full_name) AS full_name, INITCAP(TRIM(city)) AS city
FROM insurance_dw.raw.customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC) = 1;
```

`QUALIFY ROW_NUMBER() ... = 1` keeps one row per customer, which removes the duplicates. This is the standard Snowflake deduplication pattern.

### Step 4: Load dim_customer as SCD Type 2

SCD Type 2 keeps history: when a customer changes city, the old row is closed (`valid_to` is set, `is_current` becomes false) and a new row is added. Two statements do it. Run the `SET` line and both statements together.

```sql
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
```

> **Checkpoint: you should see**
>
> ```
> SELECT COUNT(*), COUNT(DISTINCT customer_id), SUM(IFF(is_current,1,0)) FROM insurance_dw.core.dim_customer;
> ```
>
> Expect 500, 500 and 500. The first load uses 1900-01-01 as the start date so every old claim finds a customer row.

> **Say this in the interview**
>
> "I keep full customer history with SCD Type 2, so a claim from last year still points to the address the customer had then."

### Step 5: Create the claims cleaning view

This view fixes everything wrong with the raw claims in one place: it casts types, upper-cases statuses, replaces a missing status with `UNKNOWN`, drops negative or invalid rows, removes duplicate claims, and looks up the right dimension keys (including the right customer version for the claim date).

```sql
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
```

### Step 6: Create the fact table and load it with MERGE

```sql
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
```

MERGE updates a claim that already exists and inserts a new one, so running it twice never creates duplicates. That property is called **idempotency**.

```sql
SELECT COUNT(*), COUNT(DISTINCT claim_id) FROM insurance_dw.core.fact_claims;
```

> **Checkpoint: you should see**
>
> Expect about **2076** rows, and both numbers equal. Run the MERGE a second time and the count must not change.

### Step 7: Load the second claims file (the incremental load)

```bash
python load_raw.py --part2
```

This appends `claims_part2.csv` (920 rows, including 20 that were already in part 1). Now run only the MERGE statement again:

```sql
MERGE INTO fact_claims f
USING insurance_dw.staging.claims_clean s ON f.claim_id = s.claim_id
WHEN MATCHED THEN UPDATE SET
  policy_key = s.policy_key, customer_key = s.customer_key, date_key = s.date_key,
  claim_amount = s.claim_amount, status = s.status
WHEN NOT MATCHED THEN INSERT (claim_id, policy_key, customer_key, date_key, claim_amount, status)
  VALUES (s.claim_id, s.policy_key, s.customer_key, s.date_key, s.claim_amount, s.status);
```

> **Checkpoint: you should see**
>
> Expect **2970** rows. The 20 re-sent claims did not create duplicates. Take a screenshot of this count: it is the evidence for your CV.

> **Say this in the interview**
>
> "New files can overlap with old ones, so I load with MERGE on the claim ID. Re-running the load is safe."

### Step 8: Show SCD Type 2 working

Pretend the customers file for a later day arrives with 40 customers who moved city. Point the staging view at that table and run the two SCD statements again, this time with today's date:

```sql
CREATE OR REPLACE VIEW insurance_dw.staging.customers_incoming AS
SELECT customer_id::INT AS customer_id, TRIM(full_name) AS full_name, INITCAP(TRIM(city)) AS city
FROM insurance_dw.raw.customers_changes;

SET effective_date = CURRENT_DATE();
-- now re-run the UPDATE and the INSERT statements from step 4
```

> **Checkpoint: you should see**
>
> Run the count query from step 4 again: expect **540** rows and **500** current. The 40 moved customers now have two rows each. The new row shows zero claims until new claims arrive after today, which is correct.

### Step 9: Run the analytics queries

`sql/03_queries.sql` contains 8 queries. Run them one at a time and read the results.

- Q1: claims by policy type and quarter (joins across the star schema).
- Q2: running total of paid claims by month (`SUM() OVER`).
- Q3: month-over-month change (`LAG`).
- Q4: top 3 customers per city (`DENSE_RANK`).
- Q5: approval rate by policy type (conditional aggregation).
- Q6: claims above 3 times the average for their policy type (window average).
- Q7: share of total by city (percent of total).
- Q8: customers who moved city and the claims under each address (SCD2).

```sql
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
```

Q4 is shown above as an example. Every query is in the zip.

### Step 10: Explore performance (30 minutes)

```sql
-- 1) Look at the plan
EXPLAIN SELECT p.policy_type, SUM(f.claim_amount)
FROM insurance_dw.core.fact_claims f JOIN insurance_dw.core.dim_policy p ON p.policy_key = f.policy_key
GROUP BY p.policy_type;

-- 2) Cluster the fact table by date and inspect it
ALTER TABLE insurance_dw.core.fact_claims CLUSTER BY (date_key);
SELECT SYSTEM$CLUSTERING_INFORMATION('insurance_dw.core.fact_claims', '(date_key)');

-- 3) Time Travel: the table as it was 5 minutes ago
SELECT COUNT(*) FROM insurance_dw.core.fact_claims AT(OFFSET => -300);
```

Run one of the queries, then open **Query History**, click the query, and open the **Query Profile** tab. Take a screenshot.

> **Watch out**
>
> With only 3,000 rows you will not see a speed difference from clustering. Do not claim an improvement. Say what you learned: Snowflake has no indexes, it stores data in micro-partitions and prunes them using min/max metadata, and clustering helps on very large tables filtered by the clustering key.

> **Say this in the interview**
>
> "Snowflake does not use indexes like SQL Server. It prunes micro-partitions, and I checked partition pruning in the Query Profile."

### Step 11: Write the README and push

- Copy `README.md` from the zip and fill in the architecture diagram (draw boxes in draw.io or Excalidraw and export a PNG into `docs/`).
- Add screenshots: the 2970 row count, the SCD2 query (Q8), and one Query Profile.
- `git add . && git commit -m "Project 1: Snowflake claims warehouse" && git push`

> **Checkpoint: you should see**
>
> Project 1 is done when: the fact table has 2970 rows, re-running the MERGE changes nothing, dim_customer has 540 rows with 500 current, and the repo has a README with screenshots.

# Project 2: Airflow medallion pipeline to Snowflake (Docker, PySpark)

**Goal:** an orchestrated pipeline that lands files in a raw zone, validates them, cleans them with PySpark into a silver layer, loads Snowflake and reconciles the result. Local folders play the role of the data lake. **Skills it proves:** Airflow (DAGs, dynamic task mapping, retries, alerts), PySpark, the medallion layers, Snowflake staging and COPY INTO, Docker. **Time:** 5 to 6 hours.

```diagram
data/*.csv --(5 mapped copy tasks)--> lake/raw/<date> --(validate)-->
   PySpark silver_clean --> lake/silver/<date>
   --(PUT to stage, COPY INTO)--> RAW.CLAIMS --(MERGE)--> fact_claims
   --(reconcile: silver count = fact count)
```

> **Be ready to explain: Airflow versus Azure Data Factory**
>
> | Azure Data Factory           | In this project (Airflow)                         |
> |------------------------------|---------------------------------------------------|
> | Pipeline                     | DAG (`claims_pipeline`)                           |
> | ForEach + Copy activity      | Dynamically mapped task `copy_to_raw.expand(...)` |
> | Schedule trigger             | `schedule="@daily"`                               |
> | Activity retry policy        | `retries=2, retry_delay`                          |
> | Failure alert (Monitor)      | `on_failure_callback`                             |
> | ADLS raw / silver containers | `lake/raw` and `lake/silver` folders              |
>
> "ADF is a managed cloud service, Airflow is open source code you run yourself. I built the same ideas in Airflow and I understand how they map to ADF."

### Step 1: Install and start Docker Desktop

- Install Docker Desktop, start it, and wait until it says it is running.
- Check in a terminal: `docker --version` and `docker compose version` both print a version.

Docker lets Airflow, Java and PySpark run in one container, so you do not install any of them on your computer.

### Step 2: Know what is in the project

- `Dockerfile` and `docker-compose.yml`: one container running Airflow in standalone mode (web UI and scheduler together). Your project folder is mounted inside it, so the DAG, the data and the output folders are shared.
- `dags/claims_pipeline.py`: the DAG.
- `pipeline/lake.py`: copy and validate files. `pipeline/snowflake_ops.py`: upload, COPY INTO, MERGE and reconcile.
- `spark/silver_clean.py`: the PySpark cleaning. `sql/04_merge_fact.sql`: the MERGE.
- `tests/test_pipeline.py`: unit tests with a fake Snowflake connection. Run `pytest -q` to see them pass (10 passed with the extractor tests).

### Step 3: Read the PySpark cleaning script

It reads all claim files, casts types, upper-cases statuses, replaces a missing status with `UNKNOWN`, separates invalid rows (negative amounts, missing keys) into a **rejects** output, removes duplicate claims, and prints a quality report. Inside Airflow, the environment variables `LAKE_RAW` and `LAKE_SILVER` tell it which folders to use.

```sql
"""Bronze -> Silver cleaning with PySpark. Runs locally and in a Databricks notebook.
Local:       python spark/silver_clean.py            (reads ./data, writes ./out/silver)
Airflow:     LAKE_RAW and LAKE_SILVER environment variables set the input and output folders
Databricks:  paste into a notebook cell and set BASE to your Volume path, e.g. /Volumes/workspace/default/raw"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
BASE = os.getenv("LAKE_RAW") or ("/Volumes/workspace/default/raw" if ON_DATABRICKS else "data")
OUT = os.getenv("LAKE_SILVER") or ("/Volumes/workspace/default/silver" if ON_DATABRICKS else "out/silver")

spark = SparkSession.builder.appName("silver_clean").getOrCreate()
read = lambda pattern: spark.read.option("header", True).csv(f"{BASE}/{pattern}")   # all strings = schema-on-read

# ---------- claims ----------
claims_raw = read("claims_part*.csv")
claims = (claims_raw
    .withColumn("claim_id", F.col("claim_id").cast("int"))
    .withColumn("policy_id", F.col("policy_id").cast("int"))
    .withColumn("claim_date", F.to_date("claim_date"))
    .withColumn("claim_amount", F.col("claim_amount").cast("double"))
    .withColumn("status", F.coalesce(F.upper(F.trim("status")), F.lit("UNKNOWN"))))

is_valid = F.col("claim_id").isNotNull() & F.col("claim_date").isNotNull() & (F.col("claim_amount") > 0)
rejects = claims.filter(~is_valid).withColumn("reject_reason",
    F.when(F.col("claim_amount") <= 0, "non_positive_amount").otherwise("missing_key_or_date"))
claims_silver = claims.filter(is_valid).dropDuplicates(["claim_id"])        # re-sent rows removed

# ---------- customers & policies ----------
customers_silver = (read("customers.csv")
    .withColumn("customer_id", F.col("customer_id").cast("int"))
    .withColumn("city", F.initcap(F.trim("city")))
    .dropDuplicates(["customer_id"]))
policies_silver = (read("policies.csv")
    .withColumn("policy_id", F.col("policy_id").cast("int"))
    .withColumn("customer_id", F.col("customer_id").cast("int"))
    .withColumn("annual_premium", F.col("annual_premium").cast("double")))

# ---------- quality report ----------
print("claims raw      :", claims_raw.count())
print("claims silver   :", claims_silver.count())
print("claims rejected :", rejects.count())
print("status values   :", sorted(r[0] for r in claims_silver.select("status").distinct().collect()))

# ---------- write ----------
def write_csv(df, name):                     # one CSV file per table, easy to upload and COPY INTO
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/{name}")

write_csv(claims_silver, "claims"); write_csv(customers_silver, "customers")
write_csv(policies_silver, "policies"); write_csv(rejects, "claims_rejected")
if ON_DATABRICKS:                            # Delta tables (Databricks only)
    claims_silver.write.format("delta").mode("overwrite").saveAsTable("silver_claims")
```

> **Say this in the interview**
>
> "Raw files are never edited. Bad rows go to a rejects output with a reason, so nothing disappears silently."

### Step 4: Read the DAG

```python
"""Airflow DAG: raw -> silver (PySpark) -> Snowflake -> fact table, with validation and reconciliation."""
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from pipeline import lake, snowflake_ops

PROJECT = "/opt/airflow/project"
FILES = ["customers.csv", "policies.csv", "claims_part1.csv", "claims_part2.csv", "customers_changes.csv"]
log = logging.getLogger("claims_pipeline")

def alert_on_failure(context):
    """Called when a task fails after all retries. Replace with an email or Slack call in real life."""
    ti = context["task_instance"]
    log.error("ALERT: task %s in DAG %s failed (run %s)", ti.task_id, ti.dag_id, context["ds"])

@dag(
    dag_id="claims_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1), "on_failure_callback": alert_on_failure},
    tags=["insurance", "medallion"],
)
def claims_pipeline():
    @task
    def copy_to_raw(fname: str, ds=None) -> str:
        return lake.copy_file(f"{PROJECT}/data", f"{PROJECT}/lake/raw/{ds}", fname)

    @task
    def validate_raw(paths) -> None:
        lake.validate_files(list(paths))

    silver_clean = BashOperator(
        task_id="silver_clean",
        bash_command=f"python {PROJECT}/spark/silver_clean.py",
        env={"LAKE_RAW": PROJECT + "/lake/raw/{{ ds }}", "LAKE_SILVER": PROJECT + "/lake/silver/{{ ds }}"},
        append_env=True,
    )

    @task
    def load_snowflake(ds=None) -> None:
        conn = snowflake_ops.connect()
        try:
            snowflake_ops.ensure_objects(conn)
            n = snowflake_ops.upload_silver(conn, f"{PROJECT}/lake/silver/{ds}")
            log.info("uploaded %s silver file(s) to the stage", n)
            snowflake_ops.copy_into_raw(conn)
            snowflake_ops.merge_fact(conn, f"{PROJECT}/sql/04_merge_fact.sql")
        finally:
            conn.close()

    @task
    def reconcile(ds=None) -> dict:
        conn = snowflake_ops.connect()
        try:
            result = snowflake_ops.reconcile(conn, f"{PROJECT}/lake/silver/{ds}")
        finally:
            conn.close()
        log.info("reconciled: %s", result)
        return result

    landed = copy_to_raw.expand(fname=FILES)
    validated = validate_raw(landed)
    validated >> silver_clean >> load_snowflake() >> reconcile()

claims_pipeline()
```

- **Dynamic task mapping**: `copy_to_raw.expand(fname=FILES)` creates one task per file at run time.
- **validate_raw** fails the run before any processing if a file is missing, empty, or has only a header.
- **silver_clean** runs the PySpark script as a separate task. The dates in the folder names come from the run date, so each run has its own raw and silver folders.
- **load_snowflake** clears the stage folder, uploads the cleaned file, truncates `RAW.CLAIMS`, runs `COPY INTO` and the MERGE.
- **reconcile** compares the clean claims in the silver file with the rows in `fact_claims` and fails if they differ.
- **retries** (2, one minute apart) and `on_failure_callback` handle failures; in real life that callback would send an email or a Slack message.

### Step 5: Prepare Snowflake and your login file

- The DAG reuses the tables from Project 1 (`RAW.CLAIMS`, `CORE.DIM_POLICY`, `CORE.DIM_CUSTOMER`, `CORE.FACT_CLAIMS` and the view `STAGING.CLAIMS_CLEAN`), so finish Project 1 first. It creates its own file format and stage.
- Copy `.env.example` to `.env` and fill in your three Snowflake values (no quotes). The `.env` file is in `.gitignore`: never commit it.

> **Watch out**
>
> This DAG replaces the contents of `RAW.CLAIMS` with the cleaned data from the lake. That is fine: the MERGE only adds or updates claims, so the fact table stays correct.

### Step 6: Build and start Airflow

```bash
docker compose up --build
```

The first build takes several minutes (it downloads Java and PySpark). When the log settles, open **http://localhost:8080**. The user is `admin`. Get the password with this command in a second terminal:

```bash
docker compose exec airflow cat standalone_admin_password.txt
```

> **Watch out**
>
> If the file is not found, search the start-up log: `docker compose logs airflow | grep -i password`. On Linux, if Airflow cannot write into `lake/`, run `chmod -R a+rwX lake`.

### Step 7: Run the pipeline

- In the DAG list find `claims_pipeline`. Turn on the **pause toggle** so it is active, then click the **play (Trigger DAG)** button.
- Open the **Graph** tab and watch the tasks turn green one after another.
- Click `silver_clean`, then **Logs**, and find the quality report.

> **Checkpoint: you should see**
>
> All tasks are green: five `copy_to_raw` instances, `validate_raw`, `silver_clean`, `load_snowflake` and `reconcile`. The silver_clean log says claims raw **3020**, claims silver **2970**, claims rejected **30**. In the folders you will find `lake/raw/<date>` and `lake/silver/<date>`.

In a Spark log, a line starting with `WARN FileStreamSink` followed by a stack trace is normal noise, not a failure. Look at the task colour.

> **Watch out**
>
> I ran this DAG in a real Airflow here, and the ingestion, validation and PySpark tasks passed. The Snowflake tasks were only tested with fake connections, so they are the most likely place for a small error. Copy any error you see into this chat and I will fix it.

### Step 8: Check Snowflake

```sql
SELECT COUNT(*), COUNT(DISTINCT claim_id) FROM insurance_dw.core.fact_claims;
LIST @insurance_dw.raw.stg_silver;
```

> **Checkpoint: you should see**
>
> The fact count is **2970**, the same as Project 1, even though the data now came through Airflow and PySpark. The stage lists a `claims/part-...csv` file. In Airflow, open the `reconcile` task, then **XCom**: `{'silver_claims': 2970, 'fact_claims': 2970}`.

> **Say this in the interview**
>
> "The same fact table comes out of two independent paths with matching counts, and the pipeline checks that itself on every run."

### Step 9: Prove it is safe to re-run

Trigger the DAG a second time (the play button again). The count must stay **2970**. This is idempotency: MERGE does not create duplicates and the stage folder is cleared before each upload.

### Step 10: Prove it handles failure

- Temporarily rename `data/policies.csv` to `data/policies_x.csv` and trigger the DAG.
- The `copy_to_raw` task for that file fails, Airflow retries it twice, then marks it failed. Open its log: it shows the retries and an `ALERT:` line from the failure callback.
- Rename the file back, open the failed task and click **Clear** to run it again; the pipeline continues.
- Take screenshots of the failed run, the retry log and the recovered run.

> **Say this in the interview**
>
> "I tested the failure path on purpose: the run stops early, retries, alerts, and I can resume from the failed task."

### Step 11: Screenshots, README, push, and stop

- Screenshots: the Graph view with green tasks, the mapped copy tasks, the quality report in the silver_clean log, the XCom reconcile result, the Snowflake count and `LIST` output, and the failure demo.
- Draw the architecture diagram (draw.io or Excalidraw) and add it to the README with a short explanation of each task.
- Commit and push: `git add . && git commit -m \"Project 2: Airflow medallion pipeline\" && git push`.
- Stop the container: `docker compose down`.

> **Checkpoint: you should see**
>
> Project 2 is done when: the DAG run is green, the quality report shows 3020 raw, 2970 silver and 30 rejected, the Snowflake fact count is 2970, a second run changes nothing, and you have seen a failure retry and alert.

# Project 3: API to Snowflake with dbt and CI

**Goal:** pull daily weather from a public API, load it to Snowflake, build tested dbt models that combine weather with claims, and run quality checks automatically on every push. **Skills it proves:** API extraction (chunking, retries, logging), dbt, data tests, CI/CD. **Time:** 5 to 6 hours.

```diagram
Open-Meteo API --(Python: monthly chunks, retries, logging)--> RAW.WEATHER_DAILY
                                                                 |
         dbt staging (stg_claims, stg_policies, stg_weather)  --  12 tests
                                                                 |
         marts: mart_claims_daily  ->  mart_rain_impact
   GitHub Actions: ruff + pytest + dbt parse on every push, dbt build on main
```

### Step 1: Check the API in your browser first

Open this link. You should see JSON with a `daily` section:

```bash
https://archive-api.open-meteo.com/v1/archive?latitude=6.9271&longitude=79.8612&start_date=2024-01-01&end_date=2024-01-05&daily=temperature_2m_max,precipitation_sum&timezone=Asia/Colombo
```

I could not call this API from my environment, so this test is yours to do. If it does not work, tell me and we will switch to another free API.

### Step 2: Read the extractor, then run it

`extractor/extract_weather.py` does three things worth explaining:

- **Chunking** (`month_chunks`): the date range is split into calendar months and fetched one month at a time.
- **Retries** (`make_session`): a requests session retries up to 5 times with backoff on 429 and 5xx errors.
- **Logging** and a duplicate-safe result (`drop_duplicates`).

```bash
python -m extractor.extract_weather 2024-01-01 2026-10-31 --load
```

> **Checkpoint: you should see**
>
> The log shows one "fetching" line per month, then `wrote 1035 rows`, and `weather_daily: loaded=True rows=1035`. The data also lands in `data/weather_daily.csv`.

> **Watch out**
>
> The newest days in the archive can lag by a few days. If the last month fails, use an end date a week earlier.

### Step 3: Run the tests

```bash
pytest -q
```

> **Checkpoint: you should see**
>
> `3 passed`. The tests check the month splitting, the shape of the data frame, and that combined chunks have no duplicate dates (the network call is replaced by a fake).

### Step 4: Run dbt against Snowflake

```bash
cd dbt_insurance
dbt debug --profiles-dir .      # checks the connection
dbt build --profiles-dir .      # runs models and tests in order
```

> **Checkpoint: you should see**
>
> All steps pass. In my DuckDB run it was **17 of 17**: 5 models and 12 tests. Models appear in the new schema `INSURANCE_DW.ANALYTICS`.

The staging models (`stg_claims`, `stg_policies`, `stg_weather`) clean and type the raw data. One example:

    select
        claim_id::int                              as claim_id,
        policy_id::int                             as policy_id,
        try_cast(claim_date as date)               as claim_date,
        try_cast(claim_amount as decimal(12, 2))   as claim_amount,
        coalesce(upper(trim(status)), 'UNKNOWN')   as status
    from {{ source('raw', 'claims') }}
    where claim_id is not null
      and try_cast(claim_date as date) is not null
      and try_cast(claim_amount as decimal(12, 2)) > 0
    qualify row_number() over (partition by claim_id order by try_cast(claim_date as date) desc) = 1

The mart combines weather and claims, one row per day, so days without claims show 0:

    -- One row per calendar day: weather plus claim activity (days with no claims show 0).
    with claims_by_day as (
        select claim_date, count(*) as claims, sum(claim_amount) as claim_amount
        from {{ ref('stg_claims') }}
        group by claim_date
    )
    select
        w.weather_date,
        w.temp_max_c,
        w.precip_mm,
        (w.precip_mm >= 10)                    as is_rainy_day,
        coalesce(c.claims, 0)                  as claims,
        coalesce(c.claim_amount, 0)            as claim_amount
    from {{ ref('stg_weather') }} w
    left join claims_by_day c on c.claim_date = w.weather_date

And the tests (the generic dbt tests `unique`, `not_null`, `relationships` and `accepted_values`) are declared in `models/schema.yml`:

```yaml
version: 2
models:
  - name: stg_claims
    columns:
      - name: claim_id
        tests: [unique, not_null]
      - name: policy_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_policies')
              field: policy_id
      - name: claim_amount
        tests: [not_null]
      - name: status
        tests:
          - accepted_values:
              values: ['OPEN', 'APPROVED', 'REJECTED', 'PAID', 'UNKNOWN']
  - name: stg_policies
    columns:
      - name: policy_id
        tests: [unique, not_null]
  - name: stg_weather
    columns:
      - name: weather_date
        tests: [unique, not_null]
  - name: mart_claims_daily
    columns:
      - name: weather_date
        tests: [unique, not_null]
```

> **Watch out**
>
> If dbt says it cannot connect, the environment variables are not set in this terminal. Set them again (Setup, step 6).

### Step 5: Read the result

```sql
SELECT * FROM insurance_dw.analytics.mart_rain_impact;
```

This compares the average number of claims on rainy days (10 mm or more) with dry days. Do not claim a conclusion: the claims are synthetic and random, so any difference is noise. The point is the pipeline, and you can say exactly that.

> **Say this in the interview**
>
> "The claim data is synthetic, so I do not claim a weather effect. I built the pipeline so that a real dataset could be dropped in."

### Step 6: Generate the dbt docs and screenshot the lineage

```bash
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

Open the page it prints, click the green lineage graph button at the bottom right, and take a screenshot of the graph.

### Step 7: Add CI with GitHub Actions

The file `.github/workflows/ci.yml` is already in the zip. It has two jobs:

- **quality** (every push): `ruff check .` for style, `pytest` for the extractor tests, `dbt parse` to validate the dbt project. It uses dummy credentials because parsing does not need a connection (I believe so, but have not run it against Snowflake).
- **dbt-build** (only on `main`): runs `dbt build` against Snowflake using repository secrets.

```yaml
name: ci
on:
  push:
  pull_request:

jobs:
  quality:
    runs-on: ubuntu-latest
    env:                      # dummy values: dbt parse does not connect to Snowflake
      SNOWFLAKE_ACCOUNT: dummy
      SNOWFLAKE_USER: dummy
      SNOWFLAKE_PASSWORD: dummy
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install ruff pytest pandas requests dbt-snowflake
      - run: ruff check .
      - run: pytest -q
      - run: dbt parse --profiles-dir .
        working-directory: dbt_insurance

  dbt-build:
    needs: quality
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    env:
      SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
      SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
      SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install dbt-snowflake
      - run: dbt build --profiles-dir .
        working-directory: dbt_insurance
```

Add the secrets in GitHub: your repo, **Settings, Secrets and variables, Actions, New repository secret**, three times: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`. Then push and open the **Actions** tab.

> **Checkpoint: you should see**
>
> Both jobs show a green tick. Take a screenshot.

> **Watch out**
>
> If `ruff` fails, run `ruff check . --fix` locally and push again. If the build job fails only on login, your Snowflake account may need key-pair login (troubleshooting page). If it keeps failing and time is short, keep the **quality** job and remove **dbt-build**, and describe only what runs.

### Step 8: README and push

Add the lineage and CI screenshots and a sentence on each design decision, then commit and push.

```bash
git add .
git commit -m "Project 3: weather API, dbt models and CI"
git push
```

> **Checkpoint: you should see**
>
> Project 3 is done when: the extractor loads about 1035 weather rows, `dbt build` passes, and the GitHub Actions run is green.

# Put it on your CV (Day 3, after the projects)

Replace the numbers in square brackets with what you really got, and delete any bullet for work you did not finish. Put these three above your AI projects in a section called **Data Engineering Projects**. Your Hospital Vital Signs pipeline stays first.

### Project 1

- Built an insurance claims data warehouse in Snowflake: star schema (fact table plus customer, policy and date dimensions) with an SCD Type 2 customer dimension that keeps address history.
- Cleaned and deduplicated about 3,000 raw claims with SQL views and loaded them with idempotent MERGE statements, so overlapping files cause no duplicates; wrote 8 analytical queries using window functions (LAG, DENSE_RANK, running totals).
- \[Optional: analysed Query Profile and clustering behaviour.\]

### Project 2

- Built a containerised Apache Airflow pipeline (Docker) following a medallion flow: dynamically mapped ingestion tasks land files in a raw zone, a validation gate stops bad runs, and PySpark produces a silver layer (deduplication, type casting, 30 invalid records isolated into a rejects output).
- Loaded Snowflake through a stage with COPY INTO and an idempotent MERGE; configured retries and failure alerts and added an automated row-count reconciliation (2,970 claims) to every run; unit tested the pipeline code with pytest.

### Project 3

- Built an API-to-warehouse pipeline: Python extractor with monthly chunking, retry with backoff and logging, loading daily weather data into Snowflake; unit tested with pytest.
- Modelled it with dbt (staging and mart layers, 12 data tests) and set up GitHub Actions CI (ruff, pytest, dbt parse, dbt build on main).

Skills line to add: **Snowflake, SQL (window functions, MERGE, SCD2), Apache Airflow, Docker, PySpark, dbt, GitHub Actions, data modeling**. List only what you actually used. Add Azure Data Factory only if you later build something with it.

Send me the three repo links and your real numbers, and I will update the CV PDF with these projects and recompile it.

## Questions you should be able to answer

- Why is the raw layer all VARCHAR? (So loads never fail on bad data; cleaning is done later, visibly, in views.)
- What does idempotent mean, and how did you get it? (Re-running gives the same result; MERGE on the natural key.)
- Explain SCD Type 2 and why a fact row joins on a date range.
- ROW_NUMBER vs RANK vs DENSE_RANK; what does QUALIFY do?
- Why does COPY INTO skip files it already loaded, and why did you use FORCE = TRUE after a TRUNCATE?
- What does Airflow do versus PySpark versus Snowflake in your pipeline? (Schedule and orchestrate, transform at scale, store and serve.) How does Airflow map to ADF?
- What does dbt add over plain SQL scripts? (Dependencies, tests, docs, lineage.)
- What would you change for real production? (A managed Airflow or ADF, a cloud object store instead of local folders, secrets in a vault, incremental dbt models, real alert channels, a bigger Spark cluster.)

# Troubleshooting

| Problem                                         | Likely cause and fix                                                                                                                                                                                                                                                                                                                                            |
|-------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| "Object does not exist" in Snowflake            | The wrong database or schema is active. Use fully qualified names (insurance_dw.core.table) or run `USE SCHEMA` first. Also check the warehouse is selected (top right).                                                                                                                                                                                        |
| Columns not found after load_raw.py             | Column names must be upper case in Snowflake. The script upper-cases them; if you loaded the CSV another way, quoted lower-case names will break unquoted queries.                                                                                                                                                                                              |
| Login or MFA error from Python or dbt           | Docs say trial accounts can still use passwords, but if you get an authentication error, switch to key-pair login: create a key with openssl, run `ALTER USER your_user SET RSA_PUBLIC_KEY='...'`, then use `private_key_file` in the Snowflake connector and `private_key_path` in the dbt profile. I have not tested this: follow Snowflake's key-pair guide. |
| MERGE count is not 2970                         | Check you loaded both claim files exactly once into RAW.CLAIMS and that dim_customer has 500 current rows before the first MERGE. Run the counts in steps 4 and 6 of Project 1.                                                                                                                                                                                 |
| docker compose up fails                         | Docker Desktop is not running, or (Windows) WSL2 is not enabled. Start Docker Desktop and wait for the running status.                                                                                                                                                                                                                                          |
| DAG does not appear in Airflow                  | The DAG file has an import error. Look for a red banner at the top of the page, or run `docker compose logs airflow`. Wait a minute: Airflow scans the folder periodically.                                                                                                                                                                                     |
| load_snowflake: No module named snowflake       | The image was built without the connector. Run `docker compose up --build` again.                                                                                                                                                                                                                                                                               |
| load_snowflake: table RAW.CLAIMS does not exist | Project 1 was not finished. Run `python load_raw.py` and the Project 1 SQL first.                                                                                                                                                                                                                                                                               |
| PUT or COPY INTO finds no files                 | The silver_clean task wrote nothing. Open its log; check the quality report and the folder `lake/silver/<date>/claims`.                                                                                                                                                                                                                                         |
| Permission denied writing lake/ (Linux)         | Run `chmod -R a+rwX lake` in the project folder.                                                                                                                                                                                                                                                                                                                |
| reconcile fails (counts differ)                 | That is the check working. Compare the silver claim count with `fact_claims`; claims whose policy or customer is missing in the dimensions are dropped by the join.                                                                                                                                                                                             |
| dbt cannot connect                              | Environment variables are not set in the same terminal, or the account identifier is wrong (format orgname-accountname).                                                                                                                                                                                                                                        |
| Open-Meteo returns errors for recent dates      | The archive lags by a few days. Use an earlier end date.                                                                                                                                                                                                                                                                                                        |
| GitHub Actions dbt-build fails on login         | Secrets are missing or misspelled, or the account needs key-pair login. Keep the quality job and drop dbt-build if time is short.                                                                                                                                                                                                                               |

Sources: Snowflake documentation on the MFA and password authentication rollout (docs.snowflake.com/en/user-guide/security-mfa-rollout).
