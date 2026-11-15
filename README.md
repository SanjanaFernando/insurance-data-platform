# Insurance Data Platform

Three data-engineering projects built on one synthetic insurance dataset (customers, policies, claims).

| Project | What it shows | Stack |
|---|---|---|
| 1. Claims warehouse | Star schema, SCD Type 2, idempotent MERGE loads, window-function analytics | Snowflake, SQL, Python |
| 2. Airflow medallion pipeline | Dynamic task mapping, validation gates, PySpark silver layer, stage + COPY INTO, MERGE, reconciliation | Apache Airflow (Docker), PySpark, Snowflake |
| 3. API to warehouse with dbt and CI | Retries and chunking, tested transformations, CI | Python, Open-Meteo API, dbt, GitHub Actions |

## Architecture
(add your architecture diagram image here: docs/architecture.png)

## How to run
1. `pip install -r requirements.txt`
2. `python generate_data.py`
3. Set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`; run `sql/01_setup.sql`, then `python load_raw.py`, then `sql/02_model.sql`.
4. Project 2: copy `.env.example` to `.env`, run `docker compose up --build`, open http://localhost:8080 and trigger the `claims_pipeline` DAG.
5. Project 3: `python -m extractor.extract_weather 2024-01-01 2026-10-31 --load`, then `cd dbt_insurance && dbt build --profiles-dir .`

For the full manual walkthrough and expected Project 1 checkpoints, see [PROJECT1_MANUAL.md](PROJECT1_MANUAL.md).

## Design decisions
- Raw layer is all VARCHAR so loads never fail on bad data; cleaning happens in views.
- Fact loads use MERGE on the natural key, so re-running or re-sending files never duplicates rows.
- dim_customer is SCD Type 2 so historical claims stay attached to the address valid at the time.

## Results
(add real numbers: rows loaded, rows rejected, tests passing, screenshots)
