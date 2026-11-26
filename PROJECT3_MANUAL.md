# Project 3: Weather API, dbt, and CI

This guide runs the weather extraction and dbt analytics pipeline in this repository. It downloads daily Colombo weather from the Open-Meteo archive, loads it into Snowflake, builds tested dbt models that combine weather and insurance claims, and checks the project in GitHub Actions.

The claims and policy data are synthetic. The weather analysis is a pipeline demonstration, not evidence that weather affects claims.

## What you will build

```text
Open-Meteo archive
       |
       | monthly requests with retries
       v
data/weather_daily.csv -> INSURANCE_DW.RAW.WEATHER_DAILY
                                  |
          RAW.CLAIMS + RAW.POLICIES + RAW.WEATHER_DAILY
                                  |
                    dbt staging views (3)
                                  |
                    dbt marts (2) + 12 tests
                                  |
                    GitHub Actions quality checks
```

The staging models clean and type the raw claims, policies, and weather tables. `mart_claims_daily` has one row per weather date and fills days without claims with zero. `mart_rain_impact` groups those days by whether rainfall was at least 10 mm.

## 1. Prerequisites and local setup

- Python 3.11 or later and Git.
- A Snowflake account with the `INSURANCE_DW` database, `WH_XS` warehouse, and `RAW.CLAIMS` and `RAW.POLICIES` tables. Complete Project 1's setup and load both claims files first. Project 3 reads these raw tables; it does not need the Project 1 core models.
- A GitHub repository containing this project if you want to run the Actions workflow.

In PowerShell, open the project folder and prepare the environment if you have not already done so:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If PowerShell blocks activation, run `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` in that terminal and activate again.

Store the Snowflake credentials in the ignored `.env` file at the project root. If it does not exist yet, copy the example and edit the copy:

```powershell
Copy-Item .env.example .env
notepad .env
```

Set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and `SNOWFLAKE_PASSWORD` in `.env`. Use the account identifier shown in Snowsight account details. The Python loader reads this file, and dbt discovers it when run from `dbt_insurance`; explicitly set PowerShell environment variables take precedence. Do not commit credentials or add them to screenshots.

## 2. Extract and load weather

From the repository root, run:

```powershell
python -m extractor.extract_weather 2024-01-01 2026-10-31 --load
```

The extractor requests the date range one calendar month at a time, retries HTTP 429 and 5xx responses with backoff, combines the responses, and removes duplicate dates. It writes `data/weather_daily.csv`, then replaces `INSURANCE_DW.RAW.WEATHER_DAILY` using the Snowflake credentials from the environment.

For the specified date range, the expected result is 1,035 daily rows. The log should end with a `wrote 1035 rows` line and `weather_daily: loaded=True rows=1035`. The Open-Meteo archive may not have the newest dates immediately; if a request fails near the end of a range, choose an earlier end date and use that same range when checking the row count.

Confirm the loaded table in Snowsight:

```sql
SELECT COUNT(*) AS row_count,
       MIN(TRY_TO_DATE(weather_date)) AS first_date,
       MAX(TRY_TO_DATE(weather_date)) AS last_date
FROM insurance_dw.raw.weather_daily;
```

The row count should be 1,035 for the date range above. If you used an earlier end date, expect a smaller count.

To create only the local CSV without connecting to Snowflake, omit `--load`. dbt still needs the weather table in Snowflake before you build the models.

## 3. Run the Python tests

From the repository root, run:

```powershell
pytest -q
```

All 10 tests in this checkout should pass. The three extractor tests cover month boundaries, output columns, and duplicate dates using a mocked API response; they do not make network requests. The other tests cover the Project 2 pipeline helpers.

## 4. Check the dbt connection and build the models

In PowerShell, change to the dbt project directory:

```powershell
Set-Location dbt_insurance
dbt debug --profiles-dir .
dbt build --profiles-dir .
```

The profile in `profiles.yml` reads `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and `SNOWFLAKE_PASSWORD` from the environment. It uses role `ACCOUNTADMIN`, warehouse `WH_XS`, database `INSURANCE_DW`, and schema `ANALYTICS`. The role needs access to the raw source tables and permission to create the `ANALYTICS` schema and its views and tables.

Expected dbt results:

| Models | Materialization | Purpose |
|---|---|---|
| `stg_claims` | View | Casts and filters raw claims, normalizes status, and keeps the latest row for each claim ID. |
| `stg_policies` | View | Types policy and customer identifiers and annual premium. |
| `stg_weather` | View | Types weather dates and measures and deduplicates dates. |
| `mart_claims_daily` | Table | Joins daily weather to claim counts and amounts, retaining weather days with no claims. |
| `mart_rain_impact` | Table | Compares daily claim measures for rainy and dry days. |

`dbt build` should complete five models and 12 data tests (17 successful model/test steps). Tests check uniqueness, non-null values, policy relationships, and accepted claim statuses. The models are created in `INSURANCE_DW.ANALYTICS`.

Return to the repository root when finished:

```powershell
Set-Location ..
```

## 5. Review the mart results

In Snowsight, run:

```sql
SELECT *
FROM insurance_dw.analytics.mart_claims_daily
ORDER BY weather_date;

SELECT *
FROM insurance_dw.analytics.mart_rain_impact
ORDER BY is_rainy_day;
```

The second query returns daily averages and counts split by the 10 mm rainfall threshold. Since the insurance claims are synthetic and randomly generated, treat any difference as noise rather than a real weather effect.

Optional: generate and serve dbt documentation from the `dbt_insurance` directory:

```powershell
Set-Location dbt_insurance
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

Open the local URL printed by dbt and use the lineage graph button to inspect the source-to-mart model flow. Stop the local server with `Ctrl+C`, then return to the repository root with `Set-Location ..`.

## 6. Run GitHub Actions

Before the first push to `main`, add these repository secrets in **Settings > Secrets and variables > Actions > New repository secret**:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`

The `.github/workflows/ci.yml` workflow runs a `quality` job on every push and pull request. It runs Ruff, pytest, and `dbt parse`; parsing uses dummy values and does not connect to Snowflake. A second `dbt-build` job runs after quality succeeds on pushes to `main` and uses the repository secrets to run `dbt build` against Snowflake.

Push your branch and open the repository's **Actions** tab. A push to `main` with valid secrets should show both jobs green. A push to another branch or a pull request runs the quality job only.

## 7. Save evidence and commit

Useful evidence includes the extractor row-count log, the dbt build results, one mart query, and the GitHub Actions run. Hide credentials and account details in screenshots. Add results and screenshots to the README or a `docs/` folder if you want to present the project.

```powershell
git add PROJECT3_MANUAL.md README.md extractor dbt_insurance .github
git commit -m "Project 3: weather API, dbt models and CI"
git push
```

Project 3 is complete when the extractor loads the weather data, `dbt build` passes its models and tests, and the GitHub Actions quality job is green. The Snowflake build job is also expected to be green for a push to `main` when its repository secrets and account permissions are configured.

## Troubleshooting

If extraction wrote `data/weather_daily.csv` but the Snowflake load failed, load the existing CSV again from the repository root without repeating the API requests:

```powershell
python -c "from load_raw import connect, load_csv; conn = connect(); load_csv(conn, 'data/weather_daily.csv', 'weather_daily'); conn.close()"
```

| Problem | What to check |
|---|---|
| API request fails or returns fewer rows | Retry later or choose an earlier end date if the archive has not published the final days. The extractor retries 429 and 5xx responses, but the date range may still be unavailable. |
| `--load` reports missing environment variables or authentication fails | Set all three `SNOWFLAKE_*` variables in the same PowerShell session. Confirm the account identifier and authentication method for your Snowflake account. |
| `RAW.WEATHER_DAILY` does not exist | Run the extractor with `--load`; omitting the option writes only the CSV. |
| dbt reports that a source table is missing | Confirm `INSURANCE_DW.RAW.CLAIMS`, `POLICIES`, and `WEATHER_DAILY` exist and that the configured role can read them. Complete the Project 1 raw load for claims and policies. |
| dbt cannot create the schema or models | Grant the configured role access to `INSURANCE_DW`, `WH_XS`, and permission to create objects in `ANALYTICS`, or update the role in `dbt_insurance/profiles.yml` to one with those permissions. |
| The GitHub `dbt-build` job fails to log in | Check the three Actions secrets and ensure the Snowflake user has the required database, warehouse, and schema permissions. The `quality` job can pass without valid Snowflake credentials because `dbt parse` does not connect. |
