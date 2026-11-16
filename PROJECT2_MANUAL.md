# Project 2: Airflow Medallion Pipeline

This walkthrough runs the insurance claims pipeline with Airflow in Docker, PySpark for the Silver transformation, and Snowflake for the final fact table. It assumes Project 1 is already set up in Snowflake.

## What the pipeline does

Airflow dynamically maps five file-copy tasks into a date-partitioned local raw folder. It validates the landed files, runs PySpark to clean and deduplicate claims into a Silver folder, uploads cleaned claims to a Snowflake stage, reloads `RAW.CLAIMS`, merges into `CORE.FACT_CLAIMS`, then checks that the Silver and fact counts match.

```text
data/*.csv -> Airflow copy + validation -> lake/raw/<run-date>
          -> PySpark cleaning -> lake/silver/<run-date>
          -> Snowflake stage -> RAW.CLAIMS -> MERGE -> CORE.FACT_CLAIMS
          -> reconcile Silver row count against fact row count
```

## 1. Prerequisites

- Complete Project 1 through its initial Snowflake setup, raw load, and model build. Project 2 reuses `INSURANCE_DW.RAW`, `STAGING.CLAIMS_CLEAN`, and the core dimensions and fact table.
- Install and start Docker Desktop. On Windows, Docker Desktop needs WSL2 enabled. Keep Docker Desktop running while using the pipeline.
- Open PowerShell in the project folder. Verify Docker is available:

```powershell
docker --version
docker compose version
```

## 2. Add Snowflake credentials for Docker

Copy the example environment file and edit the copy:

```powershell
Copy-Item .env.example .env
notepad .env
```

Set the account identifier, username, and password without quotes:

```dotenv
SNOWFLAKE_ACCOUNT=orgname-accountname
SNOWFLAKE_USER=YOUR_USER
SNOWFLAKE_PASSWORD=your-password
```

Use the Snowflake account identifier shown in Snowsight account details. `.env` is ignored by Git; keep it private and do not commit it. Project 2's Snowflake connector defaults to role `ACCOUNTADMIN` and warehouse `WH_XS`; the role must have access to the Project 1 database and permission to create a file format and stage in `INSURANCE_DW.RAW`.

## 3. Build and start Airflow

From the project folder, run:

```powershell
docker compose up --build
```

The first build can take several minutes because it installs Java, PySpark, and the Snowflake connector. Leave this terminal running. Open [http://localhost:8080](http://localhost:8080) when the Airflow startup completes.

Airflow standalone creates the `admin` user and a generated password. In a second PowerShell terminal, from the project folder, retrieve it:

```powershell
docker compose exec airflow cat standalone_admin_password.txt
```

Sign in with username `admin` and that password. If the password file is unavailable, inspect the startup logs with `docker compose logs airflow` and look for the generated password.

## 4. Find and trigger the DAG

In Airflow, open the DAG list and find `claims_pipeline`. Enable it with the pause toggle, then click **Trigger DAG** (play icon). Open the DAG's **Graph** view and follow the tasks. The five `copy_to_raw` tasks are dynamically mapped, one for each source file.

The task flow is:

1. `copy_to_raw`: copies `customers.csv`, `policies.csv`, `claims_part1.csv`, `claims_part2.csv`, and `customers_changes.csv` from `data/` into `lake/raw/<run-date>/`.
2. `validate_raw`: stops the run if a file is missing, empty, or has no data rows.
3. `silver_clean`: runs `spark/silver_clean.py` with PySpark. It standardizes types and status values, removes duplicate claim IDs, writes invalid claims to a rejects output, and writes Silver CSVs.
4. `load_snowflake`: stages the Silver claims CSV, copies it into `INSURANCE_DW.RAW.CLAIMS`, and runs `sql/04_merge_fact.sql`.
5. `reconcile`: compares the number of Silver claims with the Snowflake fact-table count and fails if they differ.

Click `silver_clean` and open **Logs**. A successful run should report:

| Check | Expected |
|---|---:|
| Raw claim rows | 3020 |
| Silver valid, deduplicated claims | 2970 |
| Rejected claims | 30 |

All task instances should turn green, including five mapped `copy_to_raw` tasks. The run writes files beneath `lake/raw/<run-date>` and `lake/silver/<run-date>` in the project folder. A Spark `WARN FileStreamSink` message can appear without indicating task failure; use the Airflow task state and final log output to determine success.

## 5. Verify Snowflake and reconciliation

In Snowsight, open a SQL Worksheet and run:

```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT claim_id) AS distinct_claims
FROM insurance_dw.core.fact_claims;

LIST @insurance_dw.raw.stg_silver;
```

Expected: **2970 rows and 2970 distinct claims**. The stage listing should include a file under the `claims/` path. In Airflow, open the successful `reconcile` task and inspect **XCom**; it should report `silver_claims: 2970` and `fact_claims: 2970`.

## 6. Demonstrate safe reruns

Trigger `claims_pipeline` again. After the second successful run, rerun the Snowflake count query. It should remain at 2970. The pipeline uploads the same cleaned claim set, and the `MERGE` updates matching claim IDs instead of inserting duplicates.

## 7. Demonstrate retry and recovery

This optional demonstration temporarily makes one input unavailable so Airflow exercises its retry and failure callback:

1. Stop or pause before the next scheduled run, then temporarily rename `data/policies.csv` to `data/policies_x.csv` in PowerShell:

   ```powershell
   Rename-Item data\policies.csv policies_x.csv
   ```

2. Trigger the DAG. The mapped copy task for `policies.csv` should fail, retry twice, and then fail the run. Open its **Logs** and find the retries and the `ALERT:` callback message.
3. Restore the file:

   ```powershell
   Rename-Item data\policies_x.csv policies.csv
   ```

4. In Airflow, clear the failed task (and any downstream tasks marked upstream-failed, if needed) to rerun the pipeline. Confirm the run succeeds.

## 8. Save evidence and stop Docker

Capture useful screenshots of the successful Graph view, mapped copy tasks, Silver quality report, reconciliation XCom, Snowflake count and stage listing, and the retry/recovery demonstration. Do not include credentials or account secrets in screenshots. Add a short Project 2 section and architecture diagram to the README, then commit the work:

```powershell
git add PROJECT2_MANUAL.md README.md dags pipeline spark sql
git commit -m "Project 2: Airflow medallion pipeline"
git push
```

When finished, stop and remove the running container with:

```powershell
docker compose down
```

Project 2 is complete when the DAG succeeds, the quality log shows 3020 raw / 2970 Silver / 30 rejected claims, Snowflake has 2970 fact rows, a second run leaves the count unchanged, and the retry/recovery path has been demonstrated.

## Troubleshooting

| Problem | What to check |
|---|---|
| Docker command fails or Airflow page does not load | Start Docker Desktop and wait until it is running. Check `docker compose ps` and `docker compose logs airflow`. |
| DAG is missing | Wait for Airflow to scan the mounted `dags/` folder, then inspect `docker compose logs airflow` for import errors. |
| Snowflake authentication fails | Check the values in `.env`, including the account identifier format, and confirm the account permits the configured authentication method. Rebuild/restart after changing the environment: `docker compose down` then `docker compose up --build`. |
| `RAW.CLAIMS` or a core object is missing | Finish the Project 1 setup, raw load, and model build before running this DAG. |
| Stage upload or `COPY INTO` finds no files | Open the `silver_clean` log and inspect `lake/silver/<run-date>/claims` for a CSV. Confirm that `load_snowflake` is using the same run date. |
| Reconciliation count differs | The task is detecting a mismatch. Compare the Silver claim CSV row count with `CORE.FACT_CLAIMS`; inspect the `load_snowflake` log for Snowflake errors and check that Project 1's model joins resolve all claim policies and customers. |
| Container cannot write to `lake/` on Linux | From the project directory, use `chmod -R a+rwX lake`. |

