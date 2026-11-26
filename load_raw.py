"""Load CSV files into Snowflake RAW tables (all columns VARCHAR = true raw layer).
Setup:  pip install "snowflake-connector-python[pandas]"
Env:    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD  (never hard-code credentials)
Usage:  python load_raw.py            # day 1: customers, policies, claims_part1, customers_changes
        python load_raw.py --part2    # later: append claims_part2 (re-sent rows are removed by MERGE)"""
import os
import sys
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().with_name(".env"), override=False)


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse="WH_XS", database="INSURANCE_DW", schema="RAW",
    )


def load_csv(conn, path, table, append=False):
    df = pd.read_csv(path, dtype=str)            # keep everything as text in raw
    df.columns = [c.upper() for c in df.columns]  # unquoted SQL later resolves to upper case
    ok, _, nrows, _ = write_pandas(
        conn, df, table.upper(), auto_create_table=not append, overwrite=not append
    )
    print(f"{table}: loaded={ok} rows={nrows} mode={'append' if append else 'overwrite'}")


if __name__ == "__main__":
    conn = connect()
    if "--part2" in sys.argv:
        load_csv(conn, "data/claims_part2.csv", "claims", append=True)
    else:
        load_csv(conn, "data/customers.csv", "customers")
        load_csv(conn, "data/policies.csv", "policies")
        load_csv(conn, "data/claims_part1.csv", "claims")
        load_csv(conn, "data/customers_changes.csv", "customers_changes")
    conn.close()
