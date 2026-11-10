"""Snowflake steps used by the DAG. Credentials come from environment variables."""
import glob
import os
from pathlib import Path

DB = "insurance_dw"
STAGE = f"{DB}.raw.stg_silver"
FILE_FORMAT = f"{DB}.raw.ff_csv"


def connect():
    import snowflake.connector  # imported lazily so unit tests do not need the package

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse="WH_XS", database=DB.upper(), schema="RAW",
    )


def _run(conn, sql: str):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


def ensure_objects(conn) -> None:
    _run(conn, f"CREATE FILE FORMAT IF NOT EXISTS {FILE_FORMAT} TYPE = CSV SKIP_HEADER = 1 "
               "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' NULL_IF = ('')")
    _run(conn, f"CREATE STAGE IF NOT EXISTS {STAGE} FILE_FORMAT = {FILE_FORMAT}")


def upload_silver(conn, silver_dir: str, table: str = "claims") -> int:
    """Replace the files under @stg_silver/<table>/ with the freshly cleaned CSV part files."""
    files = sorted(glob.glob(str(Path(silver_dir) / table / "part-*.csv")))
    if not files:
        raise FileNotFoundError(f"no silver CSV files found in {silver_dir}/{table}")
    _run(conn, f"REMOVE @{STAGE}/{table}/")
    for f in files:
        _run(conn, f"PUT 'file://{Path(f).as_posix()}' @{STAGE}/{table}/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
    return len(files)


def copy_into_raw(conn, table: str = "claims") -> None:
    """Reload RAW.<table> from the stage. FORCE = TRUE so a re-run reloads the same files."""
    _run(conn, f"TRUNCATE TABLE {DB}.raw.{table}")
    _run(conn, f"COPY INTO {DB}.raw.{table} FROM @{STAGE}/{table}/ "
               f"FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT}) PATTERN = '.*[.]csv' "
               "FORCE = TRUE ON_ERROR = ABORT_STATEMENT")


def merge_fact(conn, sql_path: str) -> None:
    _run(conn, Path(sql_path).read_text())


def fact_count(conn) -> int:
    return int(_run(conn, f"SELECT COUNT(*) FROM {DB}.core.fact_claims")[0][0])


def count_csv_rows(silver_dir: str, table: str = "claims") -> int:
    total = 0
    for f in glob.glob(str(Path(silver_dir) / table / "part-*.csv")):
        with open(f) as handle:
            total += max(sum(1 for _ in handle) - 1, 0)          # minus header
    return total


def reconcile(conn, silver_dir: str) -> dict:
    """The warehouse must contain exactly the clean claims PySpark produced."""
    expected, actual = count_csv_rows(silver_dir), fact_count(conn)
    if expected != actual:
        raise ValueError(f"reconciliation failed: silver has {expected} claims, fact_claims has {actual}")
    return {"silver_claims": expected, "fact_claims": actual}
