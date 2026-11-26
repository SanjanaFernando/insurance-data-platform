"""Bronze -> Silver cleaning with PySpark. Runs locally and in a Databricks notebook.
Local:       python spark/silver_clean.py            (reads ./data, writes ./out/silver)
Airflow:     LAKE_RAW and LAKE_SILVER environment variables set the input and output folders
Databricks:  paste into a notebook cell and set BASE to your Volume path, e.g. /Volumes/workspace/default/raw"""
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
BASE = os.getenv("LAKE_RAW") or ("/Volumes/workspace/default/raw" if ON_DATABRICKS else "data")
OUT = os.getenv("LAKE_SILVER") or ("/Volumes/workspace/default/silver" if ON_DATABRICKS else "out/silver")

spark = SparkSession.builder.appName("silver_clean").getOrCreate()
log = logging.getLogger("silver_clean")
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
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
reject_count = rejects.count()
print("claims rejected :", reject_count)
print("status values   :", sorted(r[0] for r in claims_silver.select("status").distinct().collect()))

# Rejected rows are expected data quality events, so report them as errors for
# visibility in Airflow while allowing the pipeline to continue to the reject file.
if reject_count:
    reason_counts = rejects.groupBy("reject_reason").count().collect()
    for row in reason_counts:
        sample_ids = [r[0] for r in (
            rejects.filter(F.col("reject_reason") == row["reject_reason"])
            .select("claim_id").limit(5).collect()
        )]
        log.error(
            "DATA_QUALITY_REJECT reason=%s count=%d sample_claim_ids=%s",
            row["reject_reason"], row["count"], sample_ids,
        )

# ---------- write ----------
def write_csv(df, name):                     # one CSV file per table, easy to upload and COPY INTO
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/{name}")

write_csv(claims_silver, "claims"); write_csv(customers_silver, "customers")
write_csv(policies_silver, "policies"); write_csv(rejects, "claims_rejected")
if ON_DATABRICKS:                            # Delta tables (Databricks only)
    claims_silver.write.format("delta").mode("overwrite").saveAsTable("silver_claims")
