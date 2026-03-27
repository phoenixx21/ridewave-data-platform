# Databricks notebook source
# ── Block 7 Cell 0 — Recovery Cell ──────────────────────────────
# Run this FIRST every time you open this notebook
# or after any cluster restart / compute switch
# WHY: Each notebook session is isolated — variables from
#      previous sessions or other notebooks are not available

YOUR_NAME   = "ashreya"    # ← CHANGE TO YOUR FIRST NAME (lowercase)

if YOUR_NAME == "yourname":
    raise Exception("Change YOUR_NAME first!")

CATALOG     = "de_workspace26"
YOUR_DB     = f"{CATALOG}.ridewave_{YOUR_NAME}"
S3_PATH     = f"s3://s3-de-q1-26/DE-Training/Day10/{YOUR_NAME}/"
SCHEMA_LOC  = f"s3://s3-de-q1-26/DE-Training/Day10/{YOUR_NAME}/schema/"
CKPT_BRONZE = f"s3://s3-de-q1-26/DE-Training/Day10/{YOUR_NAME}/checkpoints/bronze/"
CKPT_SILVER = f"s3://s3-de-q1-26/DE-Training/Day10/{YOUR_NAME}/checkpoints/silver/"
CKPT_GOLD   = f"s3://s3-de-q1-26/DE-Training/Day10/{YOUR_NAME}/checkpoints/gold/"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ridewave_{YOUR_NAME}")

print("=" * 50)
print(f"  Student  : {YOUR_NAME}")
print(f"  Database : {YOUR_DB}")
print(f"  S3 Path  : {S3_PATH}")
print("=" * 50)
print("✅ Ready — run cells in order from Cell 1")

# COMMAND ----------

# ── Block 7 Cell 1 — Generate and upload source data ────────────
# WHY: Creates 300 ride records and 100 driver records and
#      writes them as CSV files to your personal S3 folder
#      This simulates RideWave's nightly MySQL export to S3
# RUN ON: Cluster (Serverless may not have S3 write access)

import random
import pandas as pd
from datetime import datetime, timedelta

random.seed(42)

CITIES   = ["Mumbai","Delhi","Bengaluru","Chennai","Pune","Hyderabad"]
STATUSES = ["completed","completed","completed","cancelled","no_show"]
VEHICLES = ["Bike","Auto","Sedan","SUV","Mini"]
base     = datetime(2024, 1, 1)

# Generate 300 ride records
rides_data = []
for i in range(1, 301):
    fare   = round(random.uniform(50, 800), 2)
    status = random.choice(STATUSES)
    if i % 20 == 0: fare   = None  # intentional null — Silver handles
    if i % 25 == 0: status = None  # intentional null — Silver handles
    rides_data.append({
        "ride_id"     : f"RID{i:04d}",
        "driver_id"   : f"DRV{random.randint(1,100):03d}",
        "customer_id" : f"CUST{random.randint(1,200):03d}",
        "vehicle_type": random.choice(VEHICLES),
        "city"        : random.choice(CITIES),
        "fare_amount" : fare,
        "distance_km" : round(random.uniform(1.5, 35.0), 2),
        "ride_status" : status,
        "ride_date"   : (base + timedelta(
                          days=random.randint(0,89))
                        ).strftime("%Y-%m-%d"),
        "pickup_time" : f"{random.randint(6,23):02d}:{random.randint(0,59):02d}"
    })

# Generate 100 driver records
drivers_data = []
for i in range(1, 101):
    drivers_data.append({
        "driver_id"  : f"DRV{i:03d}",
        "driver_name": f"Driver_{i}",
        "city"       : random.choice(CITIES),
        "vehicle_type": random.choice(VEHICLES),
        "rating"     : round(random.uniform(3.5, 5.0), 1),
        "total_rides": random.randint(50, 2000),
        "joined_date": (base - timedelta(
                          days=random.randint(30,730))
                        ).strftime("%Y-%m-%d"),
        "is_active"  : random.choice(["Y","Y","Y","N"])
    })

df_rides   = spark.createDataFrame(pd.DataFrame(rides_data))
df_drivers = spark.createDataFrame(pd.DataFrame(drivers_data))

df_rides.coalesce(1).write.mode("overwrite") \
    .option("header","true").csv(f"{S3_PATH}raw/rides/")
df_drivers.coalesce(1).write.mode("overwrite") \
    .option("header","true").csv(f"{S3_PATH}raw/drivers/")

print(f"✅ Written {len(rides_data)} rides to S3")
print(f"✅ Written {len(drivers_data)} drivers to S3")

# COMMAND ----------

# ── Block 7 Cell 2 — Ingest rides to Bronze Delta table ─────────
# WHY: Bronze stores raw data exactly as received — no cleaning
#      The 4 metadata columns provide the audit trail:
#        _source     → which system sent this data
#        _ingest_ts  → when was it ingested (for SLA tracking)
#        _file_name  → which file did it come from (for debugging)
#        _run_id     → which pipeline run (for reprocessing)
# HOW: spark.read reads CSV, withColumn adds metadata,
#      write.delta appends to partitioned Delta table

from pyspark.sql import functions as F
from datetime import date

RUN_ID = str(date.today())

df_rides_raw = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{S3_PATH}raw/rides/")
)

df_rides_bronze = (df_rides_raw
    .withColumn("_source",     F.lit("s3_raw_rides"))
    .withColumn("_ingest_ts",  F.current_timestamp())
    .withColumn("_file_name",  F.lit(f"{S3_PATH}raw/rides/"))
    .withColumn("_run_id",     F.lit(RUN_ID))
    .withColumn("ingest_date", F.current_date())
)

spark.sql(f"DROP TABLE IF EXISTS {YOUR_DB}.rides_bronze")

(df_rides_bronze.write
    .format("delta")
    .mode("append")                               # Bronze Rule: always append
    .partitionBy("ingest_date")                   # partition for query performance
    .option("delta.autoOptimize.optimizeWrite", "true")
    .saveAsTable(f"{YOUR_DB}.rides_bronze")
)

count = spark.table(f"{YOUR_DB}.rides_bronze").count()
print(f"✅ rides_bronze: {count} rows")
print(f"   Expected   : 300 rows")


# COMMAND ----------

# ── Block 7 Cell 3 — Add constraints to rides_bronze ────────────
# WHY: Constraints are the last line of defence against bad data
#      If a record violates a constraint — it is rejected
#      This is data quality enforcement at the storage layer
#      Not just in application code (which can be bypassed)

spark.sql(f"""
    ALTER TABLE {YOUR_DB}.rides_bronze
    ADD CONSTRAINT ride_id_not_null
    CHECK (ride_id IS NOT NULL)
""")

spark.sql(f"""
    ALTER TABLE {YOUR_DB}.rides_bronze
    ADD CONSTRAINT fare_non_negative
    CHECK (fare_amount >= 0 OR fare_amount IS NULL)
""")

print("✅ Constraints added to rides_bronze")

# COMMAND ----------

# ── Block 7 Cell 4 — Ingest drivers to Bronze Delta table ───────
# WHY: Driver data is separate from ride data but equally important
#      Bronze captures it in the same append-only Delta pattern

df_drivers_raw = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{S3_PATH}raw/drivers/")
)

df_drivers_bronze = (df_drivers_raw
    .withColumn("_source",     F.lit("s3_raw_drivers"))
    .withColumn("_ingest_ts",  F.current_timestamp())
    .withColumn("_file_name",  F.lit(f"{S3_PATH}raw/drivers/"))
    .withColumn("_run_id",     F.lit(RUN_ID))
    .withColumn("ingest_date", F.current_date())
)

spark.sql(f"DROP TABLE IF EXISTS {YOUR_DB}.drivers_bronze")

(df_drivers_bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("ingest_date")
    .option("delta.autoOptimize.optimizeWrite", "true")
    .saveAsTable(f"{YOUR_DB}.drivers_bronze")
)

count = spark.table(f"{YOUR_DB}.drivers_bronze").count()
print(f"✅ drivers_bronze: {count} rows")
print(f"   Expected      : 100 rows")

# COMMAND ----------

# ── Block 7 Cell 6 — Bronze validator function ──────────────────
# WHY: Before Silver runs, we validate Bronze meets quality standards
#      Catches issues early — better than discovering bad data in Gold
#      This function is reusable across all Bronze tables
# HOW: Checks 4 rules: row count, null %, schema, metadata columns

def bronze_validator(df, table_name, key_column, expected_columns):
    """
    Validates Bronze DataFrame against 4 quality rules.
    Returns results dict — assert on results in Workflow for alerting.
    """
    results = {}
    row_count = df.count()

    # Rule 1: Table must have rows
    results["row_count"] = "pass" if row_count > 0 else "fail"
    print(f"  row_count    : {row_count} → {results['row_count']}")

    # Rule 2: Key column null % must be under 5%
    null_count = df.filter(F.col(key_column).isNull()).count()
    null_pct   = (null_count / row_count * 100) if row_count > 0 else 100
    results["null_check"] = {
        "status"    : "pass" if null_pct < 5 else "fail",
        "null_count": null_count,
        "null_pct"  : round(null_pct, 2)
    }
    print(f"  null_check   : {null_count} nulls ({null_pct:.1f}%) → {results['null_check']['status']}")

    # Rule 3: All expected columns must be present
    missing = [c for c in expected_columns if c not in df.columns]
    results["schema_check"] = {
        "status" : "pass" if not missing else "fail",
        "missing": missing
    }
    print(f"  schema_check : missing={missing} → {results['schema_check']['status']}")

    # Rule 4: All 4 Bronze metadata columns must exist
    meta_cols    = ["_source","_ingest_ts","_file_name","_run_id"]
    missing_meta = [c for c in meta_cols if c not in df.columns]
    results["metadata_check"] = "pass" if not missing_meta else "fail"
    print(f"  metadata     : missing={missing_meta} → {results['metadata_check']}")

    overall = "✅ PASS" if all([
        results["row_count"] == "pass",
        results["null_check"]["status"] == "pass",
        results["schema_check"]["status"] == "pass",
        results["metadata_check"] == "pass"
    ]) else "❌ FAIL"

    print(f"\n  [{table_name}] Overall: {overall}")
    return results

# Run validator on both Bronze tables
print("=== rides_bronze validation ===")
df_rides_b = spark.table(f"{YOUR_DB}.rides_bronze")
results_r  = bronze_validator(
    df_rides_b, "rides_bronze", "ride_id",
    ["ride_id","driver_id","city","fare_amount","ride_status","ride_date"]
)
assert results_r["row_count"] == "pass", "rides_bronze row count check failed!"

print("\n=== drivers_bronze validation ===")
df_drivers_b = spark.table(f"{YOUR_DB}.drivers_bronze")
results_d    = bronze_validator(
    df_drivers_b, "drivers_bronze", "driver_id",
    ["driver_id","driver_name","city","vehicle_type","rating"]
)


# COMMAND ----------

# ── Block 7 Cell 7 — Spark execution plan ───────────────────────
# WHY: explain() shows the physical execution plan Spark will use
#      PushedFilters shows Spark is pushing WHERE clause to storage
#      This means Spark reads less data = faster queries
# Answer these in SKILLS_EVIDENCE.md under Q8:
#   a. What does lazy evaluation mean?
#      What triggered the actual computation here?
#   b. Look at physical plan — what does PushedFilters tell you?

df_bronze = spark.read.format("delta").table(f"{YOUR_DB}.rides_bronze")

df_bronze \
    .filter(df_bronze.ride_status == "completed") \
    .select("ride_id","driver_id","fare_amount") \
    .explain(True)

# COMMAND ----------

# ── Block 7 Cell 8 — Final verification ─────────────────────────
# Confirm both Bronze tables exist with correct row counts

spark.sql(f"SHOW TABLES IN {YOUR_DB}").display()

for tbl in ["rides_bronze","drivers_bronze"]:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {YOUR_DB}.{tbl}").first()["n"]
    print(f"  {tbl:<25} : {n} rows")

# COMMAND ----------

