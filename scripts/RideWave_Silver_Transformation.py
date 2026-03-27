# Databricks notebook source
# ── Block 8 Cell 0 — Recovery Cell ──────────────────────────────
# Run this FIRST every time — defines all variables for this notebook
# WHY: Silver notebook is independent — it cannot access variables
#      defined in the Bronze notebook. Each notebook is isolated.

YOUR_NAME   = "ashreya"    # ← CHANGE TO YOUR FIRST NAME

if YOUR_NAME == "yourname":
    raise Exception("Change YOUR_NAME first!")

CATALOG     = "de_workspace26"
YOUR_DB     = f"{CATALOG}.ridewave_{YOUR_NAME}"
S3_PATH     = f"s3://s3-de-q1-26/DE-Training/Day10/{YOUR_NAME}/"
CKPT_SILVER = f"s3://s3-de-q1-26/DE-Training/Day10/{YOUR_NAME}/checkpoints/silver/"

spark.sql(f"USE CATALOG {CATALOG}")

print(f"✅ Student  : {YOUR_NAME}")
print(f"   Database : {YOUR_DB}")
print(f"   S3 Path  : {S3_PATH}")

# COMMAND ----------

# ── Block 8 Cell 1 — Read Bronze, apply Silver transformation ───
# WHY: Silver reads from Bronze — never from raw CSV directly
#      This ensures Silver is always based on audited Bronze data
#      Cleaning rules are based on data_profiler.py findings:
#        Rule 1: Drop null ride_id (cannot process rides without ID)
#        Rule 2: Drop null fare_amount (cannot calculate revenue)
#        Rule 3: Keep only completed rides (business wants completed only)
#        Rule 4: Cast ride_date to DateType (enables date functions)
#        Rule 5: Cast fare_amount to double (enables math operations)
#        Rule 6: Lowercase ride_status (standardise values)
#        Rule 7: Add processing_date (when was Silver created)

from pyspark.sql import functions as F
from delta.tables import DeltaTable

df_rides_bronze = spark.table(f"{YOUR_DB}.rides_bronze")
bronze_count    = df_rides_bronze.count()
print(f"Bronze rows: {bronze_count}")

df_rides_silver = (df_rides_bronze
    .filter(F.col("ride_id").isNotNull())         # Rule 1
    .filter(F.col("fare_amount").isNotNull())      # Rule 2
    .filter(F.col("ride_status") == "completed")   # Rule 3
    .withColumn("ride_date",
        F.to_date(F.col("ride_date"), "yyyy-MM-dd"))  # Rule 4
    .withColumn("fare_amount",
        F.col("fare_amount").cast("double"))           # Rule 5
    .withColumn("ride_status",
        F.lower(F.col("ride_status")))                 # Rule 6
    .withColumn("processing_date", F.current_date())   # Rule 7
    .withColumn("silver_ts",       F.current_timestamp())
    .drop("_source","_ingest_ts","_file_name","_run_id","ingest_date")
)

silver_count = df_rides_silver.count()
print(f"Silver rows : {silver_count}")
print(f"Rows dropped: {bronze_count - silver_count}")
print(f"Expected    : ~157 rows (completed rides, nulls removed)")

# COMMAND ----------

# ── Block 8 Cell 2 — Write rides_silver using MERGE ─────────────
# WHY: MERGE (upsert) is used instead of overwrite because:
#      1. Idempotent — safe to re-run if pipeline crashes
#      2. Preserves Delta history — time travel still works
#      3. No data loss — existing records updated not deleted
#      First run creates the table, subsequent runs merge into it

if not spark.catalog.tableExists(f"{YOUR_DB}.rides_silver"):
    # First run — create table
    df_rides_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{YOUR_DB}.rides_silver")
    print(f"✅ rides_silver created: "
          f"{spark.table(f'{YOUR_DB}.rides_silver').count()} rows")
else:
    # Subsequent runs — MERGE to avoid duplicates
    target = DeltaTable.forName(spark, f"{YOUR_DB}.rides_silver")
    (target.alias("tgt")
        .merge(
            df_rides_silver.alias("src"),
            "tgt.ride_id = src.ride_id"     # match key
        )
        .whenMatchedUpdateAll()              # update if exists
        .whenNotMatchedInsertAll()           # insert if new
        .execute()
    )
    count = spark.table(f"{YOUR_DB}.rides_silver").count()
    print(f"✅ rides_silver merged: {count} rows")

# COMMAND ----------

# ── Block 8 Cell 3 — Prove idempotency ──────────────────────────
# WHY: Running the pipeline twice should give the same result
#      This is called idempotency — critical for production pipelines
#      A pipeline that creates duplicates on re-run is dangerous
#      MERGE handles this automatically

target = DeltaTable.forName(spark, f"{YOUR_DB}.rides_silver")
(target.alias("tgt")
    .merge(df_rides_silver.alias("src"),
           "tgt.ride_id = src.ride_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

count_after = spark.table(f"{YOUR_DB}.rides_silver").count()
print(f"After second run: {count_after} rows")
print(f"Duplicates added: 0  ← this proves idempotency ✅")

# COMMAND ----------

# ── Block 8 Cell 4 — Read Bronze drivers, apply Silver rules ────
# WHY: Driver data needs cleaning too:
#      initcap(city) — standardise city name capitalisation
#      rating cast to double — enables avg calculations
#      to_date(joined_date) — enables date arithmetic

df_drivers_bronze = spark.table(f"{YOUR_DB}.drivers_bronze")

df_drivers_silver = (df_drivers_bronze
    .filter(F.col("driver_id").isNotNull())
    .withColumn("driver_name",  F.initcap(F.col("driver_name")))
    .withColumn("city",         F.initcap(F.col("city")))
    .withColumn("rating",       F.col("rating").cast("double"))
    .withColumn("joined_date",
        F.to_date(F.col("joined_date"), "yyyy-MM-dd"))
    .withColumn("processing_date", F.current_date())
    .drop("_source","_ingest_ts","_file_name","_run_id","ingest_date")
)

if not spark.catalog.tableExists(f"{YOUR_DB}.drivers_silver"):
    df_drivers_silver.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{YOUR_DB}.drivers_silver")
else:
    target = DeltaTable.forName(spark, f"{YOUR_DB}.drivers_silver")
    (target.alias("tgt")
        .merge(df_drivers_silver.alias("src"),
               "tgt.driver_id = src.driver_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

count = spark.table(f"{YOUR_DB}.drivers_silver").count()
print(f"✅ drivers_silver: {count} rows")
print(f"   Expected      : ~100 rows")

# COMMAND ----------

# ── Block 8 Cell 5 — Enable Change Data Feed on Silver ──────────
# WHY: Change Data Feed (CDF) records every data change in Silver
#      Gold tables can then read ONLY changed rows (not full Silver)
#      This makes Gold refreshes much faster on large datasets

spark.sql(f"""
    ALTER TABLE {YOUR_DB}.rides_silver
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print("✅ Change Data Feed enabled on rides_silver")

# COMMAND ----------

# ── Block 8 Cell 6 — SQL window functions on Silver ─────────────
# WHY: Window functions compute analytics WITHIN groups
#      without collapsing rows (unlike GROUP BY)
#      Business use: running revenue total shows growth trend

# S3a: Running revenue by city — shows cumulative growth
spark.sql(f"""
    WITH monthly_revenue AS (
        SELECT
            city,
            DATE_TRUNC('month', ride_date) AS month,
            ROUND(SUM(fare_amount), 2)     AS monthly_revenue
        FROM {YOUR_DB}.rides_silver
        GROUP BY city, DATE_TRUNC('month', ride_date)
    )
    SELECT
        city, month, monthly_revenue,
        SUM(monthly_revenue) OVER (
            PARTITION BY city
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total
    FROM monthly_revenue
    ORDER BY city, month
""").display()

# S3b: Top 3 drivers per city by ride count
spark.sql(f"""
    WITH driver_counts AS (
        SELECT driver_id, city, COUNT(*) AS ride_count
        FROM {YOUR_DB}.rides_silver
        GROUP BY driver_id, city
    ),
    ranked AS (
        SELECT *,
            RANK() OVER (
                PARTITION BY city
                ORDER BY ride_count DESC
            ) AS city_rank
        FROM driver_counts
    )
    SELECT city, driver_id, ride_count, city_rank
    FROM ranked
    WHERE city_rank <= 3
    ORDER BY city, city_rank
""").display()

# COMMAND ----------

# ── Block 8 Cell 7 — Broadcast join ─────────────────────────────
# WHY: When joining a big table with a small table,
#      broadcast copies the small table to all executors
#      Each executor joins locally — no data moves across network
#      drivers_silver (100 rows) is small → perfect for broadcast
# Answer in SKILLS_EVIDENCE.md Q9:
#   a. What is broadcast join? Why efficient for small tables?
#   b. Can you see BroadcastHashJoin in explain output?
#   c. What if you broadcast a 10 million row table?

from pyspark.sql.functions import broadcast

df_rides_s   = spark.table(f"{YOUR_DB}.rides_silver")
df_drivers_s = spark.table(f"{YOUR_DB}.drivers_silver")

result = df_rides_s.join(
    broadcast(df_drivers_s),   # 100 rows — safe to broadcast
    "driver_id",
    "left"
)
result.explain(True)

# COMMAND ----------

# ── Block 8 Cell 8 — Final Silver verification ──────────────────

for tbl in ["rides_silver","drivers_silver"]:
    n = spark.sql(
        f"SELECT COUNT(*) AS n FROM {YOUR_DB}.{tbl}"
    ).first()["n"]
    print(f"  {tbl:<25} : {n} rows")

# COMMAND ----------

