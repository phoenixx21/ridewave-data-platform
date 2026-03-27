# Block 4 - Glue ETL Job
# WHY: Convert raw CSV to clean Parquet format
#      Drop nulls, fix data types, add audit timestamp
# HOW: PySpark running on Glue managed infrastructure
#      Source: S3 raw CSV → Target: S3 processed Parquet

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args        = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── CONFIG — CHANGE YOUR_NAME BEFORE SAVING ───────────────────────
YOUR_NAME   = "ashreya"    # ← CHANGE THIS to your first name

SOURCE_PATH = f"s3://s3-de-q1-26/DE-Training/Day10/{YOUR_NAME}/raw/rides/"
TARGET_PATH = f"s3://s3-de-q1-26/DE-Training/Day10/{YOUR_NAME}/processed/rides/"

# ── READ RAW CSV ──────────────────────────────────────────────────
# WHY: Read raw data exactly as it is — no changes yet
df_raw = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(SOURCE_PATH)

print(f"Raw rows: {df_raw.count()}")

# ── APPLY CLEANING RULES ──────────────────────────────────────────
# WHY: Based on data_profiler.py findings:
#      ride_id nulls → drop (cannot have rides without ID)
#      fare_amount → cast to double (analytics needs numbers)
#      ride_date → cast to date (time-based queries need proper dates)
#      ride_status → lowercase (standardise values)
#      ingestion_timestamp → adds audit trail (when was this cleaned)
df_clean = df_raw \
    .filter(F.col("ride_id").isNotNull()) \
    .withColumn("fare_amount",
                F.col("fare_amount").cast("double")) \
    .withColumn("ride_date",
                F.to_date(F.col("ride_date"), "yyyy-MM-dd")) \
    .withColumn("ride_status",
                F.lower(F.col("ride_status"))) \
    .withColumn("ingestion_timestamp",
                F.lit(datetime.utcnow().isoformat()))

print(f"Clean rows : {df_clean.count()}")
print(f"Rows dropped: {df_raw.count() - df_clean.count()}")

# ── WRITE AS PARQUET ──────────────────────────────────────────────
# WHY: Parquet is columnar format — 10x faster for analytics
#      75% smaller than CSV — saves storage cost
df_clean.write.mode("overwrite").parquet(TARGET_PATH)
print(f"Written to: {TARGET_PATH}")

job.commit()