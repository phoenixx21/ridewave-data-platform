# Databricks notebook source
# ── Block 9 Cell 0 — Recovery Cell ──────────────────────────────
# Run this FIRST every time

YOUR_NAME    = "ashreya"    # ← CHANGE TO YOUR FIRST NAME

if YOUR_NAME == "yourname":
    raise Exception("Change YOUR_NAME first!")

CATALOG      = "de_workspace26"
YOUR_DB      = f"{CATALOG}.ridewave_{YOUR_NAME}"
YOUR_GOLD_DB = f"{CATALOG}.ridewave_gold_{YOUR_NAME}"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS ridewave_gold_{YOUR_NAME}")

print(f"✅ Student    : {YOUR_NAME}")
print(f"   Silver     : {YOUR_DB}")
print(f"   Gold       : {YOUR_GOLD_DB}")

# COMMAND ----------

# ── Block 9 Cell 1 — Verify Silver exists before building Gold ──
# WHY: Gold reads from Silver — if Silver is empty or missing
#      Gold tables will be empty or fail. Better to catch this
#      early with a clear error than let Gold run silently on bad data

rides_count   = spark.table(f"{YOUR_DB}.rides_silver").count()
drivers_count = spark.table(f"{YOUR_DB}.drivers_silver").count()

if rides_count == 0:
    raise Exception("rides_silver is empty! Run Block 8 first.")
if drivers_count == 0:
    raise Exception("drivers_silver is empty! Run Block 8 first.")

print(f"✅ Silver verified: {rides_count} rides, {drivers_count} drivers")
print(f"   Proceeding to build Gold tables...")

# COMMAND ----------

# ── Block 9 Cell 2 — Gold Table 1: Revenue by city ──────────────
# WHY: Operations team needs city-level revenue to allocate
#      resources — more drivers in high-revenue cities
#      Finance team uses this for revenue reporting
# HOW: CTE pattern makes the query readable and auditable
#      OPTIMIZE + ZORDER ensures fast queries on city column

spark.sql(f"""
    WITH cleaned_rides AS (
        SELECT *
        FROM {YOUR_DB}.rides_silver
        WHERE fare_amount IS NOT NULL
          AND ride_status = 'completed'
    ),
    city_summary AS (
        SELECT
            city,
            COUNT(*)                       AS total_rides,
            ROUND(SUM(fare_amount), 2)     AS total_revenue,
            ROUND(AVG(fare_amount), 2)     AS avg_fare,
            ROUND(AVG(distance_km), 2)     AS avg_distance_km
        FROM cleaned_rides
        GROUP BY city
    )
    SELECT * FROM city_summary
    ORDER BY total_revenue DESC
""").write.format("delta").mode("overwrite") \
    .saveAsTable(f"{YOUR_GOLD_DB}.gold_city_revenue")

spark.sql(f"OPTIMIZE {YOUR_GOLD_DB}.gold_city_revenue ZORDER BY (city)")

count = spark.table(f"{YOUR_GOLD_DB}.gold_city_revenue").count()
print(f"✅ gold_city_revenue: {count} rows (expected: 6 cities)")
spark.table(f"{YOUR_GOLD_DB}.gold_city_revenue").display()

# COMMAND ----------

# ── Block 9 Cell 3 — Gold Table 2: Driver performance ───────────
# WHY: HR and operations need to know which vehicle type
#      generates most revenue per city — informs driver recruitment
#      and incentive structure decisions
# HOW: Stream-to-static join pattern — rides (main) joined with
#      drivers (lookup) to enrich rides with vehicle type info

spark.sql(f"""
    WITH ride_data AS (
        SELECT
            r.driver_id,
            r.city,
            r.fare_amount,
            r.distance_km,
            d.vehicle_type,
            d.rating AS driver_rating
        FROM {YOUR_DB}.rides_silver r
        LEFT JOIN {YOUR_DB}.drivers_silver d
            ON r.driver_id = d.driver_id
        WHERE r.fare_amount IS NOT NULL
    ),
    driver_summary AS (
        SELECT
            city,
            vehicle_type,
            COUNT(*)                       AS total_rides,
            ROUND(SUM(fare_amount), 2)     AS total_revenue,
            ROUND(AVG(fare_amount), 2)     AS avg_fare_per_ride,
            ROUND(AVG(driver_rating), 2)   AS avg_driver_rating
        FROM ride_data
        GROUP BY city, vehicle_type
    )
    SELECT * FROM driver_summary
    ORDER BY city, total_revenue DESC
""").write.format("delta").mode("overwrite") \
    .saveAsTable(f"{YOUR_GOLD_DB}.gold_driver_performance")

spark.sql(f"OPTIMIZE {YOUR_GOLD_DB}.gold_driver_performance ZORDER BY (city)")

count = spark.table(f"{YOUR_GOLD_DB}.gold_driver_performance").count()
print(f"✅ gold_driver_performance: {count} rows")
print(f"   Expected: ~30 rows (6 cities x 5 vehicle types)")
spark.table(f"{YOUR_GOLD_DB}.gold_driver_performance").display()

# COMMAND ----------

# ── Block 9 Cell 4 — Gold Table 3: Peak hour analysis ───────────
# WHY: Operations team needs peak hours to ensure driver availability
#      More drivers needed during Morning Rush and Evening Rush
#      Dynamic pricing is also based on peak hour data
# HOW: TRY_CAST used instead of CAST for pickup_time parsing
#      (TRY_CAST returns NULL on bad values — pipeline keeps running)
#      RANK() identifies top hours per city

spark.sql(f"""
    WITH hourly_data AS (
        SELECT
            city,
            TRY_CAST(SPLIT(pickup_time, ':')[0] AS INT) AS hour_of_day,
            COUNT(*)                                      AS ride_count,
            ROUND(SUM(fare_amount), 2)                    AS hour_revenue
        FROM {YOUR_DB}.rides_silver
        WHERE fare_amount IS NOT NULL
          AND pickup_time IS NOT NULL
        GROUP BY city,
                 TRY_CAST(SPLIT(pickup_time, ':')[0] AS INT)
    ),
    with_rank AS (
        SELECT *,
            RANK() OVER (
                PARTITION BY city
                ORDER BY ride_count DESC
            ) AS peak_rank
        FROM hourly_data
    )
    SELECT
        city,
        hour_of_day,
        ride_count,
        hour_revenue,
        peak_rank,
        CASE
            WHEN hour_of_day BETWEEN 7  AND 10 THEN 'Morning Rush'
            WHEN hour_of_day BETWEEN 12 AND 14 THEN 'Lunch Peak'
            WHEN hour_of_day BETWEEN 17 AND 21 THEN 'Evening Rush'
            ELSE 'Off Peak'
        END AS time_slot
    FROM with_rank
    ORDER BY city, peak_rank
""").write.format("delta").mode("overwrite") \
    .saveAsTable(f"{YOUR_GOLD_DB}.gold_peak_hours")

spark.sql(f"OPTIMIZE {YOUR_GOLD_DB}.gold_peak_hours ZORDER BY (city)")

count = spark.table(f"{YOUR_GOLD_DB}.gold_peak_hours").count()
print(f"✅ gold_peak_hours: {count} rows")
spark.table(f"{YOUR_GOLD_DB}.gold_peak_hours").display()

# COMMAND ----------

# ── Block 9 Cell 5 — OPTIMIZE impact measurement ────────────────
# WHY: OPTIMIZE compacts many small files into fewer large files
#      More files = more open() operations = slower queries
#      OPTIMIZE is critical after many small streaming writes
# Answer in SKILLS_EVIDENCE.md Q10:
#   a. Files before vs after OPTIMIZE?
#   b. Why fewer files = faster queries?
#   c. What does ZORDER BY (city) do differently?

before = spark.sql(
    f"DESCRIBE DETAIL {YOUR_GOLD_DB}.gold_city_revenue"
).select("numFiles","sizeInBytes").first()
print(f"Before OPTIMIZE: {before['numFiles']} files, "
      f"{before['sizeInBytes']} bytes")

spark.sql(f"OPTIMIZE {YOUR_GOLD_DB}.gold_city_revenue")

after = spark.sql(
    f"DESCRIBE DETAIL {YOUR_GOLD_DB}.gold_city_revenue"
).select("numFiles","sizeInBytes").first()
print(f"After OPTIMIZE : {after['numFiles']} files, "
      f"{after['sizeInBytes']} bytes")

# NOTE: If files and bytes are same — table was already optimal
# (written in one overwrite operation = already 1 file)
# OPTIMIZE shows real benefit on streaming tables
# with hundreds of micro-batch files

# COMMAND ----------

# ── Block 9 Cell 6 — Delta Time Travel ──────────────────────────
# WHY: Every write to a Delta table creates a new version
#      Time travel lets you:
#        Debug: "what did the Gold table look like yesterday?"
#        Audit: "show me the data before that bad pipeline run"
#        Recovery: restore to a previous version if needed
# Answer in SKILLS_EVIDENCE.md:
#   When would a business use Time Travel?
#   What does version 0 show vs current version?

# See all versions of the Gold table
spark.sql(
    f"DESCRIBE HISTORY {YOUR_GOLD_DB}.gold_city_revenue"
).display()

# Query at version 0 (first write)
spark.sql(f"""
    SELECT * FROM {YOUR_GOLD_DB}.gold_city_revenue
    VERSION AS OF 0
""").display()

# COMMAND ----------

# ── Block 9 Cell 7 — Driver cohort analysis ─────────────────────
# WHY: Cohort analysis answers: "Of all drivers who took
#      their first ride in January, how many are still active
#      in March?" This drives retention strategy decisions

spark.sql(f"""
    WITH first_rides AS (
        SELECT
            driver_id,
            DATE_TRUNC('month', MIN(ride_date)) AS first_ride_month
        FROM {YOUR_DB}.rides_silver
        GROUP BY driver_id
    ),
    latest_month AS (
        SELECT MAX(DATE_TRUNC('month', ride_date)) AS max_month
        FROM {YOUR_DB}.rides_silver
    ),
    still_active AS (
        SELECT DISTINCT driver_id
        FROM {YOUR_DB}.rides_silver
        WHERE DATE_TRUNC('month', ride_date) =
              (SELECT max_month FROM latest_month)
    )
    SELECT
        fr.first_ride_month       AS acquisition_month,
        COUNT(fr.driver_id)       AS drivers_acquired,
        COUNT(sa.driver_id)       AS still_active
    FROM first_rides fr
    LEFT JOIN still_active sa ON fr.driver_id = sa.driver_id
    GROUP BY fr.first_ride_month
    ORDER BY fr.first_ride_month
""").display()

# COMMAND ----------

# ── Block 9 Cell 8 — Final Gold verification ────────────────────

print("=" * 55)
print("  RideWave Gold Layer Complete!")
print("=" * 55)

gold_tables = [
    (f"{YOUR_GOLD_DB}.gold_city_revenue",
     "Which city earns most?"),
    (f"{YOUR_GOLD_DB}.gold_driver_performance",
     "Which vehicle type performs best?"),
    (f"{YOUR_GOLD_DB}.gold_peak_hours",
     "When are peak ride hours?"),
]

for tbl, question in gold_tables:
    n = spark.table(tbl).count()
    print(f"  ✅ {tbl.split('.')[-1]:<30} : {n} rows")
    print(f"     Answers: {question}")

print("=" * 55)