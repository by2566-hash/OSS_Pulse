"""
00_bucket_gharchive_2025.py
----------------------------
One-time script: read by2566's 2025 cleaned GH Archive data and
re-write as a bucketed Hive table (256 buckets by repo_name).

After this runs, Job 04/06/07 can read from the bucketed table,
eliminating the 83 GB shuffle caused by GROUP BY repo_name.

Runtime: ~1-2 hours (one-time cost)
Space:   ~70 GB (copy of by2566's cleaned data, bucketed)

Run:
  spark-submit /tmp/00_bucket_gharchive_2025.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("00_BucketGHArchive2025") \
    .config("spark.sql.warehouse.dir",
            "hdfs:///user/jl17797_nyu_edu/oss_pulse/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BAD_DATES = {"2025-06-09", "2025-08-08", "2025-11-13", "2025-11-30"}

print("[INFO] Reading by2566 2025 cleaned data...")
df = spark.read.parquet("/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/") \
    .filter(~F.col("event_date").cast("string").isin(BAD_DATES))

print("[INFO] Writing bucketed table (256 buckets by repo_name)...")
spark.sql("CREATE DATABASE IF NOT EXISTS oss_pulse")

df.write \
    .mode("overwrite") \
    .bucketBy(256, "repo_name") \
    .sortBy("repo_name") \
    .saveAsTable("oss_pulse.gharchive_2025_bucketed")

count = spark.table("oss_pulse.gharchive_2025_bucketed").count()
print(f"[INFO] Done. Rows: {count:,}")
print("[INFO] Job 04/06/07 can now read via: spark.table('oss_pulse.gharchive_2025_bucketed')")
spark.stop()
