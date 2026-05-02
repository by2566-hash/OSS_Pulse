"""
Job 06: star_growth_hype
-------------------------
Input:  /user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/
        /user/jl17797_nyu_edu/oss_pulse/source/seed_repos.json
Output: /user/jl17797_nyu_edu/oss_pulse/analytics/star_growth_hype

Description:
  Detect "hype" repos: those with sudden star bursts in 2025.
  Metrics:
    - monthly star counts (Jan-Dec 2025)
    - peak_month: month with highest stars
    - peak_stars: stars in peak month
    - peak_ratio: peak_stars / avg_monthly_stars (hype score)
    - is_ai: whether repo is in seed list

  A high peak_ratio (>3x average) indicates hype/viral behavior.

  Depends on: Job 04 (uses top_repos_all as repo filter)

Run:
  spark-submit /tmp/06_star_growth_hype.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

spark = SparkSession.builder.appName("06_StarGrowthHype").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

seed_raw = spark.sparkContext.wholeTextFiles(
    "/user/jl17797_nyu_edu/oss_pulse/source/seed_repos.json"
).collect()
seed_list = [r["repo"].lower() for r in json.loads(seed_raw[0][1])]

# Top 1000 repos as filter
top_repos = spark.read.parquet(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/top_repos_all"
).select("repo_name")

# Monthly star counts for top repos — union 2025 + supplement (2025-12 to 2026-04)
BAD_DATES = {"2025-06-09", "2025-08-08", "2025-11-13", "2025-11-30"}
gh_2025 = spark.read.parquet("/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/") \
  .filter(~F.col("event_date").cast("string").isin(BAD_DATES))
SUPPLEMENT = "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement"
try:
    gh = gh_2025.unionByName(spark.read.parquet(SUPPLEMENT))
    print("INFO: supplement unioned")
except Exception:
    gh = gh_2025
    print("INFO: supplement not found, using 2025 only")

monthly = gh.join(top_repos, on="repo_name") \
  .filter(F.col("event_type") == "WatchEvent") \
  .withColumn("month", F.date_format("event_date", "yyyy-MM")) \
  .groupBy("repo_name", "month") \
  .agg(F.count("*").alias("monthly_stars"))

# Hype metrics per repo
window = F.window if hasattr(F, 'window') else None
from pyspark.sql.window import Window

w = Window.partitionBy("repo_name")

hype = monthly.withColumn("avg_monthly_stars", F.avg("monthly_stars").over(w)) \
  .withColumn("max_monthly_stars", F.max("monthly_stars").over(w)) \
  .withColumn("peak_ratio", F.col("max_monthly_stars") / (F.col("avg_monthly_stars") + 1)) \
  .groupBy("repo_name") \
  .agg(
    F.sum("monthly_stars").alias("total_stars"),
    F.round(F.avg("monthly_stars"), 1).alias("avg_monthly_stars"),
    F.max("monthly_stars").alias("peak_stars"),
    F.first("peak_ratio").alias("peak_ratio"),
    F.count("month").alias("months_active")
  ) \
  .withColumn("is_ai", F.col("repo_name").isin(seed_list)) \
  .orderBy(F.desc("peak_ratio")) \
  .cache()

hype.write.mode("overwrite").parquet(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/star_growth_hype")
hype.coalesce(1).write.mode("overwrite").csv(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/star_growth_hype_csv", header=True)

print(f"Done. Rows: {hype.count()}")
spark.stop()
