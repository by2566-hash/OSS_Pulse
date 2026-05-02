"""
Job 03: health_score (三方合併)
--------------------------------
Input:  /user/jl17797_nyu_edu/oss_pulse/analytics/hf_gh_join
        /user/jl17797_nyu_edu/oss_pulse/source/pypi/pypi_monthly_downloads.jsonl
Output: /user/jl17797_nyu_edu/oss_pulse/analytics/health_score

Description:
  Join HF+GH with PyPI monthly downloads.
  Compute health_score using log-normalized weighted sum:
    HF downloads 30% + PyPI downloads 20% + GH stars 15% +
    GH pushes 15% + GH PRs 10% + active_days 10%

Run:
  spark-submit /tmp/03_health_score.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("03_HealthScore").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

hf_gh = spark.read.parquet("/user/jl17797_nyu_edu/oss_pulse/analytics/hf_gh_join")

pypi = spark.read.json(
    "/user/jl17797_nyu_edu/oss_pulse/source/pypi/pypi_monthly_downloads.jsonl"
).groupBy("project").agg(
    F.sum("downloads").alias("pypi_downloads_2025"),
    F.countDistinct("month").alias("pypi_months_tracked")
)

def safe_log(col_name):
    return F.log1p(F.coalesce(F.col(col_name), F.lit(0)))

result = hf_gh.join(pypi, hf_gh["library_name"] == pypi["project"], how="left") \
  .drop("project") \
  .withColumn("health_score",
    safe_log("hf_total_downloads") * 0.30 +
    safe_log("pypi_downloads_2025") * 0.20 +
    safe_log("gh_stars_2025")       * 0.15 +
    safe_log("gh_pushes_2025")      * 0.15 +
    safe_log("gh_prs_2025")         * 0.10 +
    safe_log("gh_active_days")      * 0.10
  ).orderBy(F.desc("health_score"))

result.write.mode("overwrite").parquet(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/health_score")
result.coalesce(1).write.mode("overwrite").csv(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/health_score_csv", header=True)

print(f"Done. Rows: {result.count()}")
spark.stop()
