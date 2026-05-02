"""
Job 01: repo_daily_metrics
--------------------------
Input:  /user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/  (67.5 GB, 334 days)
        /user/jl17797_nyu_edu/oss_pulse/source/seed_repos.json
Output: /user/jl17797_nyu_edu/oss_pulse/analytics/repo_daily_metrics

Description:
  Filter GH Archive cleaned data to AI seed repos only.
  Aggregate daily: stars, forks, pushes, PRs, issues, distinct_actors.

Run:
  spark-submit /tmp/01_repo_daily_metrics.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import json

spark = SparkSession.builder.appName("01_RepoDailyMetrics").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load seed repos
seed_raw = spark.sparkContext.wholeTextFiles(
    "/user/jl17797_nyu_edu/oss_pulse/source/seed_repos.json"
).collect()
seed_set = set(r["repo"].lower() for r in json.loads(seed_raw[0][1]))
seed_bc = spark.sparkContext.broadcast(seed_set)

@F.udf(BooleanType())
def is_seed(repo_name):
    return repo_name is not None and repo_name.lower() in seed_bc.value

# Load and filter
gh = spark.read.parquet("/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/")
gh_seed = gh.filter(is_seed(F.col("repo_name")))

# Aggregate
metrics = gh_seed.groupBy("repo_name", "event_date").agg(
    F.count(F.when(F.col("event_type") == "WatchEvent", 1)).alias("stars"),
    F.count(F.when(F.col("event_type") == "ForkEvent", 1)).alias("forks"),
    F.count(F.when(F.col("event_type") == "PushEvent", 1)).alias("pushes"),
    F.count(F.when(F.col("event_type") == "PullRequestEvent", 1)).alias("prs"),
    F.count(F.when(F.col("event_type") == "IssuesEvent", 1)).alias("issues"),
    F.countDistinct("actor_login").alias("distinct_actors"),
    F.count("event_id").alias("total_events")
).orderBy("repo_name", "event_date")

metrics.write.mode("overwrite").parquet(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/repo_daily_metrics")

print(f"Done. Rows: {metrics.count()}")
spark.stop()
