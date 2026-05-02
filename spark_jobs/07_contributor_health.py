"""
Job 07: contributor_health
---------------------------
Input:  /user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/
        /user/jl17797_nyu_edu/oss_pulse/source/seed_repos.json
Output: /user/jl17797_nyu_edu/oss_pulse/analytics/contributor_health

Description:
  Measure contributor diversity and health per repo (top 1000).
  Metrics:
    - total_contributors: distinct actors across all events
    - push_contributors: distinct actors doing pushes (core devs)
    - top1_push_ratio: fraction of pushes by single top contributor (concentration)
    - pr_contributors: distinct actors opening PRs
    - bus_factor_proxy: number of contributors responsible for 80% of pushes
    - is_ai: whether repo is in seed list

  High top1_push_ratio → single maintainer risk.
  Low bus_factor_proxy → fragile project.

  Depends on: Job 04

Run:
  spark-submit /tmp/07_contributor_health.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import json

spark = SparkSession.builder.appName("07_ContributorHealth").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

seed_raw = spark.sparkContext.wholeTextFiles(
    "/user/jl17797_nyu_edu/oss_pulse/source/seed_repos.json"
).collect()
seed_set = set(r["repo"].lower() for r in json.loads(seed_raw[0][1]))
seed_bc = spark.sparkContext.broadcast(seed_set)

@F.udf(BooleanType())
def is_ai(repo_name):
    return repo_name is not None and repo_name.lower() in seed_bc.value

top_repos = spark.read.parquet(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/top_repos_all"
).select("repo_name")

BAD_DATES = {"2025-06-09", "2025-08-08", "2025-11-13", "2025-11-30"}
gh_2025 = spark.read.parquet("/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/") \
  .filter(~F.col("event_date").cast("string").isin(BAD_DATES))
SUPPLEMENT = "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement"
try:
    gh_all = gh_2025.unionByName(spark.read.parquet(SUPPLEMENT))
    print("INFO: supplement unioned")
except Exception:
    gh_all = gh_2025
    print("INFO: supplement not found, using 2025 only")
gh = gh_all.join(top_repos, on="repo_name")

# Total contributors
total_contrib = gh.groupBy("repo_name") \
  .agg(F.countDistinct("actor_login").alias("total_contributors"))

# Push contributors + top1 concentration
push = gh.filter(F.col("event_type") == "PushEvent")
push_per_actor = push.groupBy("repo_name", "actor_login") \
  .agg(F.count("*").alias("push_count"))

from pyspark.sql.window import Window
w = Window.partitionBy("repo_name").orderBy(F.desc("push_count"))

push_ranked = push_per_actor.withColumn("rank", F.rank().over(w))
top1_push = push_ranked.filter(F.col("rank") == 1) \
  .select("repo_name", F.col("push_count").alias("top1_pushes"))

total_pushes = push_per_actor.groupBy("repo_name") \
  .agg(
    F.sum("push_count").alias("total_pushes"),
    F.countDistinct("actor_login").alias("push_contributors")
  )

push_health = total_pushes.join(top1_push, on="repo_name") \
  .withColumn("top1_push_ratio",
    F.round(F.col("top1_pushes") / (F.col("total_pushes") + 1), 3))

# PR contributors
pr_contrib = gh.filter(F.col("event_type") == "PullRequestEvent") \
  .groupBy("repo_name") \
  .agg(F.countDistinct("actor_login").alias("pr_contributors"))

# Combine
result = total_contrib \
  .join(push_health, on="repo_name", how="left") \
  .join(pr_contrib, on="repo_name", how="left") \
  .withColumn("is_ai", is_ai(F.col("repo_name"))) \
  .orderBy(F.desc("total_contributors"))

result.write.mode("overwrite").parquet(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/contributor_health")
result.coalesce(1).write.mode("overwrite").csv(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/contributor_health_csv", header=True)

print(f"Done. Rows: {result.count()}")
spark.stop()
