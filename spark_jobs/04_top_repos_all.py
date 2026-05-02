"""
Job 04: top_repos_all
----------------------
Input:  /user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/
Output: /user/jl17797_nyu_edu/oss_pulse/analytics/top_repos_all

Description:
  No filtering — aggregate ALL repos from GH Archive 2025.
  Keep top 1000 by total stars.
  Used as baseline to compare AI repos vs general ecosystem.

Run:
  spark-submit /tmp/04_top_repos_all.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("04_TopReposAll").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 跳過 HDFS DataNode 損毀的 partition（BlockMissingException on 192.168.1.17）
BAD_DATES = {"2025-06-09", "2025-08-08", "2025-11-13", "2025-11-30"}

gh_2025 = spark.read.parquet("/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/") \
  .filter(~F.col("event_date").cast("string").isin(BAD_DATES))

# Supplement: 2025-12-01 to 2026-04-30 (Job 09 output)
SUPPLEMENT = "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement"
try:
    gh_supp = spark.read.parquet(SUPPLEMENT)
    gh = gh_2025.unionByName(gh_supp)
    print("INFO: supplement data loaded and unioned")
except Exception:
    gh = gh_2025
    print("INFO: supplement not found, using 2025 data only")

top_repos = gh.groupBy("repo_name").agg(
    F.count(F.when(F.col("event_type") == "WatchEvent", 1)).alias("stars"),
    F.count(F.when(F.col("event_type") == "ForkEvent", 1)).alias("forks"),
    F.count(F.when(F.col("event_type") == "PushEvent", 1)).alias("pushes"),
    F.count(F.when(F.col("event_type") == "PullRequestEvent", 1)).alias("prs"),
    F.count(F.when(F.col("event_type") == "IssuesEvent", 1)).alias("issues"),
    F.countDistinct("actor_login").alias("distinct_actors"),
    F.countDistinct("event_date").alias("active_days"),
    F.count("event_id").alias("total_events")
).orderBy(F.desc("stars")).limit(1000).cache()

top_repos.write.mode("overwrite").parquet(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/top_repos_all")
top_repos.coalesce(1).write.mode("overwrite").csv(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/top_repos_all_csv", header=True)

print(f"Done. Rows: {top_repos.count()}")
spark.stop()
