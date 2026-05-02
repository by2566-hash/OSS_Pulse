"""
Job 05: ai_vs_general
----------------------
Input:  /user/jl17797_nyu_edu/oss_pulse/analytics/top_repos_all
        /user/jl17797_nyu_edu/oss_pulse/source/seed_repos.json
Output: /user/jl17797_nyu_edu/oss_pulse/analytics/ai_vs_general

Description:
  Tag each repo in top_repos_all as AI or general.
  Compute group-level stats for comparison:
    - median/mean stars, forks, pushes, PRs
    - active_days distribution
    - contributor concentration

  Depends on: Job 04

Run:
  spark-submit /tmp/05_ai_vs_general.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

spark = SparkSession.builder.appName("05_AI_vs_General").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load seed set — use native isin() instead of Python UDF (no JVM↔Python serialization)
seed_raw = spark.sparkContext.wholeTextFiles(
    "/user/jl17797_nyu_edu/oss_pulse/source/seed_repos.json"
).collect()
seed_list = [r["repo"].lower() for r in json.loads(seed_raw[0][1])]

top = spark.read.parquet("/user/jl17797_nyu_edu/oss_pulse/analytics/top_repos_all")
top = top.withColumn("is_ai", F.col("repo_name").isin(seed_list))

# Per-repo with label
top.write.mode("overwrite").parquet(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/ai_vs_general")
top.coalesce(1).write.mode("overwrite").csv(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/ai_vs_general_csv", header=True)

# Group summary
summary = top.groupBy("is_ai").agg(
    F.count("*").alias("repo_count"),
    F.round(F.avg("stars"), 1).alias("avg_stars"),
    F.round(F.avg("forks"), 1).alias("avg_forks"),
    F.round(F.avg("pushes"), 1).alias("avg_pushes"),
    F.round(F.avg("prs"), 1).alias("avg_prs"),
    F.round(F.avg("distinct_actors"), 1).alias("avg_contributors"),
    F.round(F.avg("active_days"), 1).alias("avg_active_days"),
)
summary.show()
summary.coalesce(1).write.mode("overwrite").csv(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/ai_vs_general_summary_csv", header=True)

print("Done.")
spark.stop()
