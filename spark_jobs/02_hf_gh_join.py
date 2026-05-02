"""
Job 02: hf_gh_join
------------------
Input:  /user/jl17797_nyu_edu/oss_pulse/cleaned/huggingface_hub
        /user/jl17797_nyu_edu/oss_pulse/analytics/repo_daily_metrics
        /user/jl17797_nyu_edu/oss_pulse/source/seed_repos.json
Output: /user/jl17797_nyu_edu/oss_pulse/analytics/hf_gh_join

Description:
  Join HF model metadata (aggregated by library_name) with GH daily metrics
  via seed_repos mapping (hf_library -> github repo).

Run:
  spark-submit /tmp/02_hf_gh_join.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("02_HF_GH_Join").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. HF: aggregate by library_name
hf = spark.read.parquet("/user/jl17797_nyu_edu/oss_pulse/cleaned/huggingface_hub")
hf_agg = hf.filter("library_name != 'unknown' AND library_name IS NOT NULL") \
  .groupBy("library_name").agg(
    F.count("*").alias("hf_model_count"),
    F.sum("downloads").alias("hf_total_downloads"),
    F.sum("likes").alias("hf_total_likes"),
    F.avg("downloads").alias("hf_avg_downloads")
  )

# 2. Seed: hf_library -> github repo
seed = spark.read.json("/user/jl17797_nyu_edu/oss_pulse/source/seed_repos.json") \
  .filter("hf_library IS NOT NULL") \
  .select(
    F.col("hf_library").alias("library_name"),
    F.lower(F.col("repo")).alias("repo_name"),
    F.col("category")
  )

# 3. GH: aggregate yearly
gh = spark.read.parquet("/user/jl17797_nyu_edu/oss_pulse/analytics/repo_daily_metrics") \
  .groupBy("repo_name").agg(
    F.sum("stars").alias("gh_stars_2025"),
    F.sum("forks").alias("gh_forks_2025"),
    F.sum("pushes").alias("gh_pushes_2025"),
    F.sum("prs").alias("gh_prs_2025"),
    F.sum("issues").alias("gh_issues_2025"),
    F.sum("distinct_actors").alias("gh_actors_2025"),
    F.countDistinct("event_date").alias("gh_active_days")
  )

# 4. Join
result = hf_agg.join(seed, on="library_name", how="inner") \
  .join(gh, on="repo_name", how="left") \
  .select("repo_name", "category", "library_name",
          "hf_model_count", "hf_total_downloads", "hf_total_likes",
          "gh_stars_2025", "gh_forks_2025", "gh_pushes_2025",
          "gh_prs_2025", "gh_issues_2025", "gh_active_days") \
  .orderBy(F.desc("hf_total_downloads"))

result.write.mode("overwrite").parquet(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/hf_gh_join")
result.coalesce(1).write.mode("overwrite").csv(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/hf_gh_join_csv", header=True)

print(f"Done. Rows: {result.count()}")
spark.stop()
