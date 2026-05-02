"""
Job 08: era_comparison
-----------------------
Compare GitHub ecosystem activity between 2022 Q1 (pre-ChatGPT baseline)
and 2025 Q1 (post-coding-agent era) across the full repository population.

Hypothesis: Coding agents (Copilot, Cursor, Claude Code etc.) have caused
measurable shifts in how developers interact with repositories:
  - Smaller, more frequent commits (push_distinct_size distribution shifts left)
  - Higher PR-to-push ratio (agents prefer PR workflow)
  - Higher PR merge rate (agent PRs more consistent quality)
  - More active repos and contributors (lower barrier to entry)

Input:
  2022 Q1: /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2022q1/
  2025 Q1: /user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/
            filtered to event_date BETWEEN 2025-01-01 AND 2025-03-31

Output:
  /user/jl17797_nyu_edu/oss_pulse/analytics/era_comparison/
    summary_metrics.csv        -- high-level aggregate stats
    push_size_distribution.csv -- commit size percentile buckets
    pr_push_ratio.csv          -- per-repo PR/push ratio distribution
    active_repos_monthly.csv   -- distinct active repos per month

Run:
  spark-submit /tmp/08_era_comparison.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("08_EraComparison").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

OUT = "/user/jl17797_nyu_edu/oss_pulse/analytics/era_comparison"

# ── Load data ─────────────────────────────────────────────────────────────────

gh_2022 = (
    spark.read.parquet("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2022q1/")
    .withColumn("era", F.lit("2022-Q1"))
)

gh_2025 = (
    spark.read.parquet("/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/")
    .filter(F.col("event_date").between("2025-01-01", "2025-03-31"))
    .withColumn("era", F.lit("2025-Q1"))
)

gh = gh_2022.unionByName(gh_2025)

# ── 1. Summary metrics ────────────────────────────────────────────────────────
# High-level aggregates per era: total events, distinct repos, actors, etc.

summary = gh.groupBy("era").agg(
    F.count("*").alias("total_events"),
    F.countDistinct("repo_name").alias("distinct_repos"),
    F.countDistinct("actor_login").alias("distinct_actors"),
    F.sum(F.when(F.col("event_type") == "PushEvent", 1).otherwise(0)).alias("total_pushes"),
    F.sum(F.when(F.col("event_type") == "PullRequestEvent", 1).otherwise(0)).alias("total_prs"),
    F.sum(F.when(F.col("event_type") == "WatchEvent", 1).otherwise(0)).alias("total_stars"),
    F.sum(F.when(
        (F.col("event_type") == "PullRequestEvent") & (F.col("pr_merged") == True), 1
    ).otherwise(0)).alias("merged_prs"),
    F.avg(F.when(
        F.col("event_type") == "PushEvent", F.col("push_distinct_size")
    )).alias("avg_commit_size"),
    F.expr("percentile_approx(CASE WHEN event_type='PushEvent' THEN push_distinct_size END, 0.5)")
     .alias("median_commit_size"),
)

(summary.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/summary_metrics"))

# ── 2. Push size distribution (percentile buckets) ────────────────────────────
# Shows whether commits are getting smaller (agent-style: 1 commit per push)

pushes = gh.filter(F.col("event_type") == "PushEvent").filter(F.col("push_distinct_size").isNotNull())

push_dist = pushes.groupBy("era").agg(
    F.sum(F.when(F.col("push_distinct_size") == 1, 1).otherwise(0)).alias("commits_eq_1"),
    F.sum(F.when(F.col("push_distinct_size").between(2, 5), 1).otherwise(0)).alias("commits_2_5"),
    F.sum(F.when(F.col("push_distinct_size").between(6, 20), 1).otherwise(0)).alias("commits_6_20"),
    F.sum(F.when(F.col("push_distinct_size") > 20, 1).otherwise(0)).alias("commits_gt_20"),
    F.count("*").alias("total_push_events"),
)

(push_dist.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/push_size_distribution"))

# ── 3. PR / Push ratio per repo ───────────────────────────────────────────────
# Higher ratio → more PR-driven workflow (agent preference)
# Only repos with >= 10 push events in both eras for fair comparison

repo_metrics = gh.groupBy("era", "repo_name").agg(
    F.sum(F.when(F.col("event_type") == "PushEvent", 1).otherwise(0)).alias("pushes"),
    F.sum(F.when(F.col("event_type") == "PullRequestEvent", 1).otherwise(0)).alias("prs"),
    F.countDistinct("actor_login").alias("contributors"),
).filter(F.col("pushes") >= 10)

repo_metrics = repo_metrics.withColumn(
    "pr_push_ratio", F.col("prs") / F.col("pushes")
)

# Aggregate distribution of pr_push_ratio across all repos per era
pr_ratio_dist = repo_metrics.groupBy("era").agg(
    F.count("repo_name").alias("repo_count"),
    F.avg("pr_push_ratio").alias("avg_pr_push_ratio"),
    F.expr("percentile_approx(pr_push_ratio, 0.25)").alias("p25_pr_push_ratio"),
    F.expr("percentile_approx(pr_push_ratio, 0.5)").alias("median_pr_push_ratio"),
    F.expr("percentile_approx(pr_push_ratio, 0.75)").alias("p75_pr_push_ratio"),
    F.avg("contributors").alias("avg_contributors_per_repo"),
)

(pr_ratio_dist.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/pr_push_ratio"))

# ── 4. Active repos per month ─────────────────────────────────────────────────
# How many distinct repos had activity each month? (ecosystem breadth)

monthly = gh.withColumn("month", F.date_format("event_date", "yyyy-MM"))

active_monthly = monthly.groupBy("era", "month").agg(
    F.countDistinct("repo_name").alias("active_repos"),
    F.countDistinct("actor_login").alias("active_actors"),
    F.count("*").alias("total_events"),
)

(active_monthly.orderBy("era", "month")
 .coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/active_repos_monthly"))

print(f"Done. Output: {OUT}")
spark.stop()
