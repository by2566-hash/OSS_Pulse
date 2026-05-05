"""
Job 08 hotfix: rerun 2026-Q1 intermediate (fixed merged PR logic) + merge all 5 eras.
Only processes 2026 — reads 2022-2025 from existing intermediate results.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("08_Rerun2026Merge").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "24")

OUT = "/user/jl17797_nyu_edu/oss_pulse/analytics/era_comparison"
INTER = f"{OUT}/intermediate"

# ── Reprocess 2026-Q1 with fixed merged PR logic ────────────────────────────

path = "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1"
era = "2026-Q1"
era_tag = "2026_Q1"
print(f"\n[INFO] Reprocessing {era} from {path}")

df = spark.read.parquet(path)
cols = ["event_type", "repo_name", "actor_login", "event_date",
        "push_distinct_size", "pr_merged", "pr_number", "payload_action"]
available = set(df.columns)
df = df.select(*[c for c in cols if c in available])
df.cache()
print(f"[INFO] {era}: {df.count():,} events loaded")

# 1. Summary
summary = df.agg(
    F.lit(era).alias("era"),
    F.count("*").alias("total_events"),
    F.approx_count_distinct("repo_name", 0.01).alias("distinct_repos"),
    F.approx_count_distinct("actor_login", 0.01).alias("distinct_actors"),
    F.sum(F.when(F.col("event_type") == "PushEvent", 1).otherwise(0))
     .alias("total_pushes"),
    F.countDistinct(F.when(
        F.col("event_type") == "PullRequestEvent", F.col("pr_number")
    )).alias("distinct_prs"),
    F.sum(F.when(F.col("event_type") == "PullRequestEvent", 1).otherwise(0))
     .alias("total_pr_events"),
    F.sum(F.when(F.col("event_type") == "WatchEvent", 1).otherwise(0))
     .alias("total_stars"),
    F.countDistinct(F.when(
        (F.col("event_type") == "PullRequestEvent") &
        (
            ((F.col("payload_action") == "closed") & (F.col("pr_merged").eqNullSafe(True))) |
            (F.col("payload_action") == "merged")
        ),
        F.col("pr_number")
    )).alias("merged_prs"),
    F.avg(F.when(
        F.col("event_type") == "PushEvent", F.col("push_distinct_size")
    )).alias("avg_commit_size"),
    F.expr(
        "percentile_approx(CASE WHEN event_type='PushEvent' "
        "THEN push_distinct_size END, 0.5)"
    ).alias("median_commit_size"),
)
summary.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{INTER}/summary_{era_tag}")
print(f"[OK] summary done")

# 2. Push size distribution
push_dist = df.filter(
    (F.col("event_type") == "PushEvent") &
    F.col("push_distinct_size").isNotNull()
).agg(
    F.lit(era).alias("era"),
    F.sum(F.when(F.col("push_distinct_size") == 1, 1).otherwise(0))
     .alias("commits_eq_1"),
    F.sum(F.when(F.col("push_distinct_size").between(2, 5), 1).otherwise(0))
     .alias("commits_2_5"),
    F.sum(F.when(F.col("push_distinct_size").between(6, 20), 1).otherwise(0))
     .alias("commits_6_20"),
    F.sum(F.when(F.col("push_distinct_size") > 20, 1).otherwise(0))
     .alias("commits_gt_20"),
    F.count("*").alias("total_push_events"),
)
push_dist.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{INTER}/push_dist_{era_tag}")
print(f"[OK] push_dist done")

# 3. PR/Push ratio
repo_metrics = (
    df.groupBy("repo_name")
    .agg(
        F.sum(F.when(F.col("event_type") == "PushEvent", 1).otherwise(0))
         .alias("pushes"),
        F.countDistinct(F.when(
            F.col("event_type") == "PullRequestEvent", F.col("pr_number")
        )).alias("prs"),
        F.countDistinct("actor_login").alias("contributors"),
    )
    .filter(F.col("pushes") >= 10)
    .withColumn("pr_push_ratio", F.col("prs") / F.col("pushes"))
)
pr_ratio = repo_metrics.agg(
    F.lit(era).alias("era"),
    F.count("repo_name").alias("repo_count"),
    F.avg("pr_push_ratio").alias("avg_pr_push_ratio"),
    F.expr("percentile_approx(pr_push_ratio, 0.25)").alias("p25_pr_push_ratio"),
    F.expr("percentile_approx(pr_push_ratio, 0.5)").alias("median_pr_push_ratio"),
    F.expr("percentile_approx(pr_push_ratio, 0.75)").alias("p75_pr_push_ratio"),
    F.avg("contributors").alias("avg_contributors_per_repo"),
)
pr_ratio.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{INTER}/pr_ratio_{era_tag}")
print(f"[OK] pr_ratio done")

# 4. Active repos per month
monthly = (
    df.withColumn("month", F.date_format("event_date", "yyyy-MM"))
    .groupBy("month")
    .agg(
        F.lit(era).alias("era"),
        F.countDistinct("repo_name").alias("active_repos"),
        F.countDistinct("actor_login").alias("active_actors"),
        F.count("*").alias("total_events"),
    )
)
monthly.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{INTER}/monthly_{era_tag}")
print(f"[OK] monthly done")

df.unpersist()
print(f"[OK] 2026-Q1 intermediate rewritten")

# ── Merge all 5 eras ─────────────────────────────────────────────────────────

print("\n[INFO] Merging all 5 eras...")

ERAS = ["2022_Q1", "2023_Q1", "2024_Q1", "2025_Q1", "2026_Q1"]
METRICS = {
    "summary":   "summary_metrics",
    "push_dist": "push_size_distribution",
    "pr_ratio":  "pr_push_ratio",
    "monthly":   "active_repos_monthly",
}

for prefix, output_name in METRICS.items():
    dfs = []
    for et in ERAS:
        dfs.append(spark.read.option("header", True).csv(f"{INTER}/{prefix}_{et}"))
    merged = dfs[0]
    for d in dfs[1:]:
        merged = merged.unionByName(d, allowMissingColumns=True)
    if "month" in merged.columns:
        merged = merged.orderBy("era", "month")
    else:
        merged = merged.orderBy("era")
    merged.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT}/{output_name}")
    print(f"[OK] {output_name} written")

print(f"\n[DONE] All outputs written to {OUT}")
spark.stop()
