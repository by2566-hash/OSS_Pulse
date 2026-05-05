"""
Job 08: era_comparison (per-era design)
----------------------------------------
Compare GitHub ecosystem activity across 5 Q1 snapshots (2022-2026),
spanning pre-ChatGPT baseline through the coding-agent era.

Design: Process each era independently (15-23 GB each), then merge.
This avoids the 90 GB union that exceeds executor memory and causes
YARN kills.

Timeline:
  2022-Q1  Pre-ChatGPT, early GitHub Copilot
  2023-Q1  ChatGPT boom, Copilot GA
  2024-Q1  LLM explosion (GPT-4, Claude, Gemini)
  2025-Q1  Agent era begins (Cursor, Claude Code, Devin)
  2026-Q1  Full coding-agent saturation

Output:
  /user/jl17797_nyu_edu/oss_pulse/analytics/era_comparison/
    summary_metrics/         -- high-level aggregate stats per era
    push_size_distribution/  -- commit size buckets per era
    pr_push_ratio/           -- per-repo PR/push ratio distribution per era
    active_repos_monthly/    -- distinct active repos per month per era

Run:
  spark-submit /tmp/08_era_comparison.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("08_EraComparison").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "24")

OUT = "/user/jl17797_nyu_edu/oss_pulse/analytics/era_comparison"

ERA_SOURCES = [
    # 2022 and 2023 intermediate results already saved — skip
    # ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2022q1", "2022-Q1", None),
    # ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2023q1", "2023-Q1", None),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2024q1", "2024-Q1", None),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2025q1", "2025-Q1", None),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1", "2026-Q1", None),
]

# ── Per-era processing ────────────────────────────────────────────────────────

summary_rows = []
push_dist_rows = []
pr_ratio_rows = []
monthly_rows = []

for path, era, date_filter in ERA_SOURCES:
    print(f"\n[INFO] Processing {era} from {path}")

    try:
        df = spark.read.parquet(path)
    except Exception as e:
        print(f"[WARN] Could not load {era}: {e}")
        continue

    if date_filter:
        start, end = date_filter
        df = df.filter(F.col("event_date").between(start, end))

    # Select only columns we need across all 4 aggregations
    cols = ["event_type", "repo_name", "actor_login", "event_date",
            "push_distinct_size", "pr_merged", "pr_number", "payload_action"]
    available = set(df.columns)
    df = df.select(*[c for c in cols if c in available])
    df.cache()
    row_count = df.count()
    print(f"[INFO] {era}: {row_count:,} events loaded")

    # ── 1. Summary metrics ────────────────────────────────────────────────
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
            (F.col("payload_action") == "closed") &
            (F.col("pr_merged").eqNullSafe(True)),
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
    summary_rows.append(summary)
    print(f"[OK] {era} summary done")

    # ── 2. Push size distribution ─────────────────────────────────────────
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
    push_dist_rows.append(push_dist)
    print(f"[OK] {era} push_size done")

    # ── 3. PR / Push ratio per repo ───────────────────────────────────────
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
    pr_ratio_rows.append(pr_ratio)
    print(f"[OK] {era} pr_push_ratio done")

    # ── 4. Active repos per month ─────────────────────────────────────────
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
    monthly_rows.append(monthly)
    print(f"[OK] {era} active_monthly done")

    # ── Write intermediate results per era (crash-safe) ───────────────────
    era_tag = era.replace("-", "_")
    summary.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT}/intermediate/summary_{era_tag}")
    push_dist.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT}/intermediate/push_dist_{era_tag}")
    pr_ratio.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT}/intermediate/pr_ratio_{era_tag}")
    monthly.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT}/intermediate/monthly_{era_tag}")
    print(f"[OK] {era} intermediate results saved to HDFS")

    df.unpersist()
    print(f"[OK] {era} complete")

# ── Merge and write final CSVs ────────────────────────────────────────────────

print("\n[INFO] Merging results (including 2022/2023 from intermediate)...")

def union_all(dfs):
    result = dfs[0]
    for d in dfs[1:]:
        result = result.unionByName(d, allowMissingColumns=True)
    return result

# Load 2022/2023 intermediate results
INTER = f"{OUT}/intermediate"
PREV_ERAS = ["2022_Q1", "2023_Q1"]

for era_tag in PREV_ERAS:
    summary_rows.insert(0, spark.read.option("header", True).csv(f"{INTER}/summary_{era_tag}"))
    push_dist_rows.insert(0, spark.read.option("header", True).csv(f"{INTER}/push_dist_{era_tag}"))
    pr_ratio_rows.insert(0, spark.read.option("header", True).csv(f"{INTER}/pr_ratio_{era_tag}"))
    monthly_rows.insert(0, spark.read.option("header", True).csv(f"{INTER}/monthly_{era_tag}"))
print("[OK] 2022/2023 intermediate loaded")

(union_all(summary_rows).orderBy("era").coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUT}/summary_metrics"))
print("[OK] summary_metrics written")

(union_all(push_dist_rows).orderBy("era").coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUT}/push_size_distribution"))
print("[OK] push_size_distribution written")

(union_all(pr_ratio_rows).orderBy("era").coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUT}/pr_push_ratio"))
print("[OK] pr_push_ratio written")

(union_all(monthly_rows).orderBy("era", "month").coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUT}/active_repos_monthly"))
print("[OK] active_repos_monthly written")

print(f"\n[DONE] All outputs written to {OUT}")
spark.stop()
