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
    for col_name in cols:
        if col_name not in available:
            df = df.withColumn(col_name, F.lit(None))
    df = df.select(*cols)
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
        F.sum(F.when(
            (F.col("event_type") == "PushEvent") &
            F.col("push_distinct_size").isNotNull(),
            1
        ).otherwise(0)).alias("push_detail_events"),
        F.sum(F.when(
            (F.col("event_type") == "PullRequestEvent") &
            F.col("pr_merged").isNotNull(),
            1
        ).otherwise(0)).alias("pr_merged_events"),
        F.sum(F.when(
            (F.col("event_type") == "PullRequestEvent") &
            (F.col("payload_action") == "merged"),
            1
        ).otherwise(0)).alias("action_merged_events"),
        F.countDistinct(F.when(
            (F.col("event_type") == "PullRequestEvent") &
            (F.col("payload_action") == "closed") &
            (F.col("pr_merged").eqNullSafe(True)),
            F.col("pr_number")
        )).alias("raw_merged_prs"),
        F.countDistinct(F.when(
            (F.col("event_type") == "PullRequestEvent") &
            (F.col("payload_action") == "merged"),
            F.col("pr_number")
        )).alias("action_merged_prs"),
        F.avg(F.when(
            F.col("event_type") == "PushEvent", F.col("push_distinct_size")
        )).alias("avg_commit_size"),
        F.expr(
            "percentile_approx(CASE WHEN event_type='PushEvent' "
            "THEN push_distinct_size END, 0.5)"
        ).alias("median_commit_size"),
    ).withColumn(
        "commit_size_available", F.col("push_detail_events") > 0
    ).withColumn(
        "pr_merge_source",
        F.when(F.col("pr_merged_events") > 0, F.lit("pr_merged"))
         .when(F.col("action_merged_events") > 0, F.lit("payload_action"))
         .otherwise(F.lit("unavailable"))
    ).withColumn(
        "pr_merge_available", F.col("pr_merge_source") != "unavailable"
    ).withColumn(
        "pr_merge_events",
        F.when(F.col("pr_merge_source") == "pr_merged", F.col("pr_merged_events"))
         .when(F.col("pr_merge_source") == "payload_action", F.col("action_merged_events"))
         .otherwise(F.lit(0))
    ).withColumn(
        "merged_prs",
        F.when(F.col("pr_merge_source") == "pr_merged", F.col("raw_merged_prs"))
         .when(F.col("pr_merge_source") == "payload_action", F.col("action_merged_prs"))
         .otherwise(F.lit(None).cast("long"))
    ).withColumn(
        "avg_commit_size",
        F.when(F.col("commit_size_available"), F.col("avg_commit_size"))
         .otherwise(F.lit(None).cast("double"))
    ).withColumn(
        "median_commit_size",
        F.when(F.col("commit_size_available"), F.col("median_commit_size"))
         .otherwise(F.lit(None).cast("long"))
    ).select(
        "era",
        "total_events",
        "distinct_repos",
        "distinct_actors",
        "total_pushes",
        "distinct_prs",
        "total_pr_events",
        "total_stars",
        "merged_prs",
        "avg_commit_size",
        "median_commit_size",
        "push_detail_events",
        "commit_size_available",
        "pr_merge_events",
        "pr_merge_available",
        "pr_merge_source",
    )
    summary_rows.append(summary)
    print(f"[OK] {era} summary done")

    # ── 2. Push size distribution ─────────────────────────────────────────
    push_dist = df.filter(F.col("event_type") == "PushEvent").agg(
        F.lit(era).alias("era"),
        F.sum(F.when(F.col("push_distinct_size").isNotNull(), 1).otherwise(0))
         .alias("push_detail_events"),
        F.sum(F.when(F.col("push_distinct_size") == 0, 1).otherwise(0))
         .alias("commits_eq_0"),
        F.sum(F.when(F.col("push_distinct_size") == 1, 1).otherwise(0))
         .alias("commits_eq_1"),
        F.sum(F.when(F.col("push_distinct_size").between(2, 5), 1).otherwise(0))
         .alias("commits_2_5"),
        F.sum(F.when(F.col("push_distinct_size").between(6, 20), 1).otherwise(0))
         .alias("commits_6_20"),
        F.sum(F.when(F.col("push_distinct_size") > 20, 1).otherwise(0))
         .alias("commits_gt_20"),
        F.count("*").alias("total_pushes"),
    ).withColumn(
        "commit_size_available", F.col("push_detail_events") > 0
    ).withColumn(
        "total_push_events",
        F.when(F.col("commit_size_available"), F.col("push_detail_events"))
         .otherwise(F.lit(None).cast("long"))
    ).withColumn(
        "commits_eq_0",
        F.when(F.col("commit_size_available"), F.col("commits_eq_0"))
         .otherwise(F.lit(None).cast("long"))
    ).withColumn(
        "commits_eq_1",
        F.when(F.col("commit_size_available"), F.col("commits_eq_1"))
         .otherwise(F.lit(None).cast("long"))
    ).withColumn(
        "commits_2_5",
        F.when(F.col("commit_size_available"), F.col("commits_2_5"))
         .otherwise(F.lit(None).cast("long"))
    ).withColumn(
        "commits_6_20",
        F.when(F.col("commit_size_available"), F.col("commits_6_20"))
         .otherwise(F.lit(None).cast("long"))
    ).withColumn(
        "commits_gt_20",
        F.when(F.col("commit_size_available"), F.col("commits_gt_20"))
         .otherwise(F.lit(None).cast("long"))
    ).select(
        "era",
        "commits_eq_0",
        "commits_eq_1",
        "commits_2_5",
        "commits_6_20",
        "commits_gt_20",
        "total_push_events",
        "total_pushes",
        "push_detail_events",
        "commit_size_available",
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


def long_col(name):
    return F.col(name).cast("long")


def normalize_summary(df):
    return (
        df.withColumn("total_pushes", long_col("total_pushes"))
        .withColumn("total_pr_events", long_col("total_pr_events"))
        .withColumn("merged_prs", long_col("merged_prs"))
        .withColumn(
            "push_detail_events",
            F.coalesce(
                long_col("push_detail_events"),
                F.when(F.col("avg_commit_size").isNotNull(), long_col("total_pushes")),
            ),
        )
        .withColumn(
            "commit_size_available",
            F.coalesce(
                F.col("commit_size_available").cast("boolean"),
                F.col("avg_commit_size").isNotNull(),
            ),
        )
        .withColumn(
            "pr_merge_events",
            F.coalesce(
                long_col("pr_merge_events"),
                F.when(F.col("merged_prs").isNotNull(), long_col("total_pr_events")),
            ),
        )
        .withColumn(
            "pr_merge_available",
            F.coalesce(
                F.col("pr_merge_available").cast("boolean"),
                F.col("merged_prs").isNotNull(),
            ),
        )
        .withColumn(
            "pr_merge_source",
            F.coalesce(
                F.col("pr_merge_source"),
                F.when(F.col("merged_prs").isNotNull(), F.lit("pr_merged"))
                .otherwise(F.lit("unavailable")),
            ),
        )
        .withColumn(
            "merged_prs",
            F.when(F.col("pr_merge_available"), F.col("merged_prs"))
            .otherwise(F.lit(None).cast("long")),
        )
    )


def normalize_push_distribution(df):
    bucket_cols = ["commits_eq_1", "commits_2_5", "commits_6_20", "commits_gt_20"]
    for col_name in bucket_cols + [
        "commits_eq_0",
        "total_push_events",
        "total_pushes",
        "push_detail_events",
    ]:
        df = df.withColumn(col_name, long_col(col_name))

    known_bucket_sum = F.lit(0)
    for col_name in bucket_cols:
        known_bucket_sum = known_bucket_sum + F.coalesce(F.col(col_name), F.lit(0))

    return (
        df.withColumn(
            "commits_eq_0",
            F.coalesce(F.col("commits_eq_0"), F.col("total_push_events") - known_bucket_sum),
        )
        .withColumn(
            "push_detail_events",
            F.coalesce(F.col("push_detail_events"), F.col("total_push_events")),
        )
        .withColumn("total_pushes", F.coalesce(F.col("total_pushes"), F.col("total_push_events")))
        .withColumn(
            "commit_size_available",
            F.coalesce(
                F.col("commit_size_available").cast("boolean"),
                F.col("push_detail_events") > 0,
            ),
        )
        .withColumn(
            "total_push_events",
            F.when(F.col("commit_size_available"), F.col("total_push_events"))
            .otherwise(F.lit(None).cast("long")),
        )
    )

# Load 2022/2023 intermediate results
INTER = f"{OUT}/intermediate"
PREV_ERAS = ["2022_Q1", "2023_Q1"]

for era_tag in PREV_ERAS:
    summary_rows.insert(0, spark.read.option("header", True).csv(f"{INTER}/summary_{era_tag}"))
    push_dist_rows.insert(0, spark.read.option("header", True).csv(f"{INTER}/push_dist_{era_tag}"))
    pr_ratio_rows.insert(0, spark.read.option("header", True).csv(f"{INTER}/pr_ratio_{era_tag}"))
    monthly_rows.insert(0, spark.read.option("header", True).csv(f"{INTER}/monthly_{era_tag}"))
print("[OK] 2022/2023 intermediate loaded")

summary_final = normalize_summary(union_all(summary_rows))
push_dist_final = normalize_push_distribution(union_all(push_dist_rows))
pr_ratio_final = union_all(pr_ratio_rows)
monthly_final = union_all(monthly_rows)

(summary_final.orderBy("era").coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUT}/summary_metrics"))
print("[OK] summary_metrics written")

(push_dist_final.orderBy("era").coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUT}/push_size_distribution"))
print("[OK] push_size_distribution written")

(pr_ratio_final.orderBy("era").coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUT}/pr_push_ratio"))
print("[OK] pr_push_ratio written")

(monthly_final.orderBy("era", "month").coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{OUT}/active_repos_monthly"))
print("[OK] active_repos_monthly written")

print(f"\n[DONE] All outputs written to {OUT}")
spark.stop()
