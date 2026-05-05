"""
Job 09: repo_era_deep_dive (per-era design)
---------------------------------------------
Deep-dive comparison of AI vs Non-AI repos across 5 Q1 eras (2022-2026).

Target repos:
  AI:     seed_repos.json (~50 repos)
  Non-AI: top 200 non-AI repos from top_repos_all (by stars)

Metrics per repo per era:
  1. pr_merge_time    -- median/p75/p90 hours from PR open to merge
  2. daily_pr_count   -- daily PR open count summary
  3. daily_commits    -- daily push_distinct_size summary
  4. contributor_flow -- monthly new vs returning contributors

Output also includes group-level aggregates (AI vs Non-AI) for each metric.

Design: Per-era processing to avoid memory pressure.
Each era filters to ~250 target repos → small dataset.

Run:
  spark-submit /tmp/09_repo_era_deep_dive.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

spark = SparkSession.builder.appName("09_RepoEraDeepdive").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "24")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

OUT = "/user/jl17797_nyu_edu/oss_pulse/analytics/repo_era_deep_dive"

# ── Build target repo list: seed AI repos + top 200 non-AI ────────────────────

seed_raw = spark.sparkContext.wholeTextFiles(
    "/user/jl17797_nyu_edu/oss_pulse/source/seed_repos.json"
).collect()
seed_set = set(r["repo"].lower() for r in json.loads(seed_raw[0][1]))

top_repos_df = spark.read.parquet(
    "/user/jl17797_nyu_edu/oss_pulse/analytics/top_repos_all"
)
non_ai_repos = (
    top_repos_df
    .filter(~F.col("repo_name").isin(list(seed_set)))
    .orderBy(F.desc("stars"))
    .limit(200)
    .select("repo_name")
    .rdd.flatMap(lambda x: x).collect()
)

TARGET_REPOS = list(seed_set) + non_ai_repos
TARGET_SET = set(TARGET_REPOS)
print(f"[INFO] Target repos: {len(seed_set)} AI + {len(non_ai_repos)} non-AI = {len(TARGET_SET)}")

ERA_SOURCES = [
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2022q1", "2022-Q1", None),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2023q1", "2023-Q1", None),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2024q1", "2024-Q1", None),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2025q1", "2025-Q1", None),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1", "2026-Q1", None),
]

COLS = ["event_id", "event_type", "event_ts", "event_date",
        "actor_login", "repo_name", "payload_action",
        "pr_number", "pr_merged", "push_distinct_size"]

# ── Per-era processing ────────────────────────────────────────────────────────

pr_merge_rows = []
daily_pr_rows = []
daily_pr_ts_rows = []
daily_commits_rows = []
daily_commits_ts_rows = []
contributor_flow_rows = []

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

    # Filter to target repos only → small dataset
    available = set(df.columns)
    select_cols = [c for c in COLS if c in available]
    seed_list = list(seed_set)
    events = (
        df.filter(F.col("repo_name").isin(TARGET_REPOS))
        .select(*select_cols)
        .withColumn("era", F.lit(era))
        .withColumn("is_ai", F.col("repo_name").isin(seed_list))
        .cache()
    )

    count = events.count()
    print(f"[INFO] {era}: {count:,} events for target repos")

    if count == 0:
        events.unpersist()
        continue

    # ── 1. PR Merge Time ──────────────────────────────────────────────────

    pr_opened = (
        events
        .filter(
            (F.col("event_type") == "PullRequestEvent") &
            (F.col("payload_action") == "opened")
        )
        .select("era", "repo_name", "is_ai", "pr_number",
                F.col("event_ts").alias("opened_ts"))
    )

    pr_closed_merged = (
        events
        .filter(
            (F.col("event_type") == "PullRequestEvent") &
            ((F.col("payload_action") == "closed") & (F.col("pr_merged").eqNullSafe(True))) |
            (F.col("payload_action") == "merged")   # 2026+ schema change
        )
        .select("era", "repo_name", "pr_number",
                F.col("event_ts").alias("merged_ts"))
    )

    pr_times = (
        pr_opened.join(pr_closed_merged,
                       on=["era", "repo_name", "pr_number"], how="inner")
        .withColumn("merge_hours",
            (F.unix_timestamp("merged_ts") - F.unix_timestamp("opened_ts")) / 3600)
        .filter(F.col("merge_hours").between(0, 8760))
    )

    pr_merge_time = (
        pr_times.groupBy("era", "repo_name", "is_ai").agg(
            F.count("pr_number").alias("merged_pr_count"),
            F.avg("merge_hours").alias("avg_merge_hours"),
            F.expr("percentile_approx(merge_hours, 0.5)").alias("median_merge_hours"),
            F.expr("percentile_approx(merge_hours, 0.75)").alias("p75_merge_hours"),
            F.expr("percentile_approx(merge_hours, 0.90)").alias("p90_merge_hours"),
        )
    )
    pr_merge_rows.append(pr_merge_time)
    print(f"[OK] {era} pr_merge_time done")

    # ── 2. Daily PR Count ─────────────────────────────────────────────────

    daily_pr = (
        events
        .filter(
            (F.col("event_type") == "PullRequestEvent") &
            (F.col("payload_action") == "opened")
        )
        .groupBy("era", "repo_name", "is_ai", "event_date")
        .agg(F.count("*").alias("pr_opened"))
    )

    daily_pr_summary = (
        daily_pr.groupBy("era", "repo_name", "is_ai").agg(
            F.sum("pr_opened").alias("total_prs"),
            F.avg("pr_opened").alias("avg_daily_prs"),
            F.expr("percentile_approx(pr_opened, 0.5)").alias("median_daily_prs"),
            F.max("pr_opened").alias("peak_daily_prs"),
        )
    )
    daily_pr_rows.append(daily_pr_summary)
    daily_pr_ts_rows.append(daily_pr)
    print(f"[OK] {era} daily_pr done")

    # ── 3. Daily Commits ──────────────────────────────────────────────────

    daily_commits = (
        events
        .filter(F.col("event_type") == "PushEvent")
        .filter(F.col("push_distinct_size").isNotNull())
        .groupBy("era", "repo_name", "is_ai", "event_date")
        .agg(
            F.sum("push_distinct_size").alias("daily_commits"),
            F.count("*").alias("daily_pushes"),
            F.avg("push_distinct_size").alias("avg_commits_per_push"),
        )
    )

    daily_commits_summary = (
        daily_commits.groupBy("era", "repo_name", "is_ai").agg(
            F.sum("daily_commits").alias("total_commits"),
            F.avg("daily_commits").alias("avg_daily_commits"),
            F.expr("percentile_approx(daily_commits, 0.5)").alias("median_daily_commits"),
            F.avg("avg_commits_per_push").alias("avg_commits_per_push"),
        )
    )
    daily_commits_rows.append(daily_commits_summary)
    daily_commits_ts_rows.append(daily_commits)
    print(f"[OK] {era} daily_commits done")

    # ── 4. Contributor Flow ───────────────────────────────────────────────

    all_actors = (
        events
        .filter(F.col("actor_login").isNotNull())
        .withColumn("month", F.date_format("event_date", "yyyy-MM"))
        .select("era", "repo_name", "is_ai", "month", "actor_login")
        .distinct()
    )

    first_seen = (
        all_actors
        .groupBy("era", "repo_name", "actor_login")
        .agg(F.min("month").alias("first_month"))
    )

    actor_monthly = all_actors.join(
        first_seen, on=["era", "repo_name", "actor_login"], how="left"
    ).withColumn(
        "contributor_type",
        F.when(F.col("month") == F.col("first_month"), "new").otherwise("returning")
    )

    contributor_flow = (
        actor_monthly
        .groupBy("era", "repo_name", "is_ai", "month", "contributor_type")
        .agg(F.countDistinct("actor_login").alias("contributors"))
    )
    contributor_flow_rows.append(contributor_flow)
    print(f"[OK] {era} contributor_flow done")

    events.unpersist()
    print(f"[OK] {era} complete")

# ── Merge and write ───────────────────────────────────────────────────────────

print("\n[INFO] Merging and writing results...")

def union_all(dfs):
    result = dfs[0]
    for d in dfs[1:]:
        result = result.unionByName(d, allowMissingColumns=True)
    return result

if pr_merge_rows:
    (union_all(pr_merge_rows).orderBy("repo_name", "era").coalesce(1)
     .write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/pr_merge_time"))
    print("[OK] pr_merge_time written")

if daily_pr_rows:
    (union_all(daily_pr_rows).orderBy("repo_name", "era").coalesce(1)
     .write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/daily_pr_count"))
    (union_all(daily_pr_ts_rows).orderBy("repo_name", "era", "event_date").coalesce(1)
     .write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/daily_pr_timeseries"))
    print("[OK] daily_pr written")

if daily_commits_rows:
    (union_all(daily_commits_rows).orderBy("repo_name", "era").coalesce(1)
     .write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/daily_commits"))
    (union_all(daily_commits_ts_rows).orderBy("repo_name", "era", "event_date").coalesce(1)
     .write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/daily_commits_timeseries"))
    print("[OK] daily_commits written")

if contributor_flow_rows:
    (union_all(contributor_flow_rows).orderBy("repo_name", "era", "month").coalesce(1)
     .write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/contributor_flow"))
    print("[OK] contributor_flow written")

# ── Group-level summaries (AI vs Non-AI per era) ─────────────────────────────

print("\n[INFO] Writing group-level summaries...")

if pr_merge_rows:
    all_pr = union_all(pr_merge_rows)
    group_pr = all_pr.groupBy("era", "is_ai").agg(
        F.count("repo_name").alias("repo_count"),
        F.avg("median_merge_hours").alias("avg_median_merge_hours"),
        F.avg("merged_pr_count").alias("avg_merged_pr_count"),
    ).orderBy("era", "is_ai")
    (group_pr.coalesce(1).write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/group_pr_merge_time"))
    print("[OK] group_pr_merge_time written")

if daily_commits_rows:
    all_commits = union_all(daily_commits_rows)
    group_commits = all_commits.groupBy("era", "is_ai").agg(
        F.count("repo_name").alias("repo_count"),
        F.avg("avg_daily_commits").alias("avg_daily_commits"),
        F.avg("avg_commits_per_push").alias("avg_commits_per_push"),
    ).orderBy("era", "is_ai")
    (group_commits.coalesce(1).write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/group_daily_commits"))
    print("[OK] group_daily_commits written")

if daily_pr_rows:
    all_prs = union_all(daily_pr_rows)
    group_prs = all_prs.groupBy("era", "is_ai").agg(
        F.count("repo_name").alias("repo_count"),
        F.avg("avg_daily_prs").alias("avg_daily_prs"),
        F.sum("total_prs").alias("total_prs"),
    ).orderBy("era", "is_ai")
    (group_prs.coalesce(1).write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/group_daily_prs"))
    print("[OK] group_daily_prs written")

print(f"\n[DONE] All outputs at {OUT}")
spark.stop()
