"""
Job 08: era_comparison
-----------------------
Compare GitHub ecosystem activity across 5 Q1 snapshots (2022–2026),
spanning pre-ChatGPT baseline through the coding-agent era.

Timeline:
  2022-Q1  Pre-ChatGPT, early GitHub Copilot
  2023-Q1  ChatGPT boom, Copilot GA
  2024-Q1  LLM explosion (GPT-4, Claude, Gemini)
  2025-Q1  Agent era begins (Cursor, Claude Code, Devin)
  2026-Q1  Full coding-agent saturation

Hypothesis: Coding agents have caused measurable shifts:
  - Smaller, more frequent commits (push_distinct_size shifts toward 1)
  - Higher PR-to-push ratio (agents prefer PR workflow)
  - Higher PR merge rate (agent PRs more consistent quality)
  - More active repos and contributors (lower barrier to entry)

Input:
  2022 Q1: /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2022q1/
  2023 Q1: /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2023q1/
  2024 Q1: /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2024q1/
  2025 Q1: /user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/
            filtered to event_date BETWEEN 2025-01-01 AND 2025-03-31
  2026 Q1: /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement/
            filtered to event_date BETWEEN 2026-01-01 AND 2026-03-31

Output:
  /user/jl17797_nyu_edu/oss_pulse/analytics/era_comparison/
    summary_metrics.csv        -- high-level aggregate stats per era
    push_size_distribution.csv -- commit size percentile buckets per era
    pr_push_ratio.csv          -- per-repo PR/push ratio distribution per era
    active_repos_monthly.csv   -- distinct active repos per month per era

Run:
  spark-submit /tmp/08_era_comparison.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("08_EraComparison").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "200")

OUT = "/user/jl17797_nyu_edu/oss_pulse/analytics/era_comparison"

# ── Load data ─────────────────────────────────────────────────────────────────

def load_era(path, era_label, date_filter=None):
    try:
        df = spark.read.parquet(path)
        if date_filter:
            start, end = date_filter
            df = df.filter(F.col("event_date").between(start, end))
        return df.withColumn("era", F.lit(era_label))
    except Exception as e:
        print(f"[WARN] Could not load {era_label} from {path}: {e}")
        return None

eras = [
    load_era(
        "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2022q1",
        "2022-Q1"
    ),
    load_era(
        "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2023q1",
        "2023-Q1"
    ),
    load_era(
        "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2024q1",
        "2024-Q1"
    ),
    load_era(
        "/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025",
        "2025-Q1",
        date_filter=("2025-01-01", "2025-03-31")
    ),
    load_era(
        "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement",
        "2026-Q1",
        date_filter=("2026-01-01", "2026-03-31")
    ),
]

eras = [e for e in eras if e is not None]
print(f"[INFO] Loaded {len(eras)} eras: {[e.select('era').first()[0] for e in eras]}")

gh = eras[0]
for e in eras[1:]:
    gh = gh.unionByName(e)

gh.cache()
gh.count()  # triggers cache materialisation before 4 aggregations

# ── 1. Summary metrics ────────────────────────────────────────────────────────

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
).orderBy("era")

(summary.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/summary_metrics"))

print("summary_metrics done")

# ── 2. Push size distribution (percentile buckets) ────────────────────────────

pushes = gh.filter(F.col("event_type") == "PushEvent").filter(F.col("push_distinct_size").isNotNull())

push_dist = pushes.groupBy("era").agg(
    F.sum(F.when(F.col("push_distinct_size") == 1, 1).otherwise(0)).alias("commits_eq_1"),
    F.sum(F.when(F.col("push_distinct_size").between(2, 5), 1).otherwise(0)).alias("commits_2_5"),
    F.sum(F.when(F.col("push_distinct_size").between(6, 20), 1).otherwise(0)).alias("commits_6_20"),
    F.sum(F.when(F.col("push_distinct_size") > 20, 1).otherwise(0)).alias("commits_gt_20"),
    F.count("*").alias("total_push_events"),
).orderBy("era")

(push_dist.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/push_size_distribution"))

print("push_size_distribution done")

# ── 3. PR / Push ratio per repo ───────────────────────────────────────────────

repo_metrics = gh.groupBy("era", "repo_name").agg(
    F.sum(F.when(F.col("event_type") == "PushEvent", 1).otherwise(0)).alias("pushes"),
    F.sum(F.when(F.col("event_type") == "PullRequestEvent", 1).otherwise(0)).alias("prs"),
    F.countDistinct("actor_login").alias("contributors"),
).filter(F.col("pushes") >= 10)

repo_metrics = repo_metrics.withColumn(
    "pr_push_ratio", F.col("prs") / F.col("pushes")
)

pr_ratio_dist = repo_metrics.groupBy("era").agg(
    F.count("repo_name").alias("repo_count"),
    F.avg("pr_push_ratio").alias("avg_pr_push_ratio"),
    F.expr("percentile_approx(pr_push_ratio, 0.25)").alias("p25_pr_push_ratio"),
    F.expr("percentile_approx(pr_push_ratio, 0.5)").alias("median_pr_push_ratio"),
    F.expr("percentile_approx(pr_push_ratio, 0.75)").alias("p75_pr_push_ratio"),
    F.avg("contributors").alias("avg_contributors_per_repo"),
).orderBy("era")

(pr_ratio_dist.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/pr_push_ratio"))

print("pr_push_ratio done")

# ── 4. Active repos per month ─────────────────────────────────────────────────

monthly = gh.withColumn("month", F.date_format("event_date", "yyyy-MM"))

active_monthly = monthly.groupBy("era", "month").agg(
    F.countDistinct("repo_name").alias("active_repos"),
    F.countDistinct("actor_login").alias("active_actors"),
    F.count("*").alias("total_events"),
).orderBy("era", "month")

(active_monthly.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/active_repos_monthly"))

print("active_repos_monthly done")

gh.unpersist()
print(f"Done. Output: {OUT}")
spark.stop()
