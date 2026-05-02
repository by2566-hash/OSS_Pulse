"""
Job 09: repo_era_deep_dive
---------------------------
Deep-dive comparison of specific repos across two eras:
  - 2022 Q1: pre-ChatGPT baseline (2022-01-01 ~ 2022-03-31)
  - 2025 Q1: post-coding-agent era (2025-01-01 ~ 2025-03-31)

Target repos (AI vs non-AI, all existed before 2022):
  AI:     pytorch/pytorch, huggingface/transformers, apache/airflow
  Non-AI: django/django, kubernetes/kubernetes, facebook/react

Metrics:
  1. pr_merge_time    -- median/p75/p90 hours from PR open to merge, per repo per era
  2. daily_pr_count   -- daily PR open count per repo per era
  3. daily_commits    -- daily push_distinct_size sum per repo per era
  4. contributor_flow -- monthly new vs returning contributors per repo per era

Input:
  2022 Q1: /user/jl17797_nyu_edu/oss_pulse/source/gharchive_2022q1/   (raw JSON.gz)
  2025 Q1: /user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/     (cleaned Parquet)

Output:
  /user/jl17797_nyu_edu/oss_pulse/analytics/repo_era_deep_dive/
    pr_merge_time.csv
    daily_pr_count.csv
    daily_commits.csv
    contributor_flow.csv

Run:
  spark-submit /tmp/09_repo_era_deep_dive.py > /tmp/09_out.txt 2>&1; echo EXIT:$?
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, LongType, StringType,
    StructField, StructType,
)

spark = SparkSession.builder.appName("09_RepoEraDeepdive").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

OUT = "/user/jl17797_nyu_edu/oss_pulse/analytics/repo_era_deep_dive"

TARGET_REPOS = [
    "pytorch/pytorch",
    "huggingface/transformers",
    "apache/airflow",
    "django/django",
    "kubernetes/kubernetes",
    "facebook/react",
]

# ── Inline schema for reading 2022 Q1 raw JSON.gz ────────────────────────────

commit_author_schema = StructType([
    StructField("email", StringType(), True),
    StructField("name",  StringType(), True),
])
commit_schema = StructType([
    StructField("sha",      StringType(),         True),
    StructField("author",   commit_author_schema, True),
    StructField("message",  StringType(),         True),
    StructField("distinct", BooleanType(),        True),
    StructField("url",      StringType(),         True),
])
user_schema = StructType([
    StructField("login", StringType(), True),
    StructField("id",    LongType(),   True),
])
issue_schema = StructType([
    StructField("id",     LongType(),   True),
    StructField("number", LongType(),   True),
    StructField("title",  StringType(), True),
    StructField("state",  StringType(), True),
    StructField("user",   user_schema,  True),
])
pull_request_schema = StructType([
    StructField("id",     LongType(),    True),
    StructField("number", LongType(),    True),
    StructField("state",  StringType(),  True),
    StructField("merged", BooleanType(), True),
    StructField("title",  StringType(),  True),
    StructField("user",   user_schema,   True),
])
forkee_owner_schema = StructType([
    StructField("login", StringType(), True),
    StructField("id",    LongType(),   True),
])
forkee_schema = StructType([
    StructField("id",        LongType(),          True),
    StructField("full_name", StringType(),        True),
    StructField("owner",     forkee_owner_schema, True),
])
payload_schema = StructType([
    StructField("action",        StringType(),             True),
    StructField("ref",           StringType(),             True),
    StructField("ref_type",      StringType(),             True),
    StructField("master_branch", StringType(),             True),
    StructField("description",   StringType(),             True),
    StructField("number",        LongType(),               True),
    StructField("size",          LongType(),               True),
    StructField("distinct_size", LongType(),               True),
    StructField("push_id",       LongType(),               True),
    StructField("commits",       ArrayType(commit_schema), True),
    StructField("issue",         issue_schema,             True),
    StructField("pull_request",  pull_request_schema,      True),
    StructField("forkee",        forkee_schema,            True),
])
actor_schema = StructType([
    StructField("id",            LongType(),   True),
    StructField("login",         StringType(), True),
    StructField("display_login", StringType(), True),
    StructField("gravatar_id",   StringType(), True),
    StructField("url",           StringType(), True),
    StructField("avatar_url",    StringType(), True),
])
repo_schema = StructType([
    StructField("id",   LongType(),   True),
    StructField("name", StringType(), True),
    StructField("url",  StringType(), True),
])
org_schema = StructType([
    StructField("id",          LongType(),   True),
    StructField("login",       StringType(), True),
    StructField("gravatar_id", StringType(), True),
    StructField("url",         StringType(), True),
    StructField("avatar_url",  StringType(), True),
])
gharchive_schema = StructType([
    StructField("id",         StringType(),   True),
    StructField("type",       StringType(),   True),
    StructField("actor",      actor_schema,   True),
    StructField("repo",       repo_schema,    True),
    StructField("payload",    payload_schema, True),
    StructField("public",     BooleanType(),  True),
    StructField("created_at", StringType(),   True),
    StructField("org",        org_schema,     True),
])

# ── Load and flatten 2022 Q1 raw JSON ────────────────────────────────────────

raw_2022 = spark.read.schema(gharchive_schema).json(
    "/user/jl17797_nyu_edu/oss_pulse/source/gharchive_2022q1/"
)

events_2022 = (
    raw_2022
    .select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.to_timestamp("created_at").alias("event_ts"),
        F.to_date(F.to_timestamp("created_at")).alias("event_date"),
        F.lower(F.col("actor.login")).alias("actor_login"),
        F.lower(F.col("repo.name")).alias("repo_name"),
        F.col("payload.action").alias("payload_action"),
        F.col("payload.pull_request.number").alias("pr_number"),
        F.col("payload.pull_request.merged").alias("pr_merged"),
        F.col("payload.distinct_size").alias("push_distinct_size"),
    )
    .filter(F.col("event_ts").isNotNull())
    .filter(F.lower(F.col("repo.name")).isin(TARGET_REPOS))
    .withColumn("era", F.lit("2022-Q1"))
)

# ── Load 2025 Q1 cleaned Parquet ──────────────────────────────────────────────

events_2025 = (
    spark.read.parquet("/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/")
    .filter(F.col("event_date").between("2025-01-01", "2025-03-31"))
    .filter(F.col("repo_name").isin(TARGET_REPOS))
    .select(
        "event_id", "event_type", "event_ts", "event_date",
        "actor_login", "repo_name", "payload_action",
        "pr_number", "pr_merged", "push_distinct_size",
    )
    .withColumn("era", F.lit("2025-Q1"))
)

events = events_2022.unionByName(events_2025).cache()

# ── 1. PR Merge Time ──────────────────────────────────────────────────────────
# Join PR opened events with PR merged events on (era, repo_name, pr_number)
# Calculate hours between open and merge

pr_opened = (
    events
    .filter(
        (F.col("event_type") == "PullRequestEvent") &
        (F.col("payload_action") == "opened")
    )
    .select("era", "repo_name", "pr_number",
            F.col("event_ts").alias("opened_ts"))
)

pr_merged = (
    events
    .filter(
        (F.col("event_type") == "PullRequestEvent") &
        (F.col("payload_action") == "closed") &
        (F.col("pr_merged") == True)
    )
    .select("era", "repo_name", "pr_number",
            F.col("event_ts").alias("merged_ts"))
)

pr_times = (
    pr_opened.join(pr_merged, on=["era", "repo_name", "pr_number"], how="inner")
    .withColumn("merge_hours",
        (F.unix_timestamp("merged_ts") - F.unix_timestamp("opened_ts")) / 3600)
    .filter(F.col("merge_hours") >= 0)
    .filter(F.col("merge_hours") <= 8760)  # exclude PRs open >1 year
)

pr_merge_time = (
    pr_times.groupBy("era", "repo_name").agg(
        F.count("pr_number").alias("merged_pr_count"),
        F.avg("merge_hours").alias("avg_merge_hours"),
        F.expr("percentile_approx(merge_hours, 0.5)").alias("median_merge_hours"),
        F.expr("percentile_approx(merge_hours, 0.75)").alias("p75_merge_hours"),
        F.expr("percentile_approx(merge_hours, 0.90)").alias("p90_merge_hours"),
    )
    .orderBy("repo_name", "era")
)

(pr_merge_time.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/pr_merge_time"))

print("Done: pr_merge_time")

# ── 2. Daily PR Count ─────────────────────────────────────────────────────────

daily_pr = (
    events
    .filter(
        (F.col("event_type") == "PullRequestEvent") &
        (F.col("payload_action") == "opened")
    )
    .groupBy("era", "repo_name", "event_date").agg(
        F.count("*").alias("pr_opened")
    )
    .orderBy("repo_name", "era", "event_date")
)

# Aggregate to per-era stats (median daily PRs, growth proxy)
daily_pr_summary = (
    daily_pr.groupBy("era", "repo_name").agg(
        F.sum("pr_opened").alias("total_prs"),
        F.avg("pr_opened").alias("avg_daily_prs"),
        F.expr("percentile_approx(pr_opened, 0.5)").alias("median_daily_prs"),
        F.max("pr_opened").alias("peak_daily_prs"),
    )
    .orderBy("repo_name", "era")
)

(daily_pr_summary.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/daily_pr_count"))

# Also write raw daily for time-series chart
(daily_pr.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/daily_pr_timeseries"))

print("Done: daily_pr_count")

# ── 3. Daily Commits ──────────────────────────────────────────────────────────

daily_commits = (
    events
    .filter(F.col("event_type") == "PushEvent")
    .filter(F.col("push_distinct_size").isNotNull())
    .groupBy("era", "repo_name", "event_date").agg(
        F.sum("push_distinct_size").alias("daily_commits"),
        F.count("*").alias("daily_pushes"),
        F.avg("push_distinct_size").alias("avg_commits_per_push"),
    )
    .orderBy("repo_name", "era", "event_date")
)

daily_commits_summary = (
    daily_commits.groupBy("era", "repo_name").agg(
        F.sum("daily_commits").alias("total_commits"),
        F.avg("daily_commits").alias("avg_daily_commits"),
        F.expr("percentile_approx(daily_commits, 0.5)").alias("median_daily_commits"),
        F.avg("avg_commits_per_push").alias("avg_commits_per_push"),
    )
    .orderBy("repo_name", "era")
)

(daily_commits_summary.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/daily_commits"))

(daily_commits.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/daily_commits_timeseries"))

print("Done: daily_commits")

# ── 4. Contributor Flow ───────────────────────────────────────────────────────
# Monthly new vs returning contributors per repo per era

all_actors = (
    events
    .filter(F.col("actor_login").isNotNull())
    .withColumn("month", F.date_format("event_date", "yyyy-MM"))
    .select("era", "repo_name", "month", "actor_login")
    .distinct()
)

# First appearance of each actor in each repo+era
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
    .groupBy("era", "repo_name", "month", "contributor_type")
    .agg(F.countDistinct("actor_login").alias("contributors"))
    .orderBy("repo_name", "era", "month", "contributor_type")
)

(contributor_flow.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(f"{OUT}/contributor_flow"))

print("Done: contributor_flow")
print(f"\nAll outputs at: {OUT}")
spark.stop()
