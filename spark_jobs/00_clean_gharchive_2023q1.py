"""
Job 00_clean_2023q1: Clean GH Archive 2022 Q1 data
----------------------------------------------------
Input:  /user/jl17797_nyu_edu/oss_pulse/source/gharchive_2023q1/
Output: /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2023q1/
        (same schema as by2566's cleaned/gharchive/2025/)

Purpose: Provide pre-coding-agent baseline for era comparison (Job 08).

Run:
  spark-submit /tmp/00_clean_gharchive_2023q1.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, LongType, StringType,
    StructField, StructType,
)

spark = SparkSession.builder.appName("00_CleanGHArchive2023Q1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ── Inline schema (mirrors gharchive_schema.py) ──────────────────────────────

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

CORE_EVENT_TYPES = [
    "WatchEvent",
    "ForkEvent",
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
]

# ── Read ──────────────────────────────────────────────────────────────────────

raw = spark.read.schema(gharchive_schema).json(
    "/user/jl17797_nyu_edu/oss_pulse/source/gharchive_2023q1/"
)

# ── Clean (same logic as 00_clean_gharchive_supplement.py) ───────────────────

cleaned = (
    raw.filter(F.col("type").isin(CORE_EVENT_TYPES))
    .select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.to_timestamp("created_at").alias("event_ts"),
        F.to_date(F.to_timestamp("created_at")).alias("event_date"),
        F.col("public").alias("is_public"),
        F.col("actor.id").alias("actor_id"),
        F.lower(F.col("actor.login")).alias("actor_login"),
        F.col("repo.id").alias("repo_id"),
        F.lower(F.col("repo.name")).alias("repo_name"),
        F.col("org.id").alias("org_id"),
        F.lower(F.col("org.login")).alias("org_login"),
        F.col("payload.action").alias("payload_action"),
        F.col("payload.number").alias("payload_number"),
        F.col("payload.issue.number").alias("issue_number"),
        F.col("payload.issue.state").alias("issue_state"),
        F.col("payload.pull_request.number").alias("pr_number"),
        F.col("payload.pull_request.state").alias("pr_state"),
        F.col("payload.pull_request.merged").alias("pr_merged"),
        F.col("payload.push_id").alias("push_id"),
        F.col("payload.size").alias("push_size"),
        F.col("payload.distinct_size").alias("push_distinct_size"),
        F.size(F.col("payload.commits")).alias("commit_count"),
    )
    .filter(F.col("event_ts").isNotNull())
    .filter(F.col("repo_name").isNotNull())
    .dropDuplicates(["event_id"])
)

# ── Write ─────────────────────────────────────────────────────────────────────

OUT = "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2023q1"
cleaned.write.mode("overwrite").partitionBy("event_date").parquet(OUT)

print(f"Done. Output: {OUT}")
spark.stop()
