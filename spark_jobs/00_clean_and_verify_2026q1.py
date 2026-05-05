"""
Clean raw 2026 Q1 data and compare with existing cleaned version.
Run after download completes:
  spark-submit --driver-memory 8g --executor-memory 12g --num-executors 4 --executor-cores 4 \
    /tmp/00_clean_and_verify_2026q1.py > /tmp/verify_2026q1.log 2>&1
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Clean_Verify_2026Q1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# ── Schema (same as other Q1 clean scripts) ──────────────────────────────────

commit_schema = ArrayType(StructType([
    StructField("sha", StringType(), True),
    StructField("message", StringType(), True),
]))
issue_schema = StructType([
    StructField("number", LongType(), True),
    StructField("state", StringType(), True),
])
pull_request_schema = StructType([
    StructField("number", LongType(), True),
    StructField("state", StringType(), True),
    StructField("merged", BooleanType(), True),
])
forkee_schema = StructType([
    StructField("id", LongType(), True),
    StructField("full_name", StringType(), True),
    StructField("owner", StructType([
        StructField("login", StringType(), True),
        StructField("id", LongType(), True),
    ]), True),
])
payload_schema = StructType([
    StructField("action", StringType(), True),
    StructField("ref", StringType(), True),
    StructField("ref_type", StringType(), True),
    StructField("master_branch", StringType(), True),
    StructField("description", StringType(), True),
    StructField("number", LongType(), True),
    StructField("size", LongType(), True),
    StructField("distinct_size", LongType(), True),
    StructField("push_id", LongType(), True),
    StructField("commits", commit_schema, True),
    StructField("issue", issue_schema, True),
    StructField("pull_request", pull_request_schema, True),
    StructField("forkee", forkee_schema, True),
])
actor_schema = StructType([
    StructField("id", LongType(), True),
    StructField("login", StringType(), True),
    StructField("display_login", StringType(), True),
    StructField("gravatar_id", StringType(), True),
])
org_schema = StructType([
    StructField("id", LongType(), True),
    StructField("login", StringType(), True),
])
repo_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
])
schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", actor_schema, True),
    StructField("repo", repo_schema, True),
    StructField("payload", payload_schema, True),
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("org", org_schema, True),
])

# ── Step 1: Clean raw data ───────────────────────────────────────────────────

RAW = "/user/jl17797_nyu_edu/oss_pulse/source/gharchive_2026q1_raw"
OUT = "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1_verify"

print("[INFO] Reading raw 2026 Q1 data...")
raw = spark.read.schema(schema).json(f"{RAW}/*.json.gz")

KEEP_TYPES = ["PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent", "ForkEvent"]

cleaned = (
    raw.filter(F.col("type").isin(KEEP_TYPES))
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

print("[INFO] Writing cleaned data...")
cleaned.write.mode("overwrite").partitionBy("event_date").parquet(OUT)
print(f"[OK] Cleaned data written to {OUT}")

# ── Step 2: Compare with existing cleaned ─────────────────────────────────────

EXISTING = "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1"

print("\n[INFO] Comparing new vs existing cleaned data...")

new = spark.read.parquet(OUT)
old = spark.read.parquet(EXISTING)

new_count = new.count()
old_count = old.count()
print(f"  New cleaned:      {new_count:,}")
print(f"  Existing cleaned: {old_count:,}")
print(f"  Difference:       {new_count - old_count:,} ({(new_count - old_count) / old_count * 100:.2f}%)")

# Compare by event_type
print("\n  Event type comparison:")
new_types = new.groupBy("event_type").count().withColumnRenamed("count", "new_count")
old_types = old.groupBy("event_type").count().withColumnRenamed("count", "old_count")
compare_types = new_types.join(old_types, on="event_type", how="outer") \
    .withColumn("diff", F.col("new_count") - F.col("old_count")) \
    .orderBy("event_type")
compare_types.show(10, False)

# Compare by date
print("  Daily event count comparison (sample):")
new_daily = new.groupBy("event_date").count().withColumnRenamed("count", "new_count")
old_daily = old.groupBy("event_date").count().withColumnRenamed("count", "old_count")
compare_daily = new_daily.join(old_daily, on="event_date", how="outer") \
    .withColumn("diff", F.col("new_count") - F.col("old_count")) \
    .withColumn("diff_pct", F.round((F.col("new_count") - F.col("old_count")) / F.col("old_count") * 100, 2)) \
    .orderBy("event_date")
compare_daily.show(100, False)

# Check null patterns
print("  Null pattern comparison:")
KEY_COLS = ["event_type", "event_date", "actor_login", "repo_name",
            "pr_merged", "pr_number", "payload_action", "push_distinct_size"]
for col in KEY_COLS:
    if col in new.columns and col in old.columns:
        new_null = new.filter(F.col(col).isNull()).count()
        old_null = old.filter(F.col(col).isNull()).count()
        print(f"    {col:25s}: new={new_null:>12,} old={old_null:>12,}  diff={new_null-old_null:+,}")

# Schema comparison
print("\n  Schema comparison:")
new_cols = set(new.columns)
old_cols = set(old.columns)
if new_cols == old_cols:
    print("    Schemas match")
else:
    print(f"    Only in new: {new_cols - old_cols}")
    print(f"    Only in old: {old_cols - new_cols}")

print(f"\n[DONE] Verification complete. New cleaned at {OUT}, existing at {EXISTING}")
spark.stop()
