"""
Job 10: dev_rhythm_analysis (per-era design)
---------------------------------------------
Analyze development rhythm changes across 5 Q1 eras (2022-2026)
to measure coding agent impact on OSS development patterns.

Output:
  /user/jl17797_nyu_edu/oss_pulse/analytics/dev_rhythm/
    weekday_weekend/    -- weekday vs weekend event counts & ratio per era
    push_per_actor/     -- per-actor push frequency distribution per era

Run:
  spark-submit /tmp/10_dev_rhythm.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("10_DevRhythm").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "24")

OUT = "/user/jl17797_nyu_edu/oss_pulse/analytics/dev_rhythm"

ERA_SOURCES = [
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2022q1", "2022-Q1"),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2023q1", "2023-Q1"),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2024q1", "2024-Q1"),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2025q1", "2025-Q1"),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1", "2026-Q1"),
]

weekend_rows = []
push_actor_rows = []

for path, era in ERA_SOURCES:
    print(f"\n[INFO] Processing {era} from {path}")

    try:
        df = spark.read.parquet(path)
    except Exception as e:
        print(f"[WARN] Could not load {era}: {e}")
        continue

    df = df.select("event_type", "event_date", "actor_login", "repo_name").cache()
    total = df.count()
    print(f"[INFO] {era}: {total:,} events loaded")

    # ── 1. Weekday vs Weekend ────────────────────────────────────────────
    # dayofweek: 1=Sunday, 7=Saturday in Spark
    daily = (
        df.withColumn("dow", F.dayofweek("event_date"))
        .withColumn("is_weekend", F.col("dow").isin(1, 7))
        .groupBy("is_weekend")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("actor_login").alias("actors"),
            F.countDistinct("repo_name").alias("repos"),
        )
    )

    # Also break down by event_type
    by_type = (
        df.withColumn("dow", F.dayofweek("event_date"))
        .withColumn("is_weekend", F.col("dow").isin(1, 7))
        .groupBy("is_weekend", "event_type")
        .agg(F.count("*").alias("events"))
    )

    weekend_summary = (
        daily.withColumn("era", F.lit(era))
        .select("era", "is_weekend", "events", "actors", "repos")
    )
    weekend_by_type = (
        by_type.withColumn("era", F.lit(era))
        .select("era", "is_weekend", "event_type", "events")
    )

    # Write intermediate
    era_tag = era.replace("-", "_")
    weekend_summary.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT}/intermediate/weekend_summary_{era_tag}")
    weekend_by_type.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT}/intermediate/weekend_by_type_{era_tag}")
    weekend_rows.append(("summary", weekend_summary))
    weekend_rows.append(("by_type", weekend_by_type))
    print(f"[OK] {era} weekday_weekend done")

    # ── 2. Push frequency per actor ──────────────────────────────────────
    push_actors = (
        df.filter(F.col("event_type") == "PushEvent")
        .groupBy("actor_login")
        .agg(
            F.count("*").alias("push_count"),
            F.countDistinct("event_date").alias("active_days"),
            F.countDistinct("repo_name").alias("repo_count"),
        )
        .withColumn("pushes_per_day", F.col("push_count") / F.col("active_days"))
    )

    push_dist = push_actors.agg(
        F.lit(era).alias("era"),
        F.count("*").alias("total_push_actors"),
        F.avg("push_count").alias("avg_pushes"),
        F.expr("percentile_approx(push_count, 0.5)").alias("median_pushes"),
        F.expr("percentile_approx(push_count, 0.75)").alias("p75_pushes"),
        F.expr("percentile_approx(push_count, 0.90)").alias("p90_pushes"),
        F.expr("percentile_approx(push_count, 0.99)").alias("p99_pushes"),
        F.avg("pushes_per_day").alias("avg_pushes_per_day"),
        F.expr("percentile_approx(pushes_per_day, 0.5)").alias("median_pushes_per_day"),
        F.expr("percentile_approx(pushes_per_day, 0.90)").alias("p90_pushes_per_day"),
        F.avg("active_days").alias("avg_active_days"),
        F.avg("repo_count").alias("avg_repos_per_actor"),
        # High-frequency pushers (potential bots/agents)
        F.sum(F.when(F.col("push_count") > 100, 1).otherwise(0)).alias("actors_gt_100_pushes"),
        F.sum(F.when(F.col("push_count") > 1000, 1).otherwise(0)).alias("actors_gt_1000_pushes"),
        F.sum(F.when(F.col("pushes_per_day") > 50, 1).otherwise(0)).alias("actors_gt_50_per_day"),
    )

    push_dist.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT}/intermediate/push_actor_{era_tag}")
    push_actor_rows.append(push_dist)
    print(f"[OK] {era} push_per_actor done")

    df.unpersist()
    print(f"[OK] {era} complete")

# ── Merge and write final CSVs ───────────────────────────────────────────────

print("\n[INFO] Merging results...")

def union_all(dfs):
    result = dfs[0]
    for d in dfs[1:]:
        result = result.unionByName(d, allowMissingColumns=True)
    return result

# Weekend summary
summaries = [df for label, df in weekend_rows if label == "summary"]
by_types = [df for label, df in weekend_rows if label == "by_type"]

if summaries:
    (union_all(summaries).orderBy("era", "is_weekend").coalesce(1)
     .write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/weekday_weekend"))
    print("[OK] weekday_weekend written")

if by_types:
    (union_all(by_types).orderBy("era", "is_weekend", "event_type").coalesce(1)
     .write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/weekday_weekend_by_type"))
    print("[OK] weekday_weekend_by_type written")

# Push per actor
if push_actor_rows:
    (union_all(push_actor_rows).orderBy("era").coalesce(1)
     .write.mode("overwrite").option("header", True)
     .csv(f"{OUT}/push_per_actor"))
    print("[OK] push_per_actor written")

print(f"\n[DONE] All outputs at {OUT}")
spark.stop()
