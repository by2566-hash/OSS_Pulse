"""Quick data quality check across all 5 Q1 eras."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("DataQuality").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ERAS = [
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2022q1", "2022-Q1"),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2023q1", "2023-Q1"),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2024q1", "2024-Q1"),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2025q1", "2025-Q1"),
    ("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1", "2026-Q1"),
]

KEY_COLS = ["event_type", "event_date", "actor_login", "repo_name",
            "pr_merged", "pr_number", "payload_action", "push_distinct_size"]

for path, era in ERAS:
    print(f"\n{'='*60}")
    print(f"  {era}: {path}")
    print(f"{'='*60}")
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        print(f"  FAILED to read: {e}")
        continue

    total = df.count()
    days = df.select("event_date").distinct().count()
    print(f"  Total events: {total:,}")
    print(f"  Distinct days: {days}")
    print(f"  Avg events/day: {total/days:,.0f}" if days > 0 else "  No days")

    # Event type distribution
    print(f"\n  Event type distribution:")
    df.groupBy("event_type").count().orderBy(F.desc("count")).show(10, False)

    # Null check for key columns
    print(f"  Null counts (key columns):")
    available = set(df.columns)
    for col in KEY_COLS:
        if col in available:
            null_count = df.filter(F.col(col).isNull()).count()
            pct = null_count / total * 100 if total > 0 else 0
            flag = " *** ISSUE" if pct > 50 and col in ("pr_merged", "push_distinct_size") else ""
            print(f"    {col:25s}: {null_count:>12,} nulls ({pct:5.1f}%){flag}")
        else:
            print(f"    {col:25s}: COLUMN MISSING")

    # Date range
    date_range = df.agg(F.min("event_date"), F.max("event_date")).collect()[0]
    print(f"\n  Date range: {date_range[0]} to {date_range[1]}")

spark.stop()
