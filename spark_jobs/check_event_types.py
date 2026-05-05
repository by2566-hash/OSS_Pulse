from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[2]").appName("CheckEventTypes").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=== 2025 Q1 event types (by2566) ===")
df25 = spark.read.parquet("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2025q1/event_date=2025-01-15")
df25.groupBy("event_type").count().orderBy(F.desc("count")).show(20, False)
print(f"2025 total events (Jan 15): {df25.count()}")

print("=== 2026 Q1 event types (supplement) ===")
df26 = spark.read.parquet("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1/event_date=2026-01-15")
df26.groupBy("event_type").count().orderBy(F.desc("count")).show(20, False)
print(f"2026 total events (Jan 15): {df26.count()}")

spark.stop()
