from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[2]").appName("CheckSchema").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=== 2025 Q1 (by2566 cleaned) ===")
df25 = spark.read.parquet("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2025q1/event_date=2025-01-15")
df25.filter(F.col("event_type")=="PullRequestEvent").groupBy("pr_merged").count().show()
df25.filter(F.col("event_type")=="PushEvent").select("push_distinct_size").show(5)

print("=== 2026 Q1 (supplement cleaned) ===")
df26 = spark.read.parquet("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1/event_date=2026-01-15")
df26.filter(F.col("event_type")=="PullRequestEvent").groupBy("pr_merged").count().show()
df26.filter(F.col("event_type")=="PushEvent").select("push_distinct_size").show(5)

print("=== 2026 raw JSON field check ===")
raw = spark.read.json("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement/event_date=2026-01-15/part-00000*.parquet")
print("Cannot read parquet as JSON, skipping raw check")

spark.stop()
