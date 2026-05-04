"""Copy 2025 Q1 and 2026 Q1 to own HDFS using Spark (parallel, fast)."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("CopyQ1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("[INFO] Copying 2025 Q1...")
spark.read.parquet("/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025") \
    .filter(F.col("event_date").between("2025-01-01", "2025-03-31")) \
    .write.mode("overwrite").partitionBy("event_date") \
    .parquet("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2025q1")
print("[OK] 2025 Q1 done")

print("[INFO] Copying 2026 Q1...")
spark.read.parquet("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement") \
    .filter(F.col("event_date").between("2026-01-01", "2026-03-31")) \
    .write.mode("overwrite").partitionBy("event_date") \
    .parquet("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1")
print("[OK] 2026 Q1 done")

spark.stop()
