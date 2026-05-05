from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[2]").appName("Check2026Daily").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet("/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1")
daily = df.groupBy("event_date").agg(F.count("*").alias("events")).orderBy("event_date")
daily.show(100, False)
print(f"Total days: {daily.count()}")
print(f"Total events: {df.count()}")
spark.stop()
