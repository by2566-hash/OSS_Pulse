"""Job 10 merge-only: read 15 intermediate CSVs and write 3 final outputs."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("10_MergeOnly").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

OUT = "/user/jl17797_nyu_edu/oss_pulse/analytics/dev_rhythm"
INTER = f"{OUT}/intermediate"
ERAS = ["2022_Q1", "2023_Q1", "2024_Q1", "2025_Q1", "2026_Q1"]

METRICS = {
    "weekend_summary": "weekday_weekend",
    "weekend_by_type": "weekday_weekend_by_type",
    "push_actor": "push_per_actor",
}

for prefix, output_name in METRICS.items():
    print(f"[INFO] Merging {output_name}...")
    dfs = []
    for era_tag in ERAS:
        path = f"{INTER}/{prefix}_{era_tag}"
        try:
            df = spark.read.option("header", True).csv(path)
            dfs.append(df)
            print(f"  loaded {era_tag}")
        except Exception as e:
            print(f"  [WARN] skip {era_tag}: {e}")

    if not dfs:
        print(f"  [ERROR] no data for {output_name}")
        continue

    merged = dfs[0]
    for d in dfs[1:]:
        merged = merged.unionByName(d, allowMissingColumns=True)

    merged = merged.orderBy("era")
    merged.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT}/{output_name}")
    print(f"[OK] {output_name} written")

print(f"\n[DONE] All outputs at {OUT}")
spark.stop()
