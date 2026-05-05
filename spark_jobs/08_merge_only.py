"""
Job 08 merge-only: read 20 intermediate CSVs and write 4 final outputs.
Lightweight job — no heavy computation, just CSV read + union + write.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("08_MergeOnly").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

OUT = "/user/jl17797_nyu_edu/oss_pulse/analytics/era_comparison"
INTER = f"{OUT}/intermediate"
ERAS = ["2022_Q1", "2023_Q1", "2024_Q1", "2025_Q1", "2026_Q1"]

METRICS = {
    "summary":   "summary_metrics",
    "push_dist": "push_size_distribution",
    "pr_ratio":  "pr_push_ratio",
    "monthly":   "active_repos_monthly",
}

for prefix, output_name in METRICS.items():
    print(f"\n[INFO] Merging {output_name}...")
    dfs = []
    for era_tag in ERAS:
        path = f"{INTER}/{prefix}_{era_tag}"
        try:
            df = spark.read.option("header", True).csv(path)
            dfs.append(df)
            print(f"  loaded {era_tag} ({df.count()} rows)")
        except Exception as e:
            print(f"  [WARN] skip {era_tag}: {e}")

    if not dfs:
        print(f"  [ERROR] no data for {output_name}")
        continue

    merged = dfs[0]
    for d in dfs[1:]:
        merged = merged.unionByName(d, allowMissingColumns=True)

    order_col = "era" if "month" not in merged.columns else "era"
    if "month" in merged.columns:
        merged = merged.orderBy("era", "month")
    else:
        merged = merged.orderBy("era")

    merged.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT}/{output_name}")
    print(f"[OK] {output_name} written")

print(f"\n[DONE] All 4 final outputs written to {OUT}")
spark.stop()
