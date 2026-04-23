from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampNTZType, TimestampType

from utils.paths import EDA_DIR, ensure_parent_dir
from utils.spark_session import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EDA on cleaned Hugging Face models dataset.")
    parser.add_argument("--input", required=True, help="Path to cleaned HF models Parquet data.")
    parser.add_argument(
        "--input-format",
        choices=["json", "parquet"],
        default="parquet",
    )
    parser.add_argument("--output-dir", default=str(EDA_DIR), help="Output directory for EDA CSVs.")
    return parser.parse_args()


def read_input(spark, path: str, input_format: str) -> DataFrame:
    if input_format == "json":
        return spark.read.json(path)
    return spark.read.parquet(path)


def write_single_csv(rows: list[tuple[str, object]], output_path: str | Path) -> None:
    output_file = ensure_parent_dir(output_path)
    with output_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        writer.writerows(rows)


def write_df_as_local_csv(df: DataFrame, output_path: str | Path) -> None:
    formatted_columns = []
    for field in df.schema.fields:
        column = F.col(field.name)
        if isinstance(field.dataType, TimestampType | TimestampNTZType):
            column = F.date_format(column, "yyyy-MM-dd HH:mm:ss").alias(field.name)
        formatted_columns.append(column)

    rows = df.select(*formatted_columns).collect()
    output_file = ensure_parent_dir(output_path)
    with output_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(df.columns)
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    out = Path(args.output_dir)
    spark = create_spark_session("HFModelsEDA")

    df = read_input(spark, args.input, args.input_format)
    df.cache()
    total = df.count()

    summary_rows: list[tuple[str, object]] = [("total_records", total)]

    # ── 1. Downloads distribution ────────────────────────────
    print("=== 1. Downloads Distribution ===")

    downloads_buckets = df.select(
        F.when(F.col("downloads") == 0, "0")
        .when(F.col("downloads").between(1, 100), "1-100")
        .when(F.col("downloads").between(101, 10_000), "101-10K")
        .when(F.col("downloads").between(10_001, 1_000_000), "10K-1M")
        .otherwise("1M+")
        .alias("bucket")
    ).groupBy("bucket").count().orderBy("bucket")
    write_df_as_local_csv(downloads_buckets, out / "hf_downloads_distribution.csv")

    percentiles = df.select(
        F.expr("percentile_approx(downloads, 0.25)").alias("p25"),
        F.expr("percentile_approx(downloads, 0.50)").alias("p50"),
        F.expr("percentile_approx(downloads, 0.75)").alias("p75"),
        F.expr("percentile_approx(downloads, 0.90)").alias("p90"),
        F.expr("percentile_approx(downloads, 0.95)").alias("p95"),
        F.expr("percentile_approx(downloads, 0.99)").alias("p99"),
    ).first()
    for p in ["p25", "p50", "p75", "p90", "p95", "p99"]:
        summary_rows.append((f"downloads_{p}", percentiles[p]))

    # ── 2. Downloads vs Likes correlation ────────────────────
    print("=== 2. Downloads vs Likes Correlation ===")

    corr_val = df.stat.corr("downloads", "likes")
    summary_rows.append(("downloads_likes_correlation", f"{corr_val:.4f}"))

    # ── 3. Pipeline tag stats ────────────────────────────────
    print("=== 3. Pipeline Tag Stats ===")

    pipeline_stats = (
        df.groupBy("pipeline_tag")
        .agg(
            F.count("*").alias("model_count"),
            F.sum("downloads").alias("total_downloads"),
            F.round(F.avg("downloads"), 2).alias("avg_downloads"),
            F.round(F.avg("likes"), 2).alias("avg_likes"),
        )
        .orderBy(F.desc("total_downloads"))
    )
    write_df_as_local_csv(pipeline_stats, out / "hf_pipeline_tag_stats.csv")

    # ── 4. Library stats ─────────────────────────────────────
    print("=== 4. Library Stats ===")

    library_stats = (
        df.groupBy("library_name")
        .agg(
            F.count("*").alias("model_count"),
            F.sum("downloads").alias("total_downloads"),
            F.round(F.avg("downloads"), 2).alias("avg_downloads"),
            F.round(F.avg("likes"), 2).alias("avg_likes"),
        )
        .orderBy(F.desc("total_downloads"))
    )
    write_df_as_local_csv(library_stats, out / "hf_library_stats.csv")

    # ── 5. Monthly creation trend ────────────────────────────
    print("=== 5. Monthly Creation Trend ===")

    monthly_trend = (
        df.filter(F.col("created_ts").isNotNull())
        .withColumn("month", F.date_trunc("month", "created_ts"))
        .groupBy("month")
        .agg(
            F.count("*").alias("new_models"),
            F.sum("downloads").alias("total_downloads"),
        )
        .orderBy("month")
    )
    write_df_as_local_csv(monthly_trend, out / "hf_monthly_creation_trend.csv")

    # ── 6. License stats ─────────────────────────────────────
    print("=== 6. License Stats ===")

    license_stats = (
        df.groupBy("license")
        .agg(
            F.count("*").alias("model_count"),
            F.sum("downloads").alias("total_downloads"),
            F.round(F.avg("downloads"), 2).alias("avg_downloads"),
        )
        .orderBy(F.desc("model_count"))
    )
    write_df_as_local_csv(license_stats, out / "hf_license_stats.csv")

    # ── 7. Parameter count distribution ──────────────────────
    print("=== 7. Parameter Count Distribution ===")

    param_buckets = df.select(
        F.when(F.col("parameter_count").isNull(), "unknown")
        .when(F.col("parameter_count") < 1_000_000_000, "<1B")
        .when(F.col("parameter_count") < 7_000_000_000, "1-7B")
        .when(F.col("parameter_count") < 13_000_000_000, "7-13B")
        .when(F.col("parameter_count") < 70_000_000_000, "13-70B")
        .otherwise("70B+")
        .alias("param_bucket")
    ).groupBy("param_bucket").count().orderBy("param_bucket")
    write_df_as_local_csv(param_buckets, out / "hf_parameter_distribution.csv")

    # ── 8. Owner concentration ───────────────────────────────
    print("=== 8. Owner Concentration ===")

    top_owners = (
        df.groupBy("owner")
        .agg(
            F.count("*").alias("model_count"),
            F.sum("downloads").alias("total_downloads"),
            F.sum("likes").alias("total_likes"),
        )
        .orderBy(F.desc("total_downloads"))
        .limit(30)
    )
    write_df_as_local_csv(top_owners, out / "hf_owner_concentration.csv")

    # owner model count distribution
    owner_model_counts = (
        df.groupBy("owner").count()
        .select(
            F.when(F.col("count") == 1, "1")
            .when(F.col("count").between(2, 5), "2-5")
            .when(F.col("count").between(6, 20), "6-20")
            .when(F.col("count").between(21, 100), "21-100")
            .otherwise("100+")
            .alias("models_per_owner")
        )
        .groupBy("models_per_owner").count().alias("owner_count")
        .orderBy("models_per_owner")
    )
    write_df_as_local_csv(owner_model_counts, out / "hf_owner_model_count_distribution.csv")

    # ── 9. Model staleness ───────────────────────────────────
    print("=== 9. Model Staleness ===")

    staleness = df.select(
        F.when(F.datediff(F.current_date(), F.col("last_modified_ts")) <= 30, "<30 days")
        .when(F.datediff(F.current_date(), F.col("last_modified_ts")) <= 90, "30-90 days")
        .when(F.datediff(F.current_date(), F.col("last_modified_ts")) <= 365, "90-365 days")
        .otherwise(">1 year")
        .alias("staleness")
    ).groupBy("staleness").count().orderBy("staleness")
    write_df_as_local_csv(staleness, out / "hf_staleness_distribution.csv")

    # ── 10. Gated vs Non-Gated ───────────────────────────────
    print("=== 10. Gated vs Non-Gated ===")

    gated_stats = (
        df.withColumn(
            "is_gated",
            F.when(F.col("gated").isNull() | (F.col("gated") == "False"), "non-gated")
            .otherwise("gated")
        )
        .groupBy("is_gated")
        .agg(
            F.count("*").alias("model_count"),
            F.round(F.avg("downloads"), 2).alias("avg_downloads"),
            F.round(F.avg("likes"), 2).alias("avg_likes"),
        )
    )
    write_df_as_local_csv(gated_stats, out / "hf_gated_stats.csv")

    # ── Write summary ────────────────────────────────────────
    write_single_csv(summary_rows, out / "hf_eda_summary.csv")

    df.unpersist()
    spark.stop()

    print(f"\nEDA complete. Output directory: {out}")


if __name__ == "__main__":
    main()
