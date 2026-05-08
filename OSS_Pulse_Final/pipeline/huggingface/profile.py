from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampNTZType, TimestampType

from pipeline.huggingface.schema import hf_models_schema
from utils.paths import PROFILING_DIR, SAMPLES_DIR, ensure_parent_dir
from utils.spark_session import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile a Hugging Face models dataset with Spark.")
    parser.add_argument("--input", required=True, help="Path to HF models JSONL or Parquet data.")
    parser.add_argument(
        "--input-format",
        choices=["json", "parquet"],
        default="parquet",
        help="Input format.",
    )
    parser.add_argument(
        "--summary-output",
        default=str(PROFILING_DIR / "hf_models_profile_summary.csv"),
        help="Path for the metric summary CSV.",
    )
    parser.add_argument(
        "--pipeline-tag-output",
        default=str(PROFILING_DIR / "hf_models_pipeline_tag_distribution.csv"),
        help="Path for pipeline_tag distribution.",
    )
    parser.add_argument(
        "--library-output",
        default=str(PROFILING_DIR / "hf_models_library_distribution.csv"),
        help="Path for library_name distribution.",
    )
    parser.add_argument(
        "--null-output",
        default=str(PROFILING_DIR / "hf_models_null_counts.csv"),
        help="Path for null count metrics.",
    )
    parser.add_argument(
        "--top-authors-output",
        default=str(PROFILING_DIR / "hf_models_top_authors.csv"),
        help="Path for the top authors by model count.",
    )
    parser.add_argument(
        "--top-downloads-output",
        default=str(PROFILING_DIR / "hf_models_top_downloads.csv"),
        help="Path for the top models by downloads.",
    )
    parser.add_argument(
        "--sample-output",
        default=str(SAMPLES_DIR / "raw_hf_models_profile_sample.csv"),
        help="Path for a small report-friendly sample CSV.",
    )
    parser.add_argument("--sample-rows", type=int, default=10, help="Number of rows to export as a sample.")
    return parser.parse_args()


def read_input(spark, path: str, input_format: str) -> DataFrame:
    if input_format == "json":
        return spark.read.schema(hf_models_schema).json(path)
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
    spark = create_spark_session("HFModelsProfiling")

    df = read_input(spark, args.input, args.input_format)

    # --- summary metrics ---
    total_rows = df.count()
    total_columns = len(df.columns)
    distinct_authors = df.select("author").distinct().count()
    distinct_pipeline_tags = df.filter(F.col("pipeline_tag").isNotNull()).select("pipeline_tag").distinct().count()
    distinct_libraries = df.filter(F.col("library_name").isNotNull()).select("library_name").distinct().count()

    downloads_stats = df.agg(
        F.min("downloads").alias("min_downloads"),
        F.max("downloads").alias("max_downloads"),
        F.mean("downloads").alias("avg_downloads"),
        F.expr("percentile_approx(downloads, 0.5)").alias("median_downloads"),
    ).first()

    likes_stats = df.agg(
        F.min("likes").alias("min_likes"),
        F.max("likes").alias("max_likes"),
        F.mean("likes").alias("avg_likes"),
    ).first()

    summary_rows = [
        ("total_rows", total_rows),
        ("total_columns", total_columns),
        ("distinct_authors", distinct_authors),
        ("distinct_pipeline_tags", distinct_pipeline_tags),
        ("distinct_library_names", distinct_libraries),
        ("min_downloads", downloads_stats["min_downloads"]),
        ("max_downloads", downloads_stats["max_downloads"]),
        ("avg_downloads", f"{downloads_stats['avg_downloads']:.2f}" if downloads_stats["avg_downloads"] else None),
        ("median_downloads", downloads_stats["median_downloads"]),
        ("min_likes", likes_stats["min_likes"]),
        ("max_likes", likes_stats["max_likes"]),
        ("avg_likes", f"{likes_stats['avg_likes']:.2f}" if likes_stats["avg_likes"] else None),
    ]
    write_single_csv(summary_rows, args.summary_output)

    # --- pipeline_tag distribution ---
    pipeline_tag_df = (
        df.select(F.coalesce(F.col("pipeline_tag"), F.lit("(null)")).alias("pipeline_tag"))
        .groupBy("pipeline_tag")
        .count()
        .orderBy(F.desc("count"))
    )
    write_df_as_local_csv(pipeline_tag_df, args.pipeline_tag_output)

    # --- library_name distribution ---
    library_df = (
        df.select(F.coalesce(F.col("library_name"), F.lit("(null)")).alias("library_name"))
        .groupBy("library_name")
        .count()
        .orderBy(F.desc("count"))
    )
    write_df_as_local_csv(library_df, args.library_output)

    # --- null counts ---
    null_exprs = []
    seen_aliases = set()
    # top-level columns
    for col_name in ["modelId", "model_id", "author", "pipeline_tag", "library_name",
                     "downloads", "likes", "lastModified", "last_modified_ts", "created_at", "created_ts"]:
        if col_name in df.columns:
            null_exprs.append(
                F.sum(F.col(col_name).isNull().cast("int")).alias(f"{col_name}_nulls")
            )
            seen_aliases.add(f"{col_name}_nulls")
    # license and datasets: try nested first, then flattened
    for candidates, alias in [
        (["card_data.license", "license"], "license_nulls"),
        (["card_data.datasets", "training_datasets"], "datasets_nulls"),
    ]:
        if alias not in seen_aliases:
            for col_name in candidates:
                if col_name in df.columns:
                    null_exprs.append(
                        F.sum(F.col(col_name).isNull().cast("int")).alias(alias)
                    )
                    seen_aliases.add(alias)
                    break

    null_counts = df.agg(*null_exprs).collect()[0].asDict()
    write_single_csv(list(null_counts.items()), args.null_output)

    # --- top 20 authors by model count ---
    top_authors_df = (
        df.filter(F.col("author").isNotNull())
        .groupBy("author")
        .count()
        .orderBy(F.desc("count"))
        .limit(20)
    )
    write_df_as_local_csv(top_authors_df, args.top_authors_output)

    # --- top 20 models by downloads ---
    model_id_col = "modelId" if "modelId" in df.columns else "model_id"
    top_downloads_cols = [c for c in [model_id_col, "author", "pipeline_tag", "library_name", "downloads", "likes"] if c in df.columns]
    top_downloads_df = (
        df.filter(F.col("downloads").isNotNull())
        .select(*top_downloads_cols)
        .orderBy(F.desc("downloads"))
        .limit(20)
    )
    write_df_as_local_csv(top_downloads_df, args.top_downloads_output)

    # --- sample rows ---
    ts_col = "lastModified" if "lastModified" in df.columns else "last_modified_ts"
    sample_cols = [c for c in [model_id_col, "author", "pipeline_tag", "library_name", "downloads", "likes", ts_col] if c in df.columns]
    sample_df = df.select(*sample_cols).limit(args.sample_rows)
    write_df_as_local_csv(sample_df, args.sample_output)

    spark.stop()


if __name__ == "__main__":
    main()
