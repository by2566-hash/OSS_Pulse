from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampNTZType, TimestampType

from pipeline.gharchive.schema import CORE_EVENT_TYPES, gharchive_schema
from utils.paths import GHARCHIVE_CLEANED_DIR, SAMPLES_DIR, ensure_parent_dir
from utils.spark_session import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean GH Archive data into an analysis-ready Parquet table.")
    parser.add_argument("--input", required=True, help="Path to raw GH Archive JSON.gz or Parquet data.")
    parser.add_argument(
        "--input-format",
        choices=["json", "parquet"],
        default="parquet",
        help="Input format. Use json for direct cleaning of original GH Archive files.",
    )
    parser.add_argument(
        "--output",
        default=str(GHARCHIVE_CLEANED_DIR),
        help="Output directory for cleaned GH Archive Parquet data.",
    )
    parser.add_argument(
        "--seed-repo-file",
        default=None,
        help="Optional CSV with a repo_name column for filtering to a curated repository list.",
    )
    parser.add_argument(
        "--sample-output",
        default=str(SAMPLES_DIR / "clean_gharchive_sample.csv"),
        help="CSV path for a small cleaned-data snippet used in the report.",
    )
    parser.add_argument("--sample-rows", type=int, default=10, help="Number of rows to export as a sample.")
    parser.add_argument(
        "--write-mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Spark write mode for the cleaned Parquet output.",
    )
    return parser.parse_args()


def read_input(spark, path: str, input_format: str) -> DataFrame:
    if input_format == "json":
        return spark.read.schema(gharchive_schema).json(path)
    return spark.read.parquet(path)


def filter_seed_repos(df: DataFrame, spark, seed_repo_file: str | None) -> DataFrame:
    if not seed_repo_file:
        return df

    seed_df = spark.read.option("header", True).csv(seed_repo_file)
    if "repo_name" not in seed_df.columns:
        raise ValueError("Seed repo CSV must contain a repo_name column.")

    return df.join(
        seed_df.select(F.lower(F.col("repo_name")).alias("seed_repo_name")).distinct(),
        df.repo_name == F.col("seed_repo_name"),
        "inner",
    ).drop("seed_repo_name")


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
    spark = create_spark_session("GHArchiveCleaning")

    raw_df = read_input(spark, args.input, args.input_format)

    cleaned_df = (
        raw_df.filter(F.col("type").isin(CORE_EVENT_TYPES))
        .select(
            F.col("id").alias("event_id"),
            F.col("type").alias("event_type"),
            F.to_timestamp("created_at").alias("event_ts"),
            F.to_date(F.to_timestamp("created_at")).alias("event_date"),
            F.col("public").alias("is_public"),
            F.col("actor.id").alias("actor_id"),
            F.lower(F.col("actor.login")).alias("actor_login"),
            F.col("repo.id").alias("repo_id"),
            F.lower(F.col("repo.name")).alias("repo_name"),
            F.col("org.id").alias("org_id"),
            F.lower(F.col("org.login")).alias("org_login"),
            F.col("payload.action").alias("payload_action"),
            F.col("payload.number").alias("payload_number"),
            F.col("payload.issue.number").alias("issue_number"),
            F.col("payload.issue.state").alias("issue_state"),
            F.col("payload.pull_request.number").alias("pr_number"),
            F.col("payload.pull_request.state").alias("pr_state"),
            F.col("payload.pull_request.merged").alias("pr_merged"),
            F.col("payload.push_id").alias("push_id"),
            F.col("payload.size").alias("push_size"),
            F.col("payload.distinct_size").alias("push_distinct_size"),
            F.size(F.col("payload.commits")).alias("commit_count"),
        )
        .filter(F.col("event_ts").isNotNull())
        .filter(F.col("repo_name").isNotNull())
        .dropDuplicates(["event_id"])
    )

    cleaned_df = filter_seed_repos(cleaned_df, spark, args.seed_repo_file)

    cleaned_df.write.mode(args.write_mode).partitionBy("event_date").parquet(args.output)

    sample_df = cleaned_df.orderBy("event_ts").limit(args.sample_rows)
    write_df_as_local_csv(sample_df, args.sample_output)

    print("=== Cleaned GH Archive Schema ===")
    cleaned_df.printSchema()
    print(f"Total cleaned records: {cleaned_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
