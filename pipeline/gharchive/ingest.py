from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import functions as F

from pipeline.gharchive.schema import gharchive_schema
from utils.paths import GHARCHIVE_RAW_DIR, SAMPLES_DIR, ensure_parent_dir
from utils.spark_session import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest GH Archive JSON.gz files into Spark.")
    parser.add_argument(
        "--input",
        required=True,
        help="Path or glob for GH Archive JSON.gz files, e.g. data/source/2025-01-*.json.gz",
    )
    parser.add_argument(
        "--raw-output",
        default=str(GHARCHIVE_RAW_DIR),
        help="Output directory for raw Parquet data.",
    )
    parser.add_argument(
        "--sample-output",
        default=str(SAMPLES_DIR / "raw_gharchive_sample.csv"),
        help="CSV path for a small raw-data snippet used in the report.",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=10,
        help="Number of rows to export to the sample CSV.",
    )
    parser.add_argument(
        "--write-partitions",
        type=int,
        default=48,
        help="Number of partitions to use when writing raw Parquet locally.",
    )
    parser.add_argument(
        "--write-mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Spark write mode for the raw Parquet output.",
    )
    return parser.parse_args()


def write_sample_csv(df, output_path: str | Path, sample_rows: int) -> None:
    sample_df = (
        df.select(
            F.col("id"),
            F.col("type"),
            F.col("created_at"),
            F.col("actor.login").alias("actor_login"),
            F.col("repo.name").alias("repo_name"),
            F.col("public"),
        )
        .limit(sample_rows)
        .collect()
    )

    output_file = ensure_parent_dir(output_path)
    with output_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "type", "created_at", "actor_login", "repo_name", "public"])
        for row in sample_df:
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    spark = create_spark_session("GHArchiveIngestion")

    raw_df = spark.read.schema(gharchive_schema).json(args.input)

    print("=== Raw GH Archive Schema ===")
    raw_df.printSchema()
    print(f"Total raw records: {raw_df.count()}")

    raw_df.repartition(args.write_partitions).write.mode(args.write_mode).parquet(args.raw_output)
    write_sample_csv(raw_df, args.sample_output, args.sample_rows)

    spark.stop()


if __name__ == "__main__":
    main()
