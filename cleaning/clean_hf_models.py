from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampNTZType, TimestampType

from schemas.hf_models_schema import hf_models_schema
from utils.paths import HF_MODELS_CLEANED_DIR, SAMPLES_DIR, ensure_parent_dir
from utils.spark_session import create_spark_session


LICENSE_NORMALIZE_MAP = {
    "apache 2.0": "apache-2.0",
    "apache2": "apache-2.0",
    "apache-2": "apache-2.0",
    "mit license": "mit",
    "gpl-3": "gpl-3.0",
    "gpl3": "gpl-3.0",
    "gplv3": "gpl-3.0",
    "bsd-3": "bsd-3-clause",
    "bsd-2": "bsd-2-clause",
    "cc-by-4": "cc-by-4.0",
    "cc-by-sa-4": "cc-by-sa-4.0",
    "cc-by-nc-4": "cc-by-nc-4.0",
    "cc-by-nc-sa-4": "cc-by-nc-sa-4.0",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean Hugging Face models data into an analysis-ready Parquet table."
    )
    parser.add_argument("--input", required=True, help="Path to HF models JSONL or Parquet data.")
    parser.add_argument(
        "--input-format",
        choices=["json", "parquet"],
        default="parquet",
        help="Input format.",
    )
    parser.add_argument(
        "--output",
        default=str(HF_MODELS_CLEANED_DIR),
        help="Output directory for cleaned Parquet data.",
    )
    parser.add_argument(
        "--sample-output",
        default=str(SAMPLES_DIR / "clean_hf_models_sample.csv"),
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
        return spark.read.schema(hf_models_schema).json(path)
    return spark.read.parquet(path)


def build_license_mapping(spark) -> DataFrame:
    rows = [(k, v) for k, v in LICENSE_NORMALIZE_MAP.items()]
    return spark.createDataFrame(rows, ["raw_license", "normalized_license"])


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
    spark = create_spark_session("HFModelsCleaning")

    raw_df = read_input(spark, args.input, args.input_format)

    # 1. Filter out private and disabled models
    filtered_df = raw_df.filter(
        (F.col("private") == False) | F.col("private").isNull()  # noqa: E712
    ).filter(
        (F.col("disabled") == False) | F.col("disabled").isNull()  # noqa: E712
    )

    # 2. Select and transform columns
    cleaned_df = filtered_df.select(
        F.col("modelId").alias("model_id"),
        # split modelId into owner and model_name
        F.split(F.col("modelId"), "/").getItem(0).alias("owner"),
        F.when(
            F.size(F.split(F.col("modelId"), "/")) > 1,
            F.split(F.col("modelId"), "/").getItem(1),
        ).alias("model_name"),
        F.lower(F.col("author")).alias("author"),
        F.to_timestamp("lastModified").alias("last_modified_ts"),
        F.to_date(F.to_timestamp("lastModified")).alias("last_modified_date"),
        F.to_timestamp("created_at").alias("created_ts"),
        # normalize pipeline_tag
        F.coalesce(F.lower(F.col("pipeline_tag")), F.lit("unknown")).alias("pipeline_tag"),
        # normalize library_name
        F.coalesce(F.lower(F.col("library_name")), F.lit("unknown")).alias("library_name"),
        F.col("downloads"),
        F.col("likes"),
        F.col("downloads_all_time"),
        F.col("trending_score"),
        # extract from tags
        F.col("tags"),
        F.size(F.coalesce(F.col("tags"), F.array())).alias("tag_count"),
        # extract from card_data
        F.lower(F.col("card_data.license")).alias("raw_license"),
        F.col("card_data.datasets").alias("training_datasets"),
        F.col("card_data.language").alias("languages"),
        F.col("card_data.base_model").alias("base_model"),
        # safetensors parameters
        F.col("safetensors.parameters.total").alias("parameter_count"),
        # gated status
        F.col("gated"),
    )

    # 3. Filter bad data
    cleaned_df = (
        cleaned_df.filter(F.col("model_id").isNotNull())
        .filter(F.col("model_id").contains("/"))
        .filter(F.col("last_modified_ts").isNotNull())
        .dropDuplicates(["model_id"])
    )

    # 4. Normalize license via mapping
    license_map_df = build_license_mapping(spark).withColumnRenamed("raw_license", "lookup_license")
    cleaned_df = (
        cleaned_df.join(
            license_map_df,
            cleaned_df.raw_license == license_map_df.lookup_license,
            "left",
        )
        .withColumn(
            "license",
            F.coalesce(F.col("normalized_license"), F.col("raw_license"), F.lit("unknown")),
        )
        .drop("raw_license", "normalized_license", "lookup_license")
    )

    # 5. Write cleaned Parquet
    cleaned_df.write.mode(args.write_mode).parquet(args.output)

    # 6. Sample
    sample_df = (
        cleaned_df.select(
            "model_id", "owner", "model_name", "author", "pipeline_tag",
            "library_name", "license", "downloads", "likes", "parameter_count",
            "last_modified_ts",
        )
        .orderBy(F.desc("downloads"))
        .limit(args.sample_rows)
    )
    write_df_as_local_csv(sample_df, args.sample_output)

    print("=== Cleaned HF Models Schema ===")
    cleaned_df.printSchema()
    print(f"Total cleaned records: {cleaned_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
