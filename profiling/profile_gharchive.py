from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import TimestampNTZType, TimestampType

from schemas.gharchive_schema import gharchive_schema
from utils.paths import PROFILING_DIR, SAMPLES_DIR, ensure_parent_dir
from utils.spark_session import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile a GH Archive dataset with Spark.")
    parser.add_argument("--input", required=True, help="Path to raw GH Archive JSON.gz or Parquet data.")
    parser.add_argument(
        "--input-format",
        choices=["json", "parquet"],
        default="parquet",
        help="Input format. Use json for direct profiling of original GH Archive files.",
    )
    parser.add_argument(
        "--summary-output",
        default=str(PROFILING_DIR / "gharchive_profile_summary.csv"),
        help="Path for the metric summary CSV.",
    )
    parser.add_argument(
        "--event-output",
        default=str(PROFILING_DIR / "gharchive_event_type_distribution.csv"),
        help="Path for event type counts.",
    )
    parser.add_argument(
        "--null-output",
        default=str(PROFILING_DIR / "gharchive_null_counts.csv"),
        help="Path for null count metrics.",
    )
    parser.add_argument(
        "--repo-output",
        default=str(PROFILING_DIR / "gharchive_top_repos.csv"),
        help="Path for the most active repositories summary.",
    )
    parser.add_argument(
        "--sample-output",
        default=str(SAMPLES_DIR / "raw_gharchive_profile_sample.csv"),
        help="Path for a small report-friendly sample CSV.",
    )
    parser.add_argument("--sample-rows", type=int, default=10, help="Number of rows to export as a sample.")
    return parser.parse_args()


def read_input(spark, path: str, input_format: str) -> DataFrame:
    if input_format == "json":
        return spark.read.schema(gharchive_schema).json(path)
    return spark.read.parquet(path)


def has_column(df: DataFrame, dotted_name: str) -> bool:
    parts = dotted_name.split(".")
    dtype = dict(df.dtypes)
    if parts[0] not in dtype:
        return False
    if len(parts) == 1:
        return True

    field = df.schema[parts[0]].dataType
    for part in parts[1:]:
        if not hasattr(field, "fieldNames") or part not in field.fieldNames():
            return False
        field = field[part].dataType
    return True


def pick_column(df: DataFrame, candidates: list[str], alias: str | None = None) -> Column:
    for candidate in candidates:
        if has_column(df, candidate):
            column = F.col(candidate)
            return column.alias(alias) if alias else column
    raise ValueError(f"None of the candidate columns exist: {candidates}")


def optional_null_metric(df: DataFrame, candidates: list[str], alias: str) -> Column:
    for candidate in candidates:
        if has_column(df, candidate):
            return F.sum(F.col(candidate).isNull().cast("int")).alias(alias)
    return F.lit(None).cast("long").alias(alias)


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
    spark = create_spark_session("GHArchiveProfiling")

    df = read_input(spark, args.input, args.input_format)
    if has_column(df, "created_at") and not has_column(df, "event_ts"):
        df = df.withColumn("event_ts", F.to_timestamp("created_at"))
    elif has_column(df, "event_ts"):
        df = df.withColumn("event_ts", F.to_timestamp("event_ts"))
    else:
        raise ValueError("Input dataset must contain either created_at or event_ts.")

    event_type_col = pick_column(df, ["type", "event_type"], "event_type")
    repo_name_col = pick_column(df, ["repo.name", "repo_name"], "repo_name")
    actor_login_col = pick_column(df, ["actor.login", "actor_login"], "actor_login")

    total_rows = df.count()
    total_columns = len(df.columns)
    distinct_repos = df.select(repo_name_col).distinct().count()
    distinct_actors = df.select(actor_login_col).distinct().count()
    min_ts, max_ts = (
        df.agg(F.min("event_ts").alias("min_ts"), F.max("event_ts").alias("max_ts"))
        .select(
            F.date_format("min_ts", "yyyy-MM-dd HH:mm:ss").alias("min_ts"),
            F.date_format("max_ts", "yyyy-MM-dd HH:mm:ss").alias("max_ts"),
        )
        .first()
    )

    summary_rows = [
        ("total_rows", total_rows),
        ("total_columns", total_columns),
        ("distinct_repositories", distinct_repos),
        ("distinct_actors", distinct_actors),
        ("min_event_timestamp", min_ts),
        ("max_event_timestamp", max_ts),
    ]
    write_single_csv(summary_rows, args.summary_output)

    event_type_df = df.select(event_type_col).groupBy("event_type").count().orderBy(F.desc("count"))
    write_df_as_local_csv(event_type_df, args.event_output)

    null_counts = (
        df.agg(
            optional_null_metric(df, ["id", "event_id"], "id_nulls"),
            optional_null_metric(df, ["type", "event_type"], "type_nulls"),
            optional_null_metric(df, ["created_at", "event_ts"], "created_at_or_event_ts_nulls"),
            optional_null_metric(df, ["actor.login", "actor_login"], "actor_login_nulls"),
            optional_null_metric(df, ["repo.name", "repo_name"], "repo_name_nulls"),
            optional_null_metric(df, ["payload.action", "payload_action"], "payload_action_nulls"),
        )
        .collect()[0]
        .asDict()
    )
    write_single_csv(list(null_counts.items()), args.null_output)

    top_repo_df = (
        df.select(repo_name_col)
        .groupBy("repo_name")
        .count()
        .filter(F.col("repo_name").isNotNull())
        .orderBy(F.desc("count"))
        .limit(20)
    )
    write_df_as_local_csv(top_repo_df, args.repo_output)

    sample_df = (
        df.select(
            pick_column(df, ["id", "event_id"], "id"),
            event_type_col,
            pick_column(df, ["created_at", "event_ts"], "created_at_or_event_ts"),
            actor_login_col,
            repo_name_col,
        )
        .limit(args.sample_rows)
    )
    write_df_as_local_csv(sample_df, args.sample_output)

    spark.stop()


if __name__ == "__main__":
    main()
