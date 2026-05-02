from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from utils.paths import HF_MODELS_RAW_DIR, SAMPLES_DIR, ensure_parent_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Hugging Face Hub model metadata."
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    # --- fetch mode: download metadata from HF Hub API ---
    fetch_parser = sub.add_parser("fetch", help="Download model metadata from the Hugging Face Hub API.")
    fetch_parser.add_argument(
        "--output",
        default="data/source/huggingface_hub/hf_models.jsonl",
        help="Output path for the JSON Lines file.",
    )

    # --- spark mode: convert JSONL to raw Parquet ---
    spark_parser = sub.add_parser("spark", help="Convert fetched JSONL to raw Parquet using Spark.")
    spark_parser.add_argument(
        "--input",
        required=True,
        help="Path to the HF models JSONL file.",
    )
    spark_parser.add_argument(
        "--raw-output",
        default=str(HF_MODELS_RAW_DIR),
        help="Output directory for raw Parquet data.",
    )
    spark_parser.add_argument(
        "--sample-output",
        default=str(SAMPLES_DIR / "raw_hf_models_sample.csv"),
        help="CSV path for a small raw-data snippet used in the report.",
    )
    spark_parser.add_argument(
        "--sample-rows",
        type=int,
        default=10,
        help="Number of rows to export to the sample CSV.",
    )
    spark_parser.add_argument(
        "--write-partitions",
        type=int,
        default=16,
        help="Number of partitions to use when writing raw Parquet.",
    )
    spark_parser.add_argument(
        "--write-mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Spark write mode for the raw Parquet output.",
    )
    return parser.parse_args()


# ── fetch mode ──────────────────────────────────────────────


def run_fetch(args: argparse.Namespace) -> None:
    try:
        from huggingface_hub import HfApi
    except ImportError:
        print("ERROR: huggingface_hub is not installed. Run: pip install huggingface_hub")
        sys.exit(1)

    api = HfApi()
    output_path = ensure_parent_dir(args.output)
    eval_output_path = ensure_parent_dir(
        Path(output_path).parent / "hf_eval_results.jsonl"
    )

    print("Fetching model metadata from Hugging Face Hub (this may take a while) ...")
    count = 0
    eval_count = 0
    with (
        output_path.open("w", encoding="utf-8") as f,
        eval_output_path.open("w", encoding="utf-8") as ef,
    ):
        for model in api.list_models(
            sort="downloads",
            expand=[
                "author", "cardData", "config", "createdAt", "disabled",
                "downloads", "downloadsAllTime", "gated",
                "lastModified", "library_name", "likes", "pipeline_tag",
                "private", "safetensors", "sha", "tags", "trendingScore",
            ],
        ):
            # --- main model record ---
            record = {
                "modelId": model.id,
                "author": model.author,
                "sha": model.sha,
                "lastModified": model.last_modified,
                "private": model.private,
                "disabled": model.disabled,
                "gated": model.gated if isinstance(model.gated, str) else str(model.gated) if model.gated else None,
                "pipeline_tag": model.pipeline_tag,
                "tags": model.tags,
                "library_name": model.library_name,
                "likes": model.likes,
                "downloads": model.downloads,
                "downloads_all_time": getattr(model, "downloads_all_time", None),
                "trending_score": getattr(model, "trending_score", None),
                "card_data": None,
                "safetensors": None,
                "config": None,
                "created_at": getattr(model, "created_at", None),
            }

            if model.card_data:
                record["card_data"] = {
                    "license": getattr(model.card_data, "license", None),
                    "datasets": getattr(model.card_data, "datasets", None),
                    "language": getattr(model.card_data, "language", None),
                    "base_model": getattr(model.card_data, "base_model", None)
                    if isinstance(getattr(model.card_data, "base_model", None), str)
                    else None,
                }

            if model.safetensors:
                total = getattr(model.safetensors, "total", None)
                if total is None and isinstance(model.safetensors, dict):
                    total = model.safetensors.get("total", None)
                if total is not None:
                    record["safetensors"] = {"parameters": {"total": total}}

            if model.config:
                cfg = model.config if isinstance(model.config, dict) else {}
                record["config"] = {
                    "model_type": cfg.get("model_type", None),
                    "architectures": cfg.get("architectures", None),
                }

            # serialize datetimes to ISO strings
            for key in ("lastModified", "created_at"):
                val = record[key]
                if val is not None and not isinstance(val, str):
                    record[key] = val.isoformat()

            f.write(json.dumps(record, ensure_ascii=False) + "\n")

            # --- eval results (separate file) ---
            if model.card_data:
                eval_results = getattr(model.card_data, "eval_results", None)
                if eval_results:
                    for er in eval_results:
                        eval_record = {
                            "model_id": model.id,
                            "task_type": getattr(er, "task_type", None),
                            "dataset_type": getattr(er, "dataset_type", None),
                            "dataset_name": getattr(er, "dataset_name", None),
                            "dataset_config": getattr(er, "dataset_config", None),
                            "dataset_split": getattr(er, "dataset_split", None),
                            "metric_type": getattr(er, "metric_type", None),
                            "metric_value": getattr(er, "metric_value", None),
                            "verified": getattr(er, "verified", None),
                        }
                        ef.write(json.dumps(eval_record, ensure_ascii=False) + "\n")
                        eval_count += 1

            count += 1
            if count % 50_000 == 0:
                print(f"  ... fetched {count:,} models ({eval_count:,} eval results)")

    print(f"Done. Wrote {count:,} models to {output_path}")
    print(f"Done. Wrote {eval_count:,} eval results to {eval_output_path}")


# ── spark mode ──────────────────────────────────────────────


def write_sample_csv(df, output_path: str | Path, sample_rows: int) -> None:
    sample_df = (
        df.select(
            "modelId",
            "author",
            "pipeline_tag",
            "library_name",
            "downloads",
            "likes",
            "lastModified",
        )
        .limit(sample_rows)
        .collect()
    )

    output_file = ensure_parent_dir(output_path)
    with output_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["modelId", "author", "pipeline_tag", "library_name", "downloads", "likes", "lastModified"])
        for row in sample_df:
            writer.writerow(row)


def run_spark(args: argparse.Namespace) -> None:
    from pipeline.huggingface.schema import hf_models_schema
    from utils.spark_session import create_spark_session

    spark = create_spark_session("HFModelsIngestion")

    raw_df = spark.read.schema(hf_models_schema).json(args.input)

    print("=== Raw HF Models Schema ===")
    raw_df.printSchema()
    print(f"Total raw records: {raw_df.count()}")

    raw_df.repartition(args.write_partitions).write.mode(args.write_mode).parquet(args.raw_output)
    write_sample_csv(raw_df, args.sample_output, args.sample_rows)

    spark.stop()


# ── entry point ─────────────────────────────────────────────


def main() -> None:
    args = parse_args()
    if args.mode == "fetch":
        run_fetch(args)
    elif args.mode == "spark":
        run_spark(args)


if __name__ == "__main__":
    main()
