# Hugging Face Hub Model Metadata — Data Profiling, Cleaning, and Ingestion Report

## 1. Introduction

This report documents the ingestion, profiling, and cleaning workflow for the Hugging Face Hub model metadata used in the OSS Pulse project. The objective is to convert raw model metadata from the Hugging Face Hub API into a structured, analysis-ready dataset that can support downstream Spark analytics on AI open-source project adoption and health.

## 2. Data Source Description

- **Data source**: Hugging Face Hub — model metadata
- **Owner / access**: Public API via the `huggingface_hub` Python library
- **Original format**: JSON Lines (one record per model), fetched from `HfApi.list_models()`
- **Collection scope**: All public models on the Hugging Face Hub (snapshot taken on TODO_DATE)
- **Estimated size**: TODO_COUNT models, approximately TODO_SIZE on disk as JSONL

### Core Fields

| Field | Type | Description |
|-------|------|-------------|
| `modelId` | string | Unique identifier in `owner/model_name` format |
| `author` | string | The model uploader or organization |
| `pipeline_tag` | string | Task type (e.g., `text-generation`, `image-classification`) |
| `library_name` | string | Framework (e.g., `transformers`, `diffusers`, `gguf`) |
| `tags` | array of strings | User-defined and auto-detected tags |
| `downloads` | long | Download count in the last 30 days |
| `likes` | long | Number of likes on the Hub |
| `lastModified` | string (ISO timestamp) | Last update timestamp |
| `created_at` | string (ISO timestamp) | Model creation timestamp |
| `private` | boolean | Whether the model is private |
| `card_data.license` | string | License extracted from the model card |
| `card_data.datasets` | array of strings | Training datasets listed in the model card |
| `card_data.language` | array of strings | Languages the model supports |
| `safetensors.parameters.total` | long | Total parameter count (if available) |

## 3. Ingestion Workflow

The ingestion is split into two stages:

### Stage 1: API Fetch (pure Python)

- Script used: `ingestion/ingest_hf_models.py` (mode: `fetch`)
- Uses `huggingface_hub.HfApi().list_models(full=True, cardData=True)` to iterate over all public models
- Serializes each model as one JSON line into a `.jsonl` file
- This stage does **not** use Spark — it is a pure Python data collection step

### Stage 2: Spark Ingestion (JSON → Parquet)

- Script used: `ingestion/ingest_hf_models.py` (mode: `spark`)
- Spark API used: DataFrame API
- Input format: JSON Lines
- Explicit schema: yes, via `schemas/hf_models_schema.py`
- Output format: raw Parquet at `data/raw/huggingface_hub`

### Ingestion Commands

```bash
# Stage 1: fetch from API
python -m ingestion.ingest_hf_models fetch --output data/source/huggingface_hub/hf_models.jsonl

# Stage 2: convert to Parquet
python -m ingestion.ingest_hf_models spark --input data/source/huggingface_hub/hf_models.jsonl
```

## 4. Data Profiling

- Script used: `profiling/profile_hf_models.py`
- Total number of records: `TODO`
- Total number of columns: `TODO`
- Distinct authors: `TODO`
- Distinct pipeline tags: `TODO`
- Distinct library names: `TODO`

### 4.1 Downloads Statistics

| Metric | Value |
|--------|-------|
| Min downloads | TODO |
| Max downloads | TODO |
| Average downloads | TODO |
| Median downloads | TODO |

### 4.2 Pipeline Tag Distribution

| pipeline_tag | count |
|-------------|-------|
| TODO | TODO |

(Top 10 pipeline tags from `output/profiling/hf_models_pipeline_tag_distribution.csv`)

### 4.3 Library Name Distribution

| library_name | count |
|-------------|-------|
| TODO | TODO |

(Top 10 library names from `output/profiling/hf_models_library_distribution.csv`)

### 4.4 Missing Values and Data Quality

| Column | Null Count |
|--------|-----------|
| modelId | TODO |
| author | TODO |
| pipeline_tag | TODO |
| library_name | TODO |
| downloads | TODO |
| likes | TODO |
| lastModified | TODO |
| created_at | TODO |
| card_data.license | TODO |
| card_data.datasets | TODO |

### 4.5 Observed Data Issues

- Many models lack a `pipeline_tag` — they are uploaded without specifying a task type
- `library_name` is null for models not associated with a recognized framework
- `card_data.license` is inconsistently formatted (e.g., `apache-2.0` vs `Apache 2.0`)
- `card_data.datasets` is often null — many uploaders do not specify training data
- Some models have `downloads = 0` and `likes = 0`, indicating placeholder or unused uploads
- `safetensors.parameters.total` is only available for models stored in safetensors format

## 5. Data Cleaning

- Script used: `cleaning/clean_hf_models.py`

### Cleaning Steps

1. **Filter private and disabled models** — remove `private = true` and `disabled = true` records.
2. **Split `modelId`** into `owner` and `model_name` columns.
3. **Normalize `pipeline_tag`** — lowercase; null values replaced with `"unknown"`.
4. **Normalize `library_name`** — lowercase; null values replaced with `"unknown"`.
5. **Parse timestamps** — convert `lastModified` → `last_modified_ts` + `last_modified_date`; convert `created_at` → `created_ts`.
6. **Extract `card_data` fields** — `license`, `training_datasets`, `languages`, `base_model`.
7. **Normalize license** — map common synonyms (e.g., `"apache 2.0"` → `"apache-2.0"`); null → `"unknown"`.
8. **Compute derived fields** — `tag_count` from tags array; `parameter_count` from safetensors.
9. **Filter bad data** — drop records where `modelId` is null, does not contain `/`, or `lastModified` is null.
10. **Deduplicate** — `dropDuplicates(["model_id"])`.

### Cleaned Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `model_id` | string | Original `modelId` (e.g., `meta-llama/Llama-3-8B`) |
| `owner` | string | Organization or user |
| `model_name` | string | Model name after the `/` |
| `author` | string | Lowercased author |
| `pipeline_tag` | string | Normalized task type |
| `library_name` | string | Normalized framework |
| `license` | string | Normalized license |
| `downloads` | long | 30-day download count |
| `likes` | long | Like count |
| `downloads_all_time` | long | All-time downloads |
| `trending_score` | long | Trending score |
| `tag_count` | int | Number of tags |
| `parameter_count` | long | Model parameter count |
| `training_datasets` | array | Training datasets from card |
| `languages` | array | Supported languages |
| `base_model` | string | Base model if fine-tuned |
| `gated` | string | Access restriction status |
| `last_modified_ts` | timestamp | Last modified timestamp |
| `last_modified_date` | date | Last modified date |
| `created_ts` | timestamp | Creation timestamp |
| `tags` | array | Original tag list |

## 6. Dataset Snippet

### Raw Data Sample

(From `output/samples/raw_hf_models_sample.csv`)

| modelId | author | pipeline_tag | library_name | downloads | likes | lastModified |
|---------|--------|-------------|-------------|-----------|-------|-------------|
| TODO | TODO | TODO | TODO | TODO | TODO | TODO |

### Cleaned Data Sample

(From `output/samples/clean_hf_models_sample.csv`)

| model_id | owner | model_name | pipeline_tag | library_name | license | downloads | likes | parameter_count |
|----------|-------|-----------|-------------|-------------|---------|-----------|-------|----------------|
| TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO |

## 7. Output for the Next Project Stage

The cleaned Hugging Face models dataset will support the following downstream analyses:

- **Adoption signals**: download and like distributions across pipeline tags and libraries to identify which AI tasks and frameworks have the strongest real-world usage.
- **Cross-source join with GH Archive**: linking `owner/model_name` to GitHub repository names to correlate GitHub activity (stars, forks, PRs, issues) with Hugging Face adoption metrics.
- **Under-recognized project detection**: models with high downloads but low GitHub stars, or vice versa.
- **Health scoring enrichment**: the `parameter_count`, `license`, `tag_count`, and `downloads` fields will feed into the repository health dashboard and the OpenSSF-inspired scoring framework.
- **Ecosystem analysis**: comparing model counts, downloads, and community activity across task categories (e.g., text-generation vs image-classification) and framework ecosystems (e.g., transformers vs diffusers).

## 8. Appendix

### 8.1 Commands Used for This Submission

```bash
cd /path/to/OSS_Pulse

# Fetch metadata from Hugging Face Hub API
python -m ingestion.ingest_hf_models fetch \
  --output data/source/huggingface_hub/hf_models.jsonl

# Spark: convert JSONL to raw Parquet
python -m ingestion.ingest_hf_models spark \
  --input data/source/huggingface_hub/hf_models.jsonl

# Profile raw data
python -m profiling.profile_hf_models \
  --input data/raw/huggingface_hub \
  --input-format parquet

# Clean data
python -m cleaning.clean_hf_models \
  --input data/raw/huggingface_hub \
  --input-format parquet

# Profile cleaned data
python -m profiling.profile_hf_models \
  --input data/cleaned/huggingface_hub \
  --input-format parquet \
  --summary-output output/profiling/hf_clean_profile_summary.csv \
  --pipeline-tag-output output/profiling/hf_clean_pipeline_tag_distribution.csv \
  --library-output output/profiling/hf_clean_library_distribution.csv \
  --null-output output/profiling/hf_clean_null_counts.csv \
  --top-authors-output output/profiling/hf_clean_top_authors.csv \
  --top-downloads-output output/profiling/hf_clean_top_downloads.csv \
  --sample-output output/samples/clean_hf_models_profile_sample.csv
```

### 8.2 Running on NYU Dataproc

```bash
# SSH into Dataproc master node
gcloud compute ssh nyu-dataproc-m --project=hpc-dataproc-19b8 --zone=us-central1-f

# Upload code and data
gcloud compute scp --recurse ./OSS_Pulse \
  jl17797_nyu_edu@nyu-dataproc-m:~/oss_pulse/project \
  --project=hpc-dataproc-19b8 --zone=us-central1-f

# Upload JSONL to HDFS
hdfs dfs -mkdir -p /user/jl17797_nyu_edu/oss_pulse/source/huggingface_hub
hdfs dfs -put data/source/huggingface_hub/hf_models.jsonl \
  /user/jl17797_nyu_edu/oss_pulse/source/huggingface_hub/

# Run Spark jobs
spark-submit --deploy-mode client \
  ingestion/ingest_hf_models.py spark \
  --input hdfs:///user/jl17797_nyu_edu/oss_pulse/source/huggingface_hub/hf_models.jsonl \
  --raw-output hdfs:///user/jl17797_nyu_edu/oss_pulse/raw/huggingface_hub

spark-submit --deploy-mode client \
  profiling/profile_hf_models.py \
  --input hdfs:///user/jl17797_nyu_edu/oss_pulse/raw/huggingface_hub

spark-submit --deploy-mode client \
  cleaning/clean_hf_models.py \
  --input hdfs:///user/jl17797_nyu_edu/oss_pulse/raw/huggingface_hub
```
