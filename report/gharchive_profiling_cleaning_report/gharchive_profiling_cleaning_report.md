# GH Archive Data Profiling, Cleaning, and Ingestion Report

## 1. Introduction

This report documents the ingestion, profiling, and cleaning workflow for the GH Archive data used in the OSS Pulse project. The objective of this phase is to convert raw GitHub public event data into a structured dataset that can support downstream Spark analytics on AI-related open-source repositories.

## 2. Data Source Description

- **Data source**: GH Archive
- **Owner / access**: Public GitHub activity data archived by GH Archive
- **Original format**: Hourly `JSON.gz` files
- **Collection scope for this submission**: One 24-hour UTC window from 2025-01-16 00:00:00 UTC through 2025-01-16 23:59:59 UTC, using `data/source/gharchive/2025-01-16-*.json.gz`
- **Relevant event types**: `WatchEvent`, `ForkEvent`, `PushEvent`, `PullRequestEvent`, `IssuesEvent`

### Core Fields

- `id`
- `type`
- `created_at`
- `actor`
- `repo`
- `payload`
- `public`
- `org`

## 3. Ingestion Workflow

Describe how the raw GH Archive files were loaded into Spark.

- Script used: `ingestion/ingest_gharchive.py`
- Spark API used: DataFrame API
- Input format: `JSON.gz`
- Output format: raw Parquet

### Ingestion Notes

- Input path used: `data/source/gharchive/2025-01-16-*.json.gz`
- Explicit schema: yes, via `schemas/gharchive_schema.py`
- Raw Parquet output: `data/raw/gharchive`

## 4. Data Profiling

Describe the exploratory profiling you ran on the raw dataset.

- Script used: `profiling/profile_gharchive.py`
- Total number of records: `5,360,096`
- Total number of columns: `9`
- Distinct repositories: `947,906`
- Distinct actors: `716,998`
- Event timestamp range: `2025-01-16 00:00:00 UTC` to `2025-01-16 23:59:59 UTC`

### 4.1 Event Type Distribution

Summarize the distribution of GitHub event types and include a small table or chart.

### 4.2 Missing Values and Data Quality

Summarize important null patterns, for example:

- missing `actor.login`
- missing `repo.name`
- missing `payload.action`
- malformed or null timestamps

### 4.3 Observed Data Issues

Document any issues you found, such as:

- nested JSON complexity in `payload`
- inconsistent payload shape across event types
- duplicated event IDs
- noisy repositories outside the final analysis scope
- local CSV exports should preserve UTC timestamps explicitly when samples are shared outside Spark

## 5. Data Cleaning

Describe the transformations applied in `cleaning/clean_gharchive.py`.

### Cleaning Steps

1. Filter to the five core event types used by the project.
2. Flatten nested fields from `actor`, `repo`, `org`, and `payload`.
3. Convert `created_at` into `event_ts` and `event_date`.
4. Normalize repository and actor names to lowercase.
5. Drop records with missing repository names or invalid timestamps.
6. Remove duplicate records using `event_id`.
7. Optionally filter to a curated repository seed list.

### Cleaned Output Schema

List the main cleaned columns here:

- `event_id`
- `event_type`
- `event_ts`
- `event_date`
- `actor_id`
- `actor_login`
- `repo_id`
- `repo_name`
- `payload_action`
- `issue_number`
- `pr_number`
- `pr_merged`
- `commit_count`

## 6. Dataset Snippet

Insert a small snippet from the raw dataset and a small snippet from the cleaned dataset. Do not include the full dataset.

### Raw Data Sample

Paste or screenshot `output/samples/raw_gharchive_sample.csv`

### Cleaned Data Sample

Paste or screenshot `output/samples/clean_gharchive_sample.csv`

## 7. Output for the Next Project Stage

Explain how the cleaned GH Archive dataset will support the next stage of the project.

Suggested points:

- daily repository metrics
- trend analysis for stars, forks, issues, and pull requests
- future join with Hugging Face repository-linked metadata
- lightweight frontend views for repository detail and comparison pages

## 8. Appendix

This appendix separates the commands used for the current submission from the intended full-project scaling pattern. The submission commands reflect the local profiling/cleaning run for a single 24-hour UTC window, while the full-project commands describe how the same pipeline can be extended to the target one-year analysis scope on SSH/HPC.

### 8.1 Commands Used for This Submission

```bash
cd /Users/yubo/Downloads/2437/Project

python3 -m ingestion.ingest_gharchive \
  --input "data/source/gharchive/2025-01-16-*.json.gz"

python3 -m profiling.profile_gharchive \
  --input data/raw/gharchive \
  --input-format parquet

python3 -m cleaning.clean_gharchive \
  --input data/raw/gharchive \
  --input-format parquet

python3 -m profiling.profile_gharchive \
  --input data/cleaned/gharchive \
  --input-format parquet \
  --summary-output output/profiling/gharchive_clean_profile_summary.csv \
  --event-output output/profiling/gharchive_clean_event_type_distribution.csv \
  --null-output output/profiling/gharchive_clean_null_counts.csv \
  --repo-output output/profiling/gharchive_clean_top_repos.csv \
  --sample-output output/samples/clean_gharchive_profile_sample.csv
```

### 8.2 Project-Scale Commands for the Full-Year Target

The OSS Pulse project targets a full-year 2025 analysis window. In practice, the one-year GH Archive pipeline is intended to run on SSH/HPC rather than on a local laptop. The same scripts used in this submission can be reused at larger scale by expanding the input pattern or looping over a longer date range.

Example download pattern for a full-year 2025 archive collection:

```bash
cd /path/to/project
mkdir -p data/source/gharchive

for month in {01..12}; do
  for day in {01..31}; do
    for hour in {0..23}; do
      curl -fL --retry 3 --retry-delay 2 -C - \
        -o "data/source/gharchive/2025-${month}-${day}-${hour}.json.gz" \
        "https://data.gharchive.org/2025-${month}-${day}-${hour}.json.gz"
    done
  done
done
```

Example processing pattern on SSH/HPC for the one-year project:

```bash
cd /path/to/project

spark-submit ingestion/ingest_gharchive.py \
  --input "data/source/gharchive/2025-*.json.gz"

spark-submit profiling/profile_gharchive.py \
  --input data/raw/gharchive \
  --input-format parquet

spark-submit cleaning/clean_gharchive.py \
  --input data/raw/gharchive \
  --input-format parquet
```
