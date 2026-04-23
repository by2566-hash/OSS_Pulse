# OSS Pulse SSH/HPC Runbook

This folder contains the SSH/HPC scripts and directory conventions for running the GH Archive pipeline at the project scale.

## 1. Recommended Strategy

For this project, the recommended execution mode is:

- **monthly rolling processing**
- keep **monthly source and raw data only temporarily**
- keep **final cleaned Parquet for the full year**
- keep **profiling CSVs, samples, and logs on SSH/local storage**

This strategy is designed for clusters where:

- local home storage is not large enough to hold the full year of `json.gz`
- HDFS quota is not large enough to retain full-year source + raw + cleaned simultaneously

## 2. Directory Convention

The project uses **two storage layers**:

- **SSH / local filesystem on the HPC node**
  - stores project code
  - stores the current month's downloaded `json.gz` archives
  - stores profiling CSVs, small dataset samples, logs, and report artifacts
- **HDFS**
  - stores temporary monthly source and raw data during processing
  - stores the final full-year cleaned Parquet dataset

### Recommended SSH / local layout

```text
$HOME/oss_pulse/
├── project/                      # synced code workspace
├── data/
│   └── source/
│       └── gharchive/
│           └── 2025/
│               ├── 2025-01/      # current month download folder
│               ├── 2025-02/
│               └── ...
├── output/
│   └── gharchive/
│       └── 2025/
│           ├── profiling/
│           │   ├── monthly/
│           │   │   ├── 2025-01/
│           │   │   ├── 2025-02/
│           │   │   └── ...
│           │   └── gharchive_clean_*_2025.csv
│           └── samples/
│               ├── monthly/
│               └── clean_gharchive_profile_sample_2025.csv
├── logs/
│   └── gharchive/
│       └── 2025/
│           ├── monthly/
│           └── profile_clean_gharchive_2025.log
└── reports/
```

### Recommended HDFS layout

```text
/user/$USER/oss_pulse/
├── stage/
│   ├── source/
│   │   └── gharchive/
│   │       └── 2025/
│   │           ├── 2025-01/
│   │           ├── 2025-02/
│   │           └── ...
│   ├── raw/
│   │   └── gharchive/
│   │       └── 2025/
│   │           ├── 2025-01/
│   │           ├── 2025-02/
│   │           └── ...
│   └── cleaned/
│       └── gharchive/
│           └── 2025/
│               ├── 2025-01/
│               ├── 2025-02/
│               └── ...
└── cleaned/
    └── gharchive/
        └── 2025/
            ├── event_date=2025-01-01/
            ├── event_date=2025-01-02/
            └── ...
```

## 3. Script Roles

### Rolling scripts you should use

- `hpc/scripts/download_gharchive_month.sh`
  - download one month of GH Archive files to SSH/local storage
- `hpc/scripts/upload_gharchive_month_to_hdfs.sh`
  - upload one month of source files to HDFS staging
- `hpc/scripts/run_gharchive_month_pipeline.sh`
  - ingest, profile, clean, profile, merge into final yearly cleaned output, then clean up staging
- `hpc/scripts/run_gharchive_2025_rolling.sh`
  - orchestrate the full year month-by-month

### Legacy one-shot scripts

The following scripts remain available, but they assume much larger storage:

- `hpc/scripts/download_gharchive_2025.sh`
- `hpc/scripts/upload_gharchive_2025_to_hdfs.sh`
- `hpc/scripts/run_gharchive_2025_pipeline.sh`

Use them only if your local and HDFS capacity are large enough to retain the entire year at once.

## 4. Setup Sequence

### 4.1 Copy the environment template

```bash
cd /path/to/project
cp hpc/env/oss_pulse_2025.env.example hpc/env/oss_pulse_2025.env
```

Edit `hpc/env/oss_pulse_2025.env` to match your SSH username, paths, Spark defaults, and Python environment.

### 4.2 Create the directory layout

```bash
bash hpc/scripts/setup_oss_pulse_hpc_layout.sh hpc/env/oss_pulse_2025.env
```

### 4.3 Sync or clone the project code to SSH

Recommended target:

```bash
$HOME/oss_pulse/project
```

## 5. Recommended Execution Flow

### 5.1 Run a one-day smoke test first

Before the rolling full-year run, confirm that:

- `spark-submit` works
- `hdfs` works
- ingestion, profiling, and cleaning all succeed for one day

### 5.2 Run the rolling yearly pipeline

```bash
bash hpc/scripts/run_gharchive_2025_rolling.sh hpc/env/oss_pulse_2025.env
```

Optional: run only part of the year.

```bash
bash hpc/scripts/run_gharchive_2025_rolling.sh hpc/env/oss_pulse_2025.env 1 3
```

That example runs January through March.

## 6. What Gets Retained

After each month:

- monthly local source files can be deleted automatically
- monthly HDFS staged source can be deleted automatically
- monthly HDFS staged raw can be deleted automatically
- monthly staged cleaned data is merged into the final yearly cleaned directory and then removed

What remains long-term:

- final yearly cleaned Parquet in `HDFS_CLEAN_DIR`
- monthly profiling CSVs
- monthly samples
- monthly logs
- final full-year cleaned profile CSVs

## 7. Notes

- This rolling design is intentionally rerunnable. If you rerun a month, that month's final cleaned partitions are removed and replaced before the merge step completes.
- If you later create a curated AI repository seed list, you can point `SEED_REPO_FILE` in the env file to that CSV so the rolling pipeline keeps only the repos relevant to OSS Pulse.
- The same core Spark Python code is reused locally and on SSH/HPC; only the orchestration layer changes.
