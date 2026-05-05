# OSS Pulse

Group 9 — NYU BDAD Spring 2026
**jl17797** (Jhe Chen Li) · **by2566** (Bo Yu)

Analysing open-source software ecosystem health by combining
GitHub Archive event data, Hugging Face Hub model metadata, and PyPI download statistics.

---

## Project Structure

```
OSS_Pulse/
├── pipeline/                   # Python data pipeline (by data source)
│   ├── gharchive/              # GH Archive: schema, ingest, clean, profile, EDA
│   │   ├── schema.py
│   │   ├── ingest.py
│   │   ├── clean.py
│   │   ├── profile.py
│   │   ├── eda.ipynb
│   │   └── smoke_test.sh       # local one-day smoke test
│   └── huggingface/            # HF Hub: schema, ingest, clean, profile, EDA
│       ├── schema.py
│       ├── ingest.py
│       ├── clean.py
│       ├── profile.py
│       └── eda.py
│
├── spark_jobs/                 # Standalone Spark jobs for NYU Dataproc
│   ├── 00_download_gharchive_supplement.sh   # download 2025-12 ~ 2026-04 → HDFS
│   ├── 00_clean_gharchive_supplement.py      # clean supplement data
│   ├── 00_clean_gharchive_20XXq1.py          # clean Q1 era data (2022–2024)
│   ├── 01_repo_daily_metrics.py              # AI repos daily activity
│   ├── 02_hf_gh_join.py                      # HF + GH join
│   ├── 03_health_score.py                    # three-way health score
│   ├── 04_top_repos_all.py                   # top 1000 repos (full ecosystem)
│   ├── 05_ai_vs_general.py                   # AI vs general comparison
│   ├── 06_star_growth_hype.py                # hype detection
│   ├── 07_contributor_health.py              # contributor diversity
│   ├── 08_era_comparison.py                  # cross-era comparison (2022–2026 Q1)
│   └── 09_repo_era_deep_dive.py              # per-repo era deep dive (AI vs non-AI)
│
├── src/main/scala/osspulse/    # Scala Spark pipeline (HF Hub ingestion)
├── build.sbt
│
├── data/                       # Analysis result CSVs (committed to git)
│   ├── ai_vs_general.csv
│   ├── contributor_health.csv
│   ├── health_score.csv
│   ├── hf_gh_join.csv
│   ├── star_growth_hype.csv
│   ├── top_repos_all.csv
│   └── era_comparison/         # Job 08 cross-era outputs
│       ├── summary_metrics.csv
│       ├── push_size_distribution.csv
│       ├── pr_push_ratio.csv
│       └── active_repos_monthly.csv
│
├── utils/                      # Shared Python utilities
│   ├── paths.py
│   └── spark_session.py
│
├── hpc/                        # Dataproc cluster config
│   └── env/oss_pulse_2025.env.example
│
├── logs/                       # Runbook and EDA progress log
│   ├── pipeline_runbook.md
│   └── eda_progress.md
│
└── report/                     # Reports and LaTeX source
```

---

## Data Sources

| Source | Coverage | Location (HDFS) |
|--------|----------|-----------------|
| GH Archive | 2025-01-01 ~ 2025-11-30 (334 days) | `/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/` |
| GH Archive supplement | 2025-12-01 ~ 2026-04-30 | `/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement/` |
| GH Archive 2022 Q1 | 2022-01 ~ 2022-03 | `/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2022q1/` |
| GH Archive 2023 Q1 | 2023-01 ~ 2023-03 | `/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2023q1/` |
| GH Archive 2024 Q1 | 2024-01 ~ 2024-03 | `/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2024q1/` |
| GH Archive 2025 Q1 | 2025-01 ~ 2025-03 | `/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2025q1/` |
| GH Archive 2026 Q1 | 2026-01 ~ 2026-03 | `/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1/` |
| Hugging Face Hub | April 2026 snapshot (2,815,064 models) | `/user/jl17797_nyu_edu/oss_pulse/cleaned/huggingface_hub/` |
| PyPI downloads | 2025 monthly (46 libraries) | `data/source/pypi_monthly_downloads.jsonl` |

**Total scale:** ~1.46 billion events, 87.9 GB cleaned Parquet across 5 Q1 eras.

---

## Analytics Outputs

| Job | Output | Description |
|-----|--------|-------------|
| 01 | `repo_daily_metrics` | Daily activity metrics per AI seed repo |
| 02 | `hf_gh_join` | HuggingFace model stats joined with GitHub activity |
| 03 | `health_score` | Composite health score (HF 30% + PyPI 20% + GH 50%) |
| 04 | `top_repos_all` | Top 1000 repos across entire GitHub ecosystem |
| 05 | `ai_vs_general` | AI repos vs general repos comparison |
| 06 | `star_growth_hype` | Star growth rate and hype detection |
| 07 | `contributor_health` | Bus factor, push concentration, contributor diversity |
| 08 | `era_comparison` | Cross-era comparison across 5 Q1 snapshots (2022–2026) |

---

## Pipeline Entry Points

### Local smoke test (GH Archive, one day)
```bash
bash pipeline/gharchive/smoke_test.sh 2025-01-16
```

### Dataproc Spark jobs
```bash
# Upload and run (replace <N> with job number)
gcloud compute scp spark_jobs/<script>.py nyu-dataproc-m:/tmp/ \
  --project=hpc-dataproc-19b8 --zone=us-central1-f
gcloud compute ssh nyu-dataproc-m --project=hpc-dataproc-19b8 --zone=us-central1-f \
  --command "spark-submit /tmp/<script>.py"
```

See `logs/pipeline_runbook.md` for the full execution order and status.

---

## Requirements

- Python 3.13 · `pyspark==4.1.1` · Java 17
- Scala 2.12 · sbt (for `scala/` module)
- Google Cloud SDK (`gcloud`) for Dataproc access
