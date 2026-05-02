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
│   ├── 01_repo_daily_metrics.py              # AI repos daily activity
│   ├── 02_hf_gh_join.py                      # HF + GH join
│   ├── 03_health_score.py                    # three-way health score
│   ├── 04_top_repos_all.py                   # top 1000 repos (full ecosystem)
│   ├── 05_ai_vs_general.py                   # AI vs general comparison
│   ├── 06_star_growth_hype.py                # hype detection
│   └── 07_contributor_health.py              # contributor diversity
│
├── src/main/scala/osspulse/    # Scala Spark pipeline (HF Hub ingestion)
├── build.sbt
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
| Hugging Face Hub | April 2026 snapshot (2,815,064 models) | `/user/jl17797_nyu_edu/oss_pulse/cleaned/huggingface_hub/` |
| PyPI downloads | 2025 monthly (46 libraries) | `data/source/pypi_monthly_downloads.jsonl` |

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
