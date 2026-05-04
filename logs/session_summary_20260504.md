# OSS Pulse — Session Summary (2026-05-03 ~ 2026-05-04)

## Project Overview
- **Title:** OSS Pulse: From Hype to Health — Understanding Open Source Projects
- **Team:** Group 9 — Jhe Chen Li (jl17797) + Bo Yu (by2566)
- **Deadline:** 2026-05-05 (presentation)
- **Platform:** Google Cloud Dataproc (NYU HPC), HDFS 500 GB quota

## Data Sources & Status

### GH Archive (GitHub event data)
| Dataset | Size | Days | Status |
|---------|------|------|--------|
| 2025 full year (by2566) | 67.5 GB | 334 | ✅ Read-only from by2566's HDFS |
| Supplement (Dec 2025 – Apr 2026) | 27.3 GB | 151 | ✅ Cleaned (ignoreCorruptFiles for ~580 corrupt Apr files) |
| 2022 Q1 cleaned | 14.5 GB | 90 | ✅ |
| 2023 Q1 cleaned | 17.3 GB | 90 | ✅ |
| 2024 Q1 cleaned | 23.0 GB | 91 (leap year) | ✅ |
| **Total GH Archive** | **~99 GB cleaned** | **485+ days** | **~146M events** |

### Other Sources
- HF Hub: 2,815,064 models (April 2026 snapshot)
- PyPI: 46 libraries × 12 months

### HDFS Space
- Used: 99.5 GB / 500 GB | Available: 400.5 GB
- All Q1 raw data deleted after cleaning (rolling pipeline)

## Analytics Jobs Status

| Job | Script | Output | Status |
|-----|--------|--------|--------|
| 01 | build_repo_daily_metrics.py | repo_daily_metrics | ✅ |
| 02 | hf_gh_join.py | hf_gh_join, hf_gh_join_csv | ✅ |
| 03 | 03_health_score.py | health_score, health_score_csv | ✅ |
| 04 | 04_top_repos_all.py | top_repos_all, top_repos_all_csv | ✅ Re-run with supplement (38 min) |
| 05 | 05_ai_vs_general.py | ai_vs_general, ai_vs_general_csv | ✅ |
| 06 | 06_star_growth_hype.py | star_growth_hype, star_growth_hype_csv | ✅ Re-run with supplement (13 min) |
| 07 | 07_contributor_health.py | contributor_health, contributor_health_csv | ✅ First successful run (27 min) |
| 08 | 08_era_comparison.py | era_comparison/ (4 CSVs) | 🔄 Running (per-era design) |
| 09 | 09_repo_era_deep_dive.py | repo_era_deep_dive/ (9 CSVs) | ⏳ After Job 08 |

## Key Optimizations Applied

### Job 04 (top_repos_all)
- `shuffle.partitions` 400 → 200 (runtime 4.5h → 38 min)
- Hive bucketed table fallback (try/except)
- Supplement union with graceful fallback

### Job 07 (contributor_health)
- `gh.cache()` (3 scans → 1)
- `push_per_actor.cache()` (2 scans → 1)
- `result.count()` before write (triggers cache)
- `shuffle.partitions = 200`
- Native `isin()` instead of Python UDF

### Job 08 (era_comparison) — Major Redesign
- **Old design:** Union 5 eras (90 GB) → cache → 4 aggregations → YARN kill (exceeded 48 GB memory)
- **New design:** Per-era independent processing → each era 15-23 GB → cache fits → merge small results
- `approx_count_distinct` for repos/actors (1% error, faster)
- `countDistinct(pr_number)` to fix PR lifecycle double counting
- `eqNullSafe(True)` for null-safe boolean comparison
- Dynamic shuffle partitions: 24 for groupBy("era"), 200 for groupBy("era","repo_name")

### Job 09 (repo_era_deep_dive) — Redesign
- Expanded from 6 hardcoded repos to ~250 (seed AI + top 200 non-AI)
- Read cleaned parquet instead of deleted raw JSON
- Per-era processing (same pattern as Job 08)
- Added group-level AI vs Non-AI summaries

### Supplement Clean
- Split from single job (YARN 5h timeout) to monthly batches (30-40 min each)
- `ignoreCorruptFiles=true` to skip ~580 corrupt Apr files
- `--num-executors 4 --executor-cores 2 --executor-memory 6g` resource config

### All Clean Scripts
- Added `ignoreCorruptFiles=true` to 2022/2023/2024 Q1 clean scripts
- Handles truncated gzip files from interrupted downloads

## Key Findings (Slide 12)

### Health Score (Job 03)
1. **transformers is triple champion:** health_score 14.68, PyPI 9.38B, HF 834K models
2. **Hype ≠ Health:** Ollama 44K stars but 241 HF downloads; DeepSeek pure hype
3. **Quiet Powerhouse:** sentence-transformers 327K HF downloads per star
4. **Traditional ML vs LLM split:** scikit-learn 14.28B PyPI, 122 HF downloads
5. **Stars are worst health indicator:** top health repos rank #67-186 in stars

### Star Growth Hype (Job 06)
6. **40% have burst, only 0.2% extreme:** AI repos avg peak_ratio 2.10 vs non-AI 3.17

### Contributor Health (Job 07)
7. **Push concentration ≠ single maintainer risk:** must combine with pr_contributors
8. **Stars ≠ contributors:** deepseek-r1 93K actors but 5 push contributors
9. **Healthiest projects:** grafana (0.052), pytorch (0.140), transformers (0.191)

## Technical Issues Encountered

### YARN Resource Management
- **Lifetime limit:** 18000s (5 hours) — supplement clean and Job 08 exceeded this
- **Resource preemption:** Other users' jobs killed our executors
- **OOM on master node:** 8+ parallel downloads killed spark-submit client processes
- **Solution:** Sequential execution, conservative resource configs, per-era job design

### Data Quality
- **Corrupt gzip files:** 2026-04-06 to 04-30 (~580 files) — truncated during download
- **Placeholder file:** 2026-02-14-10.json.gz was empty placeholder → fixed
- **._COPYING_ residuals:** Hundreds of interrupted upload files in 2022 Q1 → cleaned
- **Solution:** `ignoreCorruptFiles=true` in all clean scripts

### Hive Metastore
- Bucketing job failed: "Could not connect to meta store" → Connection refused
- Fallback: Job 04 reads raw parquet instead of bucketed Hive table

## Files on Cluster (/tmp/)

### Scripts
- 00_bucket_gharchive_2025.py
- 00_clean_gharchive_supplement.py
- 00_clean_gharchive_2022q1.py / 2023q1 / 2024q1
- 00_clean_gharchive_year.py (generic)
- 00_clean_supplement_monthly.py / batch.sh
- 00_download_q1_fast.sh (8-way parallel)
- 04_top_repos_all.py
- 06_star_growth_hype.py
- 07_contributor_health.py
- 08_era_comparison.py (per-era design)
- 09_repo_era_deep_dive.py (250 repos, per-era)
- run_jobs_chain.sh
- run_overnight.sh (with safe_delete_raw)

### Local CSV Data (data/)
- top_repos_all.csv (57 KB, 1000 repos)
- ai_vs_general.csv (62 KB)
- star_growth_hype.csv (63 KB, 1000 repos)
- contributor_health.csv (51 KB, 1000 repos)
- health_score.csv (4 KB, 36 repos)
- hf_gh_join.csv (3 KB)

## Pending Work
1. Job 08 完成 → 下載 CSV → 分析 era findings
2. Job 09 跑 → 下載 CSV → 分析 repo deep-dive findings
3. 複製 2025 Q1 到自己的 HDFS（~18 GB，避免每次讀 334 天）
4. Tableau 視覺化
5. 簡報最終版
6. Commit + push

## Health Score Formula
```
health_score = log1p(HF_downloads) × 0.30
             + log1p(PyPI_downloads) × 0.20
             + log1p(GH_stars) × 0.15
             + log1p(GH_pushes) × 0.15
             + log1p(GH_PRs) × 0.10
             + log1p(GH_active_days) × 0.10
```
Extends OpenSSF Criticality Score with HF + PyPI adoption dimensions.

## HDFS Paths
```
/user/jl17797_nyu_edu/oss_pulse/
├── cleaned/
│   ├── gharchive_2022/          (Jan only, 4.3 GB — old, partial)
│   ├── gharchive_2022q1/        (90 days, 14.5 GB) ✅
│   ├── gharchive_2023q1/        (90 days, 17.3 GB) ✅
│   ├── gharchive_2024q1/        (91 days, 23.0 GB) ✅
│   └── gharchive_supplement/    (151 days, 27.3 GB) ✅
├── analytics/
│   ├── top_repos_all/           ✅
│   ├── top_repos_all_csv/       ✅
│   ├── contributor_health/      ✅
│   ├── contributor_health_csv/  ✅
│   ├── star_growth_hype/        ✅
│   ├── star_growth_hype_csv/    ✅
│   ├── ai_vs_general/           ✅
│   ├── ai_vs_general_csv/       ✅
│   ├── health_score/            ✅
│   ├── health_score_csv/        ✅ (missing? check)
│   ├── repo_era_deep_dive/      ✅ (old 2-era version)
│   └── era_comparison/          🔄 (Job 08 running)
├── source/
│   ├── huggingface_hub/         (229 MB)
│   ├── pypi/                    (38.5 KB)
│   └── seed_repos.json          (8.8 KB)
└── raw/                         (376.8 MB, HF/PyPI)

/user/by2566_nyu_edu/oss_pulse/
└── cleaned/gharchive/2025/      (334 days, 67.5 GB) — read-only
```
