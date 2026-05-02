# OSS Pulse — Pipeline Runbook
_Group 9 | jl17797 (Jhe Chen Li) + by2566 (Bo Yu)_
_Last updated: 2026-05-02_

## 環境

| 項目 | 值 |
|------|-----|
| Cluster | NYU Dataproc, `nyu-dataproc-m` |
| Project | `hpc-dataproc-19b8` |
| Zone | `us-central1-f` |
| SSH alias | `dp "<cmd>"` (見 ~/.zshrc) |
| jl17797 HDFS | `/user/jl17797_nyu_edu/oss_pulse/` |
| by2566 HDFS | `/user/by2566_nyu_edu/oss_pulse/` |
| BigQuery (PyPI) | project `oss-pulse-leeggroy` (leeggroy@gmail.com) |

---

## HDFS 目錄結構

```
/user/jl17797_nyu_edu/oss_pulse/
├── source/
│   ├── huggingface_hub/        # HF raw JSONL (1.7GB gzipped)
│   ├── pypi/
│   │   └── pypi_monthly_downloads.jsonl   # 46 libraries, 2025 monthly
│   └── seed_repos.json         # 93 AI/ML repos seed list
│
├── raw/
│   └── huggingface_hub/        # HF raw Parquet
│
├── cleaned/
│   └── huggingface_hub/        # HF cleaned Parquet (2,815,064 rows)
│
└── analytics/
    ├── repo_daily_metrics/     # Job 01 output
    ├── hf_gh_join/             # Job 02 output
    ├── health_score/           # Job 03 output
    ├── top_repos_all/          # Job 04 output
    ├── ai_vs_general/          # Job 05 output
    ├── star_growth_hype/       # Job 06 output
    └── contributor_health/     # Job 07 output

/user/by2566_nyu_edu/oss_pulse/
└── cleaned/
    └── gharchive/2025/         # 334 days, partitioned by event_date, 67.5 GB
```

---

## 如何執行 Spark Job

### 從本機上傳並執行

```bash
# 上傳 script
gcloud compute scp spark_jobs/<script>.py nyu-dataproc-m:/tmp/<script>.py \
  --project=hpc-dataproc-19b8 --zone=us-central1-f

# 執行（結果存到 /tmp/out.txt）
dp "spark-submit /tmp/<script>.py > /tmp/out.txt 2>&1; echo EXIT:\$?"

# 查看輸出
dp "tail -50 /tmp/out.txt"

# 查看 HDFS 結果
dp "hdfs dfs -ls /user/jl17797_nyu_edu/oss_pulse/analytics/<output>/"
dp "hdfs dfs -cat /user/jl17797_nyu_edu/oss_pulse/analytics/<output>_csv/*.csv"
```

### 下載結果到本機

```bash
dp "hdfs dfs -getmerge /user/jl17797_nyu_edu/oss_pulse/analytics/<output>_csv /tmp/<output>.csv"
gcloud compute scp nyu-dataproc-m:/tmp/<output>.csv data/<output>.csv \
  --project=hpc-dataproc-19b8 --zone=us-central1-f
```

---

## Jobs 執行順序與狀態

| Job | Script | Input | Output | 狀態 |
|-----|--------|-------|--------|------|
| 01 | `01_repo_daily_metrics.py` | GH Archive (by2566) + seed | `repo_daily_metrics` | ✅ 完成 |
| 02 | `02_hf_gh_join.py` | HF cleaned + Job01 + seed | `hf_gh_join` | ✅ 完成 |
| 03 | `03_health_score.py` | Job02 + PyPI JSONL | `health_score` | ✅ 完成 |
| 04 | `04_top_repos_all.py` | GH Archive (by2566) ALL + supplement | `top_repos_all` | ⬜ 待執行 |
| 05 | `05_ai_vs_general.py` | Job04 + seed | `ai_vs_general` | ⬜ 待執行（需 Job04）|
| 06 | `06_star_growth_hype.py` | GH Archive + supplement + Job04 + seed | `star_growth_hype` | ⬜ 待執行（需 Job04）|
| 07 | `07_contributor_health.py` | GH Archive + supplement + Job04 + seed | `contributor_health` | ⬜ 待執行（需 Job04）|
| 00a | `00_download_gharchive_supplement.sh` | GH Archive website (HTTP) | `source/gharchive_raw/` | 🔄 執行中（補 2025-12 ~ 2026-04）|
| 00b | `00_clean_gharchive_supplement.py` | `source/gharchive_raw/` | `cleaned/gharchive_supplement/` | ⬜ 待執行（需 00a）|

---

## 本機資料

| 檔案 | 說明 |
|------|------|
| `data/seed_repos.json` | 93 AI/ML repo seed list |
| `data/hf_gh_join.csv` | HF + GH join 結果 (36 repos) |
| `data/health_score.csv` | 三方合併 + health score |
| `data/source/pypi_monthly_downloads.jsonl` | PyPI 月下載量 |

---

## PyPI BigQuery 查詢方式

```bash
# 切換到個人帳號
gcloud config set account leeggroy@gmail.com
gcloud config set project oss-pulse-leeggroy2  # 第二個帳號額度較多

# 查詢
bq query --project_id=oss-pulse-leeggroy2 --use_legacy_sql=false \
  --format=json --max_rows=10000 \
  'SELECT file.project, DATE_TRUNC(DATE(timestamp), MONTH) as month, COUNT(*) as downloads
   FROM `bigquery-public-data.pypi.file_downloads`
   WHERE DATE(timestamp) BETWEEN "2025-01-01" AND "2025-12-31"
     AND file.project IN ("transformers", ...)
   GROUP BY file.project, month'

# 切回 NYU 帳號
gcloud config set account jl17797@nyu.edu
gcloud config set project hpc-dataproc-19b8
```

---

---

## GH Archive 資料補充（2025-12-01 ~ 2026-04-30）

目的：讓 GH 數據時間範圍對齊 HF 數據（April 2026 snapshot）。

### 步驟

```bash
# 1. 上傳 scripts
gcloud compute scp spark_jobs/00_download_gharchive_supplement.sh nyu-dataproc-m:/tmp/ \
  --project=hpc-dataproc-19b8 --zone=us-central1-f
gcloud compute scp spark_jobs/00_clean_gharchive_supplement.py nyu-dataproc-m:/tmp/ \
  --project=hpc-dataproc-19b8 --zone=us-central1-f

# 2. 在 Dataproc 背景下載（約 151 天 × 24 小時 = 3624 個 .json.gz）
dp "nohup bash /tmp/00_download_gharchive_supplement.sh > /tmp/dl_supp.log 2>&1 &"

# 3. 監控下載進度
dp "tail -20 /tmp/dl_supp.log"
dp "hdfs dfs -ls /user/jl17797_nyu_edu/oss_pulse/source/gharchive_raw/ | wc -l"

# 4. 等下載完成後執行清理 Job
dp "spark-submit /tmp/00_clean_gharchive_supplement.py > /tmp/00b_out.txt 2>&1; echo EXIT:\$?"
dp "tail -30 /tmp/00b_out.txt"

# 5. 確認輸出
dp "hdfs dfs -ls /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement/"
```

### 說明
- `download_supplement.sh`：用 curl pipe 直接 stream 到 HDFS，不佔用 /tmp 空間
- `09_clean_supplement.py`：standalone（schema 已 inline），不依賴 schemas/ utils/
- Jobs 04/06/07 已更新：優先 union supplement，若不存在則 fallback 用 2025 data

### HDFS 路徑
| 用途 | 路徑 |
|------|------|
| 下載的 raw JSON.gz | `/user/jl17797_nyu_edu/oss_pulse/source/gharchive_raw/` |
| 清理後 Parquet | `/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement/` |

---

## 注意事項

- GH Archive 資料在 by2566 的目錄，需要 ACL 權限（已開通）
- BigQuery 免費額度：`leeggroy@gmail.com` 已用約 233GB，`leeggroy2@gmail.com` 剩餘較多
- `dp` alias = `gcloud compute ssh nyu-dataproc-m --project=hpc-dataproc-19b8 --zone=us-central1-f --command`
