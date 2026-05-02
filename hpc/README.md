# OSS Pulse — Dataproc Cluster Config

此目錄存放 NYU Dataproc 連線所需的環境設定範本。

## 環境設定

```bash
cp hpc/env/oss_pulse_2025.env.example hpc/env/oss_pulse_2025.env
# 編輯 oss_pulse_2025.env，填入個人帳號與路徑
```

## Cluster 執行方式

所有 Spark jobs 已統一移至 `spark_jobs/`，執行方式請參考：

- `logs/pipeline_runbook.md` — 完整執行順序與 HDFS 路徑
- `spark_jobs/00_download_gharchive_supplement.sh` — 資料下載（直接 stream 到 HDFS）
- `spark_jobs/00_clean_gharchive_supplement.py` — 資料清理

## SSH 快速指令

```bash
# 加入 ~/.zshrc
alias dp='gcloud compute ssh nyu-dataproc-m --project=hpc-dataproc-19b8 --zone=us-central1-f --command'

# 上傳 script
gcloud compute scp spark_jobs/<script> nyu-dataproc-m:/tmp/ \
  --project=hpc-dataproc-19b8 --zone=us-central1-f

# 執行
dp "spark-submit /tmp/<script>.py > /tmp/out.txt 2>&1; echo EXIT:\$?"
```
