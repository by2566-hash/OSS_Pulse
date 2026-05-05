#!/bin/bash
# Orchestrator: download → clean → verify 2026 Q1
# Run: nohup bash /tmp/run_verify_2026q1.sh > /tmp/verify_2026q1_main.log 2>&1 &

echo "=== [$(date)] Step 1: Download raw 2026 Q1 ==="
bash /tmp/00_download_2026q1_raw.sh 2>&1 | tee /tmp/download_2026q1.log
echo "=== [$(date)] Download complete ==="

# Check file count
COUNT=$(hdfs dfs -ls /user/jl17797_nyu_edu/oss_pulse/source/gharchive_2026q1_raw/ 2>/dev/null | grep -c ".json.gz")
echo "Files downloaded: $COUNT / 2160"

if [ "$COUNT" -lt 2000 ]; then
  echo "[WARN] Only $COUNT files downloaded, expected ~2160. Proceeding anyway."
fi

echo ""
echo "=== [$(date)] Step 2: Clean & Verify ==="
spark-submit \
  --driver-memory 8g \
  --executor-memory 12g \
  --num-executors 4 \
  --executor-cores 4 \
  /tmp/00_clean_and_verify_2026q1.py 2>&1 | tee /tmp/verify_2026q1_spark.log

echo ""
echo "=== [$(date)] All steps complete ==="
echo "Check logs:"
echo "  Download: /tmp/download_2026q1.log"
echo "  Spark:    /tmp/verify_2026q1_spark.log"
echo "  Main:     /tmp/verify_2026q1_main.log"
