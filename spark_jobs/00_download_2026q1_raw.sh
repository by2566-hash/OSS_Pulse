#!/bin/bash
# Download raw 2026 Q1 GH Archive data to HDFS
# Run on master node: nohup bash /tmp/00_download_2026q1_raw.sh > /tmp/download_2026q1.log 2>&1 &

RAW_DIR="/user/jl17797_nyu_edu/oss_pulse/source/gharchive_2026q1_raw"
hdfs dfs -mkdir -p "$RAW_DIR"

TOTAL=0
FAIL=0
SKIP=0

for month in 01 02 03; do
  case $month in
    01|03) days=31 ;;
    02) days=28 ;;
  esac
  for day in $(seq -w 1 $days); do
    for hour in $(seq 0 23); do
      FILE="2026-${month}-${day}-${hour}.json.gz"
      HDFS_PATH="${RAW_DIR}/${FILE}"

      # Skip if already downloaded
      if hdfs dfs -test -e "$HDFS_PATH" 2>/dev/null; then
        SKIP=$((SKIP + 1))
        continue
      fi

      echo "[$(date +%H:%M:%S)] Downloading $FILE..."
      curl -sL --retry 3 --retry-delay 5 "https://data.gharchive.org/${FILE}" \
        | hdfs dfs -put - "$HDFS_PATH" 2>/dev/null

      if [ $? -eq 0 ]; then
        TOTAL=$((TOTAL + 1))
      else
        echo "[WARN] Failed: $FILE"
        FAIL=$((FAIL + 1))
      fi
    done
    echo "[INFO] Done 2026-${month}-${day} | downloaded=$TOTAL skipped=$SKIP failed=$FAIL"
  done
done

echo ""
echo "[DONE] Total downloaded: $TOTAL, Skipped: $SKIP, Failed: $FAIL"
echo "[INFO] Verifying file count..."
COUNT=$(hdfs dfs -ls "$RAW_DIR" | grep -c ".json.gz")
echo "[INFO] Files on HDFS: $COUNT / 2160 expected"
