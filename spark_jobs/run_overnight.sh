#!/usr/bin/env bash
# Automated pipeline while away
# Usage: nohup bash /tmp/run_overnight.sh > /tmp/overnight.log 2>&1 &

set -uo pipefail

SPARK="spark-submit --num-executors 6 --executor-cores 3 --executor-memory 8g --driver-memory 4g"
HDFS_BASE="/user/jl17797_nyu_edu/oss_pulse"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

safe_delete_raw() {
    local raw_path="$1" cleaned_path="$2" label="$3"
    if hdfs dfs -test -d "$cleaned_path" 2>/dev/null; then
        local dir_count
        dir_count=$(hdfs dfs -count "$cleaned_path" 2>/dev/null | awk '{print $1}')
        if [ "$dir_count" -gt 5 ] 2>/dev/null; then
            log "$label cleaned OK ($dir_count dirs). Deleting raw..."
            hdfs dfs -rm -r "$raw_path"
            log "$label raw deleted."
        else
            log "ERROR: $label cleaned exists but only $dir_count dirs. Skipping delete."
            return 1
        fi
    else
        log "ERROR: $label cleaned output not found at $cleaned_path. Skipping delete."
        return 1
    fi
}

# 1. Wait for 2023 Q1 download to finish (2160 files)
log "Waiting for 2023 Q1 download (target: 2160 files)..."
while true; do
    count=$(hdfs dfs -count "$HDFS_BASE/source/gharchive_2023q1" 2>/dev/null | awk '{print $2}')
    log "2023 Q1: $count / 2160"
    [ "$count" -ge 2160 ] 2>/dev/null && break
    sleep 120
done
log "2023 Q1 download complete."

# 2. Clean 2023 Q1
log "Cleaning 2023 Q1..."
$SPARK /tmp/00_clean_gharchive_2023q1.py > /tmp/clean_2023q1.log 2>&1
if [ $? -ne 0 ]; then
    log "ERROR: 2023 Q1 clean failed. Check /tmp/clean_2023q1.log"
    exit 1
fi
safe_delete_raw "$HDFS_BASE/source/gharchive_2023q1" "$HDFS_BASE/cleaned/gharchive_2023q1" "2023 Q1"

# 3. Start 2024 Q1 download + wait + clean
log "Starting 2024 Q1 download..."
bash /tmp/00_download_q1_fast.sh 2024 > /tmp/download_2024q1_fast.log 2>&1
log "2024 Q1 download complete."

log "Cleaning 2024 Q1..."
$SPARK /tmp/00_clean_gharchive_2024q1.py > /tmp/clean_2024q1.log 2>&1
if [ $? -ne 0 ]; then
    log "ERROR: 2024 Q1 clean failed. Check /tmp/clean_2024q1.log"
    exit 1
fi
safe_delete_raw "$HDFS_BASE/source/gharchive_2024q1" "$HDFS_BASE/cleaned/gharchive_2024q1" "2024 Q1"

log "=== ALL DONE ==="
log "Ready for Job 08 (era comparison)"
