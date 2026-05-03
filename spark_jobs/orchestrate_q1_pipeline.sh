#!/usr/bin/env bash
# ============================================================
# orchestrate_q1_pipeline.sh
# Rolling pipeline: download → clean → delete raw
#
# Execution order (maximises space efficiency):
#   1. supplement (2025-12 ~ 2026-04): clean → delete raw  → free ~123 GB
#   2. 2022 Q1:                        clean → delete raw  → free ~151 GB
#   3. 2023 Q1: download → clean → delete raw              → free ~235 GB
#   4. 2024 Q1: download → clean → delete raw              → free ~157 GB
#
# Usage (run on nyu-dataproc-m):
#   nohup bash /tmp/orchestrate_q1_pipeline.sh > /tmp/orchestrate.log 2>&1 &
# ============================================================

set -euo pipefail

HDFS_BASE="/user/jl17797_nyu_edu/oss_pulse"
Q1_TARGET=2160      # 90 days × 24 hours
SUPP_TARGET=3624    # 151 days × 24 hours

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

hdfs_count() {
    hdfs dfs -count "$1" 2>/dev/null | awk '{print $2}' || echo 0
}

wait_for_download() {
    local dir="$1" target="$2" label="$3"
    log "Waiting for $label ($target files)..."
    while true; do
        local count; count=$(hdfs_count "$dir")
        log "$label: $count / $target"
        [[ "$count" -ge "$target" ]] && { log "$label complete."; break; }
        sleep 120
    done
}

run_clean() {
    local script="$1" label="$2"
    log "Cleaning $label..."
    spark-submit "/tmp/$script" > "/tmp/${script%.py}_out.txt" 2>&1 \
        && log "$label clean OK" \
        || { log "ERROR: $label clean failed. Check /tmp/${script%.py}_out.txt"; exit 1; }
}

delete_raw() {
    local dir="$1" label="$2"
    log "Deleting $label raw: $dir"
    hdfs dfs -rm -r "$dir" && log "$label raw deleted."
}

start_download_if_needed() {
    local dir="$1" target="$2" script="$3" logfile="$4" label="$5"
    local count; count=$(hdfs_count "$dir")
    if [[ "$count" -lt "$target" ]]; then
        log "Starting $label download (currently $count files)..."
        nohup bash "/tmp/$script" >> "$logfile" 2>&1 &
        log "$label download PID: $!"
    else
        log "$label already complete ($count files), skipping download."
    fi
}

# ── PHASE 1: Supplement (最先清，釋放 ~123 GB) ────────────────────────────────
SUPP_RAW="$HDFS_BASE/source/gharchive_raw"
SUPP_CLEAN="$HDFS_BASE/cleaned/gharchive_supplement"

start_download_if_needed \
    "$SUPP_RAW" "$SUPP_TARGET" \
    "00_download_gharchive_supplement.sh" "/tmp/download_supp.log" "supplement"

wait_for_download "$SUPP_RAW" "$SUPP_TARGET" "supplement"
run_clean "00_clean_gharchive_supplement.py" "supplement"
delete_raw "$SUPP_RAW" "supplement"
log "PHASE 1 done. Supplement cleaned → raw deleted (~123 GB freed)."

# ── PHASE 2: 2022 Q1 (clean 已下載的 raw，釋放 ~151 GB) ──────────────────────
RAW_2022="$HDFS_BASE/source/gharchive_2022q1"

start_download_if_needed \
    "$RAW_2022" "$Q1_TARGET" \
    "00_download_gharchive_2022q1.sh" "/tmp/download_2022q1.log" "2022-Q1"

wait_for_download "$RAW_2022" "$Q1_TARGET" "2022-Q1"
run_clean "00_clean_gharchive_2022q1.py" "2022-Q1"
delete_raw "$RAW_2022" "2022-Q1"
log "PHASE 2 done. 2022-Q1 cleaned → raw deleted (~151 GB freed)."

# ── PHASE 3: 2023 Q1 ──────────────────────────────────────────────────────────
RAW_2023="$HDFS_BASE/source/gharchive_2023q1"

start_download_if_needed \
    "$RAW_2023" "$Q1_TARGET" \
    "00_download_gharchive_2023q1.sh" "/tmp/download_2023q1.log" "2023-Q1"

wait_for_download "$RAW_2023" "$Q1_TARGET" "2023-Q1"
run_clean "00_clean_gharchive_2023q1.py" "2023-Q1"
delete_raw "$RAW_2023" "2023-Q1"
log "PHASE 3 done. 2023-Q1 cleaned → raw deleted."

# ── PHASE 4: 2024 Q1 ──────────────────────────────────────────────────────────
RAW_2024="$HDFS_BASE/source/gharchive_2024q1"

start_download_if_needed \
    "$RAW_2024" "$Q1_TARGET" \
    "00_download_gharchive_2024q1.sh" "/tmp/download_2024q1.log" "2024-Q1"

wait_for_download "$RAW_2024" "$Q1_TARGET" "2024-Q1"
run_clean "00_clean_gharchive_2024q1.py" "2024-Q1"
delete_raw "$RAW_2024" "2024-Q1"
log "PHASE 4 done. 2024-Q1 cleaned → raw deleted."

# ── Summary ───────────────────────────────────────────────────────────────────
log "=== ALL PHASES COMPLETE ==="
log "Cleaned outputs ready for Job 08:"
log "  $HDFS_BASE/cleaned/gharchive_supplement/   (2026-Q1 subset)"
log "  $HDFS_BASE/cleaned/gharchive_2022q1/"
log "  $HDFS_BASE/cleaned/gharchive_2023q1/"
log "  $HDFS_BASE/cleaned/gharchive_2024q1/"
log "  by2566: /user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/ (2025-Q1 subset)"
