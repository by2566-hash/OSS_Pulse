#!/usr/bin/env bash
# ============================================================
# 00_clean_supplement_batch.sh
# Clean supplement data month by month to avoid YARN lifetime timeout.
#
# Supplement covers: 2025-12, 2026-01, 2026-02, 2026-03, 2026-04
# Each month ~700 files (~17 GB raw) → completes in ~30-40 min
#
# Usage:
#   nohup bash /tmp/00_clean_supplement_batch.sh > /tmp/supp_batch.log 2>&1 &
# ============================================================

set -euo pipefail

MONTHS="2026-02 2026-03 2026-04"
SCRIPT="/tmp/00_clean_supplement_monthly.py"
OUT_LOG_DIR="/tmp"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

for month in $MONTHS; do
    log "=== Cleaning $month (mode: append) ==="
    spark-submit \
        --num-executors 4 \
        --executor-cores 2 \
        --executor-memory 6g \
        --driver-memory 4g \
        "$SCRIPT" "$month" "append" \
        > "${OUT_LOG_DIR}/supp_${month}.log" 2>&1 \
        && log "$month OK" \
        || { log "ERROR: $month failed. Check ${OUT_LOG_DIR}/supp_${month}.log"; exit 1; }
done

log "=== ALL 5 MONTHS COMPLETE ==="
log "Output: /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement"
