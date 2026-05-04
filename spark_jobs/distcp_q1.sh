#!/usr/bin/env bash
# Fast parallel copy of 2025 Q1 and 2026 Q1 using distcp
# Usage: bash /tmp/distcp_q1.sh

set -uo pipefail

echo "[INFO] Building 2025 Q1 source list..."
SRC_2025="/tmp/distcp_2025q1_srcs.txt"
> "$SRC_2025"
for month in 01 02 03; do
  case $month in 01) days=31;; 02) days=28;; 03) days=31;; esac
  for d in $(seq 1 $days); do
    day=$(printf "%02d" $d)
    echo "/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025/event_date=2025-${month}-${day}" >> "$SRC_2025"
  done
done

echo "[INFO] Copying 2025 Q1 ($(wc -l < $SRC_2025) dirs)..."
hdfs dfs -mkdir -p /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2025q1
hadoop distcp -m 10 -overwrite \
  -f "$SRC_2025" \
  /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2025q1/
echo "[INFO] 2025 Q1 done."

echo "[INFO] Building 2026 Q1 source list..."
SRC_2026="/tmp/distcp_2026q1_srcs.txt"
> "$SRC_2026"
for month in 01 02 03; do
  case $month in 01) days=31;; 02) days=28;; 03) days=31;; esac
  for d in $(seq 1 $days); do
    day=$(printf "%02d" $d)
    echo "/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement/event_date=2026-${month}-${day}" >> "$SRC_2026"
  done
done

echo "[INFO] Copying 2026 Q1 ($(wc -l < $SRC_2026) dirs)..."
hdfs dfs -mkdir -p /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1
hadoop distcp -m 10 -overwrite \
  -f "$SRC_2026" \
  /user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1/
echo "[INFO] 2026 Q1 done."

echo "[DONE] Both Q1 datasets copied."
