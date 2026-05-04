#!/usr/bin/env bash
DST="/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2025q1"
SRC="/user/by2566_nyu_edu/oss_pulse/cleaned/gharchive/2025"
hdfs dfs -mkdir -p "$DST"
for month in 01 02 03; do
  case $month in 01) days=31;; 02) days=28;; 03) days=31;; esac
  for d in $(seq 1 $days); do
    day=$(printf "%02d" $d)
    hdfs dfs -cp "${SRC}/event_date=2025-${month}-${day}" "${DST}/" 2>/dev/null && echo "OK: 2025-${month}-${day}"
  done
done
echo "DONE"
