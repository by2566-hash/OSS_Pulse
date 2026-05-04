#!/usr/bin/env bash
DST="/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_2026q1"
SRC="/user/jl17797_nyu_edu/oss_pulse/cleaned/gharchive_supplement"
hdfs dfs -mkdir -p "$DST"
for month in 01 02 03; do
  case $month in 01) days=31;; 02) days=28;; 03) days=31;; esac
  for d in $(seq 1 $days); do
    day=$(printf "%02d" $d)
    hdfs dfs -test -d "${DST}/event_date=2026-${month}-${day}" 2>/dev/null && continue
    hdfs dfs -cp "${SRC}/event_date=2026-${month}-${day}" "${DST}/" && echo "OK: 2026-${month}-${day}"
  done
done
echo "DONE"
