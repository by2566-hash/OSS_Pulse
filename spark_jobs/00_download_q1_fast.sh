#!/usr/bin/env bash
# Fast parallel Q1 download for a given year
# Usage: bash /tmp/00_download_q1_fast.sh 2023
#        bash /tmp/00_download_q1_fast.sh 2024

set -uo pipefail

YEAR="${1:?Usage: $0 <year>}"
HDFS_BASE="/user/jl17797_nyu_edu/oss_pulse/source/gharchive_${YEAR}q1"
GH_BASE="https://data.gharchive.org"
PARALLEL=4

# Determine days per month (handle leap year)
is_leap() { (( ($1 % 4 == 0 && $1 % 100 != 0) || $1 % 400 == 0 )); }
feb_days=28
is_leap "$YEAR" && feb_days=29

log() { echo "[$(date '+%H:%M:%S')] $*"; }

hdfs dfs -mkdir -p "$HDFS_BASE"

# Build list of existing files (one HDFS call)
EXISTING="/tmp/existing_${YEAR}q1.txt"
hdfs dfs -ls "$HDFS_BASE" 2>/dev/null | awk '{print $NF}' | sed 's|.*/||' | sort > "$EXISTING"
existing_count=$(wc -l < "$EXISTING")
log "Existing files: $existing_count"

download_day() {
    local date_str="$1"
    for h in $(seq 0 23); do
        local fname="${date_str}-${h}.json.gz"
        if grep -qF "$fname" "$EXISTING" 2>/dev/null; then
            continue
        fi
        local url="${GH_BASE}/${fname}"
        local http_code
        http_code=$(curl -s -o /dev/null -w "%{http_code}" "$url")
        if [[ "$http_code" != "200" ]]; then
            log "[SKIP] $fname (HTTP $http_code)"
            continue
        fi
        curl -sf "$url" | hdfs dfs -put -f - "${HDFS_BASE}/${fname}" \
            && log "[DONE] $fname" \
            || log "[FAIL] $fname"
    done
}

export -f download_day log
export GH_BASE HDFS_BASE EXISTING

for month_info in "01 31" "02 $feb_days" "03 31"; do
    month="${month_info%% *}"
    days="${month_info##* }"
    log "=== ${YEAR}-${month} (${days} days, ${PARALLEL} parallel) ==="

    for day in $(seq 1 "$days"); do
        printf "${YEAR}-${month}-%02d\n" "$day"
    done | xargs -P"$PARALLEL" -I{} bash -c 'download_day "$@"' _ {}

    log "=== ${YEAR}-${month} done ==="
done

final_count=$(hdfs dfs -count "$HDFS_BASE" 2>/dev/null | awk '{print $2}')
log "COMPLETE. Total files: $final_count"
