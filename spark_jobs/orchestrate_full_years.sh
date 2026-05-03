#!/usr/bin/env bash
# ============================================================
# orchestrate_full_years.sh
# Rolling pipeline for full-year GH Archive data (2022–2024)
# Downloads month-by-month to stay within 500 GB HDFS quota.
#
# Each month:  ~50 GB raw  →  Spark clean  →  ~6 GB parquet  →  delete raw
# Net per month: only ~6 GB stays on HDFS after cleanup.
#
# Usage (run on nyu-dataproc-m):
#   nohup bash /tmp/orchestrate_full_years.sh > /tmp/orchestrate_years.log 2>&1 &
#
# Prerequisites:
#   /tmp/00_clean_gharchive_year.py   (generic cleaner)
#   /tmp/00_download_gharchive_supplement.sh  (for 2026 data, already handled)
# ============================================================

set -euo pipefail

HDFS_BASE="/user/jl17797_nyu_edu/oss_pulse"
GH_BASE="https://data.gharchive.org"
YEARS="2022 2023 2024"   # 2025 = by2566, 2026 = supplement

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

hdfs_count() { hdfs dfs -count "$1" 2>/dev/null | awk '{print $2}' || echo 0; }

days_in_month() {
    python3 -c "
import calendar
print(calendar.monthrange($1, $2)[1])
"
}

# Download one month, stream directly to HDFS
download_month() {
    local year="$1" month="$2"
    local days; days=$(days_in_month "$year" "$month")
    local dest="$HDFS_BASE/source/gharchive_${year}/month_${month}"
    local existing_list="/tmp/gharchive_${year}_${month}_existing.txt"

    hdfs dfs -mkdir -p "$dest"
    hdfs dfs -ls "$dest" 2>/dev/null | awk '{print $NF}' | sed 's|.*/||' | sort > "$existing_list"
    log "[$year-$(printf '%02d' $month)] Existing: $(wc -l < "$existing_list") files"

    local cur
    cur=$(python3 -c "from datetime import date; print(date($year, $month, 1).isoformat())")
    local end
    end=$(python3 -c "
from datetime import date
import calendar
d = date($year, $month, calendar.monthrange($year, $month)[1])
print(d.isoformat())")

    while [[ ! "$cur" > "$end" ]]; do
        for hour in $(seq 0 23); do
            local fname="${cur}-${hour}.json.gz"
            if grep -qF "$fname" "$existing_list" 2>/dev/null; then
                continue
            fi
            local url="${GH_BASE}/${fname}"
            local http_code
            http_code=$(curl -s -o /dev/null -w "%{http_code}" "$url")
            if [[ "$http_code" != "200" ]]; then
                log "[WARN] HTTP $http_code for $fname — skipping"
                continue
            fi
            log "[DOWN] $fname"
            curl -s "$url" | hdfs dfs -put -f - "${dest}/${fname}"
        done
        cur=$(python3 -c "
from datetime import date, timedelta
d = date.fromisoformat('$cur') + timedelta(days=1)
print(d.isoformat())")
    done
    log "[$year-$(printf '%02d' $month)] Download complete."
}

# Clean one month then delete raw
clean_and_free_month() {
    local year="$1" month="$2"
    local src="$HDFS_BASE/source/gharchive_${year}/month_${month}"
    local out="$HDFS_BASE/cleaned/gharchive_${year}"

    log "[$year-$(printf '%02d' $month)] Cleaning..."
    spark-submit \
        --conf "spark.sql.shuffle.partitions=200" \
        /tmp/00_clean_gharchive_year.py "${year}/month_${month}" \
        > "/tmp/clean_${year}_${month}.txt" 2>&1 \
        && log "[$year-$(printf '%02d' $month)] Clean OK" \
        || { log "ERROR: clean failed. See /tmp/clean_${year}_${month}.txt"; exit 1; }

    log "[$year-$(printf '%02d' $month)] Deleting raw..."
    hdfs dfs -rm -r "$src"
    log "[$year-$(printf '%02d' $month)] Raw deleted. ~50 GB freed."
}

# ── Main loop ─────────────────────────────────────────────────────────────────

for year in $YEARS; do
    log "=== Starting $year ==="
    for month in $(seq 1 12); do
        log "--- $year-$(printf '%02d' $month) ---"
        download_month "$year" "$month"
        clean_and_free_month "$year" "$month"
    done
    log "=== $year complete. Cleaned: $HDFS_BASE/cleaned/gharchive_${year}/ ==="
done

log "ALL YEARS COMPLETE: 2022, 2023, 2024 fully cleaned."
log "Combined with by2566 2025 + supplement 2026, era comparison ready."
