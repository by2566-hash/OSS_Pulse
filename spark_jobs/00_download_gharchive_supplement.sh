#!/usr/bin/env bash
# ============================================================
# download_supplement.sh
# Download GH Archive hourly files (2025-12-01 to 2026-04-30)
# and stream them directly into HDFS via stdin pipe.
#
# Usage (run on nyu-dataproc-m):
#   nohup bash /tmp/download_supplement.sh > /tmp/download_supplement.log 2>&1 &
#
# Files land at:
#   /user/jl17797_nyu_edu/oss_pulse/source/gharchive_raw/YYYY-MM-DD-H.json.gz
# ============================================================

set -euo pipefail

HDFS_DEST="/user/jl17797_nyu_edu/oss_pulse/source/gharchive_raw"
GH_BASE="https://data.gharchive.org"

hdfs dfs -mkdir -p "$HDFS_DEST"

# Generate all dates from 2025-12-01 to 2026-04-30
START="2025-12-01"
END="2026-04-30"
cur="$START"

while [[ ! "$cur" > "$END" ]]; do
    for hour in $(seq 0 23); do
        fname="${cur}-${hour}.json.gz"
        hdfs_path="${HDFS_DEST}/${fname}"

        # Skip if already downloaded
        if hdfs dfs -test -e "$hdfs_path" 2>/dev/null; then
            echo "[SKIP] $fname already in HDFS"
            continue
        fi

        url="${GH_BASE}/${fname}"
        http_code=$(curl -s -o /dev/null -w "%{http_code}" "$url")

        if [[ "$http_code" != "200" ]]; then
            echo "[WARN] HTTP $http_code for $url — skipping"
            continue
        fi

        echo "[DOWN] $fname"
        curl -s "$url" | hdfs dfs -put -f - "$hdfs_path"
        echo "[DONE] $fname → $hdfs_path"
    done

    # Advance date by 1 day
    cur=$(date -d "$cur + 1 day" +%Y-%m-%d 2>/dev/null \
          || python3 -c "
from datetime import date, timedelta
d = date.fromisoformat('$cur') + timedelta(days=1)
print(d.isoformat())")
done

echo "=== All downloads complete ==="
