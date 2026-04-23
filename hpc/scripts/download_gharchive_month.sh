#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HPC_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${1:-${HPC_DIR}/env/oss_pulse_2025.env}"
MONTH="${2:-}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Environment file not found: ${ENV_FILE}" >&2
  exit 1
fi

if [[ -z "${MONTH}" ]]; then
  echo "Usage: $0 <env-file> <YYYY-MM>" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"

MONTH_DIR="${LOCAL_SOURCE_DIR}/${MONTH}"
mkdir -p "${MONTH_DIR}" "${LOCAL_LOG_DIR}"

generate_monthly_paths() {
  python3 - "${MONTH}" <<'PY'
from datetime import datetime, timedelta
import sys

month = sys.argv[1]
start = datetime.strptime(month + "-01", "%Y-%m-%d")
if start.month == 12:
    end = datetime(start.year + 1, 1, 1)
else:
    end = datetime(start.year, start.month + 1, 1)

current = start
while current < end:
    print(f"{current:%Y-%m-%d}-{current.hour}.json.gz")
    current += timedelta(hours=1)
PY
}

download_one() {
  local file_name="$1"
  local url="https://data.gharchive.org/${file_name}"
  local output_path="${MONTH_DIR}/${file_name}"

  echo "Downloading ${file_name}"
  curl -fL --retry 5 --retry-delay 2 -C - -o "${output_path}" "${url}"
}

export MONTH_DIR
export -f download_one

generate_monthly_paths | xargs -n 1 -P "${DOWNLOAD_JOBS}" -I {} bash -c 'download_one "$@"' _ {}

echo "Monthly download complete: ${MONTH_DIR}"
