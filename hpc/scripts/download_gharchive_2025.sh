#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HPC_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${1:-${HPC_DIR}/env/oss_pulse_2025.env}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Environment file not found: ${ENV_FILE}" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"

YEAR="${YEAR:-2025}"
mkdir -p "${LOCAL_SOURCE_DIR}" "${LOCAL_LOG_DIR}"

generate_hourly_paths() {
  python3 - <<PY
from datetime import datetime, timedelta

year = int("${YEAR}")
start = datetime(year, 1, 1, 0)
end = datetime(year, 12, 31, 23)
current = start
while current <= end:
    print(f"{current:%Y-%m-%d}-{current.hour}.json.gz")
    current += timedelta(hours=1)
PY
}

download_one() {
  local file_name="$1"
  local url="https://data.gharchive.org/${file_name}"
  local output_path="${LOCAL_SOURCE_DIR}/${file_name}"

  echo "Downloading ${file_name}"
  curl -fL --retry 5 --retry-delay 2 -C - -o "${output_path}" "${url}"
}

export LOCAL_SOURCE_DIR
export -f download_one

generate_hourly_paths | xargs -n 1 -P "${DOWNLOAD_JOBS}" -I {} bash -c 'download_one "$@"' _ {}

echo "Download complete: ${LOCAL_SOURCE_DIR}"
