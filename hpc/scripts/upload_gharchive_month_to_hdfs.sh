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
HDFS_MONTH_DIR="${HDFS_SOURCE_DIR}/${MONTH}"

if [[ ! -d "${MONTH_DIR}" ]]; then
  echo "Local month source directory does not exist: ${MONTH_DIR}" >&2
  exit 1
fi

if ! compgen -G "${MONTH_DIR}/*.json.gz" > /dev/null; then
  echo "No GH Archive json.gz files found in ${MONTH_DIR}" >&2
  exit 1
fi

hdfs dfs -mkdir -p "${HDFS_SOURCE_DIR}"
if hdfs dfs -test -d "${HDFS_MONTH_DIR}"; then
  hdfs dfs -rm -r -skipTrash "${HDFS_MONTH_DIR}"
fi
hdfs dfs -mkdir -p "${HDFS_MONTH_DIR}"
hdfs dfs -put -f "${MONTH_DIR}"/*.json.gz "${HDFS_MONTH_DIR}/"

echo "Uploaded ${MONTH} GH Archive source files to HDFS: ${HDFS_MONTH_DIR}"
