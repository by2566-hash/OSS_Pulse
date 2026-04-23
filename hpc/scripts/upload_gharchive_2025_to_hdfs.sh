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

if [[ ! -d "${LOCAL_SOURCE_DIR}" ]]; then
  echo "Local source directory does not exist: ${LOCAL_SOURCE_DIR}" >&2
  exit 1
fi

hdfs dfs -mkdir -p "${HDFS_SOURCE_DIR}"
hdfs dfs -put -f "${LOCAL_SOURCE_DIR}"/*.json.gz "${HDFS_SOURCE_DIR}/"

echo "Uploaded GH Archive 2025 source files to HDFS: ${HDFS_SOURCE_DIR}"
