#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HPC_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${1:-${HPC_DIR}/env/oss_pulse_2025.env}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Environment file not found: ${ENV_FILE}" >&2
  echo "Copy hpc/env/oss_pulse_2025.env.example to hpc/env/oss_pulse_2025.env first." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"

mkdir -p \
  "${LOCAL_PROJECT_ROOT}" \
  "${PROJECT_CODE_DIR}" \
  "${LOCAL_SOURCE_DIR}" \
  "${LOCAL_PROFILE_DIR}" \
  "${LOCAL_SAMPLE_DIR}" \
  "${LOCAL_LOG_DIR}" \
  "${LOCAL_REPORT_DIR}"

hdfs dfs -mkdir -p \
  "${HDFS_ROOT}" \
  "${HDFS_SOURCE_DIR}" \
  "${HDFS_RAW_DIR}" \
  "${HDFS_CLEAN_STAGE_DIR}" \
  "${HDFS_CLEAN_DIR}"

cat <<EOF
OSS Pulse HPC layout created.

SSH / local directories:
  LOCAL_PROJECT_ROOT = ${LOCAL_PROJECT_ROOT}
  PROJECT_CODE_DIR   = ${PROJECT_CODE_DIR}
  LOCAL_SOURCE_DIR   = ${LOCAL_SOURCE_DIR}
  LOCAL_PROFILE_DIR  = ${LOCAL_PROFILE_DIR}
  LOCAL_SAMPLE_DIR   = ${LOCAL_SAMPLE_DIR}
  LOCAL_LOG_DIR      = ${LOCAL_LOG_DIR}
  LOCAL_REPORT_DIR   = ${LOCAL_REPORT_DIR}

HDFS directories:
  HDFS_ROOT          = ${HDFS_ROOT}
  HDFS_SOURCE_DIR    = ${HDFS_SOURCE_DIR}
  HDFS_RAW_DIR       = ${HDFS_RAW_DIR}
  HDFS_CLEAN_STAGE_DIR = ${HDFS_CLEAN_STAGE_DIR}
  HDFS_CLEAN_DIR     = ${HDFS_CLEAN_DIR}

Next steps:
  1. Sync or clone your project code into ${PROJECT_CODE_DIR}
  2. Prefer the rolling pipeline:
     bash hpc/scripts/run_gharchive_2025_rolling.sh hpc/env/oss_pulse_2025.env
  3. The one-shot yearly scripts remain available, but they require much more storage.
EOF
