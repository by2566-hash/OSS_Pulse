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

mkdir -p "${LOCAL_PROFILE_DIR}" "${LOCAL_SAMPLE_DIR}" "${LOCAL_LOG_DIR}"

if [[ ! -d "${PROJECT_CODE_DIR}" ]]; then
  echo "Project code directory not found: ${PROJECT_CODE_DIR}" >&2
  exit 1
fi

export PYTHONPATH="${PROJECT_CODE_DIR}:${PYTHONPATH:-}"
export SPARK_DRIVER_MEMORY
export SPARK_EXECUTOR_MEMORY
export SPARK_DRIVER_MAX_RESULT_SIZE
export SPARK_MAX_PARTITION_BYTES
export SPARK_DEFAULT_PARALLELISM

if [[ -n "${PYSPARK_PYTHON:-}" ]]; then
  export PYSPARK_PYTHON
fi
if [[ -n "${PYSPARK_DRIVER_PYTHON:-}" ]]; then
  export PYSPARK_DRIVER_PYTHON
fi

SPARK_COMMON_ARGS=(
  --master "${SPARK_MASTER}"
  --deploy-mode "${SPARK_DEPLOY_MODE}"
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}"
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}"
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}"
  --conf "spark.num.executors=${SPARK_NUM_EXECUTORS}"
  --conf "spark.driver.maxResultSize=${SPARK_DRIVER_MAX_RESULT_SIZE}"
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS}"
  --conf "spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM}"
  --conf "spark.sql.files.maxPartitionBytes=${SPARK_MAX_PARTITION_BYTES}"
  --conf "spark.sql.session.timeZone=UTC"
)

cd "${PROJECT_CODE_DIR}"

echo "=== Step 1: Ingest GH Archive 2025 source files from HDFS to raw Parquet ==="
"${SPARK_SUBMIT_CMD}" "${SPARK_COMMON_ARGS[@]}" ingestion/ingest_gharchive.py \
  --input "${HDFS_SOURCE_DIR}/*.json.gz" \
  --raw-output "${HDFS_RAW_DIR}" \
  --sample-output "${LOCAL_SAMPLE_DIR}/raw_gharchive_sample_2025.csv" \
  --write-partitions "${INGEST_WRITE_PARTITIONS}" \
  2>&1 | tee "${LOCAL_LOG_DIR}/ingest_gharchive_2025.log"

echo "=== Step 2: Profile raw Parquet dataset ==="
"${SPARK_SUBMIT_CMD}" "${SPARK_COMMON_ARGS[@]}" profiling/profile_gharchive.py \
  --input "${HDFS_RAW_DIR}" \
  --input-format parquet \
  --summary-output "${LOCAL_PROFILE_DIR}/gharchive_profile_summary_2025.csv" \
  --event-output "${LOCAL_PROFILE_DIR}/gharchive_event_type_distribution_2025.csv" \
  --null-output "${LOCAL_PROFILE_DIR}/gharchive_null_counts_2025.csv" \
  --repo-output "${LOCAL_PROFILE_DIR}/gharchive_top_repos_2025.csv" \
  --sample-output "${LOCAL_SAMPLE_DIR}/raw_gharchive_profile_sample_2025.csv" \
  2>&1 | tee "${LOCAL_LOG_DIR}/profile_raw_gharchive_2025.log"

echo "=== Step 3: Clean raw Parquet dataset into HDFS cleaned Parquet ==="
"${SPARK_SUBMIT_CMD}" "${SPARK_COMMON_ARGS[@]}" cleaning/clean_gharchive.py \
  --input "${HDFS_RAW_DIR}" \
  --input-format parquet \
  --output "${HDFS_CLEAN_DIR}" \
  --sample-output "${LOCAL_SAMPLE_DIR}/clean_gharchive_sample_2025.csv" \
  2>&1 | tee "${LOCAL_LOG_DIR}/clean_gharchive_2025.log"

echo "=== Step 4: Profile cleaned Parquet dataset ==="
"${SPARK_SUBMIT_CMD}" "${SPARK_COMMON_ARGS[@]}" profiling/profile_gharchive.py \
  --input "${HDFS_CLEAN_DIR}" \
  --input-format parquet \
  --summary-output "${LOCAL_PROFILE_DIR}/gharchive_clean_profile_summary_2025.csv" \
  --event-output "${LOCAL_PROFILE_DIR}/gharchive_clean_event_type_distribution_2025.csv" \
  --null-output "${LOCAL_PROFILE_DIR}/gharchive_clean_null_counts_2025.csv" \
  --repo-output "${LOCAL_PROFILE_DIR}/gharchive_clean_top_repos_2025.csv" \
  --sample-output "${LOCAL_SAMPLE_DIR}/clean_gharchive_profile_sample_2025.csv" \
  2>&1 | tee "${LOCAL_LOG_DIR}/profile_clean_gharchive_2025.log"

cat <<EOF
GH Archive 2025 pipeline completed.

Local outputs:
  Profiles: ${LOCAL_PROFILE_DIR}
  Samples:  ${LOCAL_SAMPLE_DIR}
  Logs:     ${LOCAL_LOG_DIR}

HDFS outputs:
  Source:   ${HDFS_SOURCE_DIR}
  Raw:      ${HDFS_RAW_DIR}
  Cleaned:  ${HDFS_CLEAN_DIR}
EOF
