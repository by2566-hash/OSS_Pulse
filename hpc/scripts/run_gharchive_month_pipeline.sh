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

MONTH_SOURCE_DIR="${LOCAL_SOURCE_DIR}/${MONTH}"
MONTH_PROFILE_DIR="${LOCAL_PROFILE_DIR}/monthly/${MONTH}"
MONTH_SAMPLE_DIR="${LOCAL_SAMPLE_DIR}/monthly/${MONTH}"
MONTH_LOG_DIR="${LOCAL_LOG_DIR}/monthly/${MONTH}"

HDFS_MONTH_SOURCE_DIR="${HDFS_SOURCE_DIR}/${MONTH}"
HDFS_MONTH_RAW_DIR="${HDFS_RAW_DIR}/${MONTH}"
HDFS_MONTH_CLEAN_STAGE_DIR="${HDFS_CLEAN_STAGE_DIR}/${MONTH}"

mkdir -p "${MONTH_PROFILE_DIR}" "${MONTH_SAMPLE_DIR}" "${MONTH_LOG_DIR}"

if [[ ! -d "${PROJECT_CODE_DIR}" ]]; then
  echo "Project code directory not found: ${PROJECT_CODE_DIR}" >&2
  exit 1
fi

export PYTHONPATH="${PROJECT_CODE_DIR}:${PYTHONPATH:-}"
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

clean_seed_args=()
if [[ -n "${SEED_REPO_FILE:-}" ]]; then
  clean_seed_args+=(--seed-repo-file "${SEED_REPO_FILE}")
fi

generate_month_days() {
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
    print(current.strftime("%Y-%m-%d"))
    current += timedelta(days=1)
PY
}

remove_final_month_partitions() {
  while read -r day; do
    local partition_path="${HDFS_CLEAN_DIR}/event_date=${day}"
    if hdfs dfs -test -d "${partition_path}"; then
      hdfs dfs -rm -r -skipTrash "${partition_path}"
    fi
  done < <(generate_month_days)
}

move_month_partitions_into_final_clean() {
  hdfs dfs -mkdir -p "${HDFS_CLEAN_DIR}"
  while read -r day; do
    local stage_partition="${HDFS_MONTH_CLEAN_STAGE_DIR}/event_date=${day}"
    if hdfs dfs -test -d "${stage_partition}"; then
      hdfs dfs -mv "${stage_partition}" "${HDFS_CLEAN_DIR}/"
    fi
  done < <(generate_month_days)
}

cleanup_month_staging() {
  if [[ "${DELETE_HDFS_STAGE_AFTER_MONTH:-true}" == "true" ]]; then
    if hdfs dfs -test -d "${HDFS_MONTH_SOURCE_DIR}"; then
      hdfs dfs -rm -r -skipTrash "${HDFS_MONTH_SOURCE_DIR}"
    fi
    if hdfs dfs -test -d "${HDFS_MONTH_RAW_DIR}"; then
      hdfs dfs -rm -r -skipTrash "${HDFS_MONTH_RAW_DIR}"
    fi
  fi

  if hdfs dfs -test -d "${HDFS_MONTH_CLEAN_STAGE_DIR}"; then
    hdfs dfs -rm -r -skipTrash "${HDFS_MONTH_CLEAN_STAGE_DIR}"
  fi

  if [[ "${DELETE_LOCAL_SOURCE_AFTER_MONTH:-true}" == "true" ]]; then
    rm -rf "${MONTH_SOURCE_DIR}"
  fi
}

cd "${PROJECT_CODE_DIR}"

echo "=== ${MONTH} Step 1: Ingest monthly GH Archive source into staging raw Parquet ==="
"${SPARK_SUBMIT_CMD}" "${SPARK_COMMON_ARGS[@]}" ingestion/ingest_gharchive.py \
  --input "${HDFS_MONTH_SOURCE_DIR}/*.json.gz" \
  --raw-output "${HDFS_MONTH_RAW_DIR}" \
  --sample-output "${MONTH_SAMPLE_DIR}/raw_gharchive_sample_${MONTH}.csv" \
  --write-partitions "${INGEST_WRITE_PARTITIONS}" \
  --write-mode overwrite \
  2>&1 | tee "${MONTH_LOG_DIR}/ingest_gharchive_${MONTH}.log"

echo "=== ${MONTH} Step 2: Profile staging raw Parquet ==="
"${SPARK_SUBMIT_CMD}" "${SPARK_COMMON_ARGS[@]}" profiling/profile_gharchive.py \
  --input "${HDFS_MONTH_RAW_DIR}" \
  --input-format parquet \
  --summary-output "${MONTH_PROFILE_DIR}/gharchive_profile_summary_${MONTH}.csv" \
  --event-output "${MONTH_PROFILE_DIR}/gharchive_event_type_distribution_${MONTH}.csv" \
  --null-output "${MONTH_PROFILE_DIR}/gharchive_null_counts_${MONTH}.csv" \
  --repo-output "${MONTH_PROFILE_DIR}/gharchive_top_repos_${MONTH}.csv" \
  --sample-output "${MONTH_SAMPLE_DIR}/raw_gharchive_profile_sample_${MONTH}.csv" \
  2>&1 | tee "${MONTH_LOG_DIR}/profile_raw_gharchive_${MONTH}.log"

echo "=== ${MONTH} Step 3: Clean staging raw Parquet into staging cleaned Parquet ==="
"${SPARK_SUBMIT_CMD}" "${SPARK_COMMON_ARGS[@]}" cleaning/clean_gharchive.py \
  --input "${HDFS_MONTH_RAW_DIR}" \
  --input-format parquet \
  --output "${HDFS_MONTH_CLEAN_STAGE_DIR}" \
  --sample-output "${MONTH_SAMPLE_DIR}/clean_gharchive_sample_${MONTH}.csv" \
  --write-mode overwrite \
  "${clean_seed_args[@]}" \
  2>&1 | tee "${MONTH_LOG_DIR}/clean_gharchive_${MONTH}.log"

echo "=== ${MONTH} Step 4: Profile staging cleaned Parquet ==="
"${SPARK_SUBMIT_CMD}" "${SPARK_COMMON_ARGS[@]}" profiling/profile_gharchive.py \
  --input "${HDFS_MONTH_CLEAN_STAGE_DIR}" \
  --input-format parquet \
  --summary-output "${MONTH_PROFILE_DIR}/gharchive_clean_profile_summary_${MONTH}.csv" \
  --event-output "${MONTH_PROFILE_DIR}/gharchive_clean_event_type_distribution_${MONTH}.csv" \
  --null-output "${MONTH_PROFILE_DIR}/gharchive_clean_null_counts_${MONTH}.csv" \
  --repo-output "${MONTH_PROFILE_DIR}/gharchive_clean_top_repos_${MONTH}.csv" \
  --sample-output "${MONTH_SAMPLE_DIR}/clean_gharchive_profile_sample_${MONTH}.csv" \
  2>&1 | tee "${MONTH_LOG_DIR}/profile_clean_gharchive_${MONTH}.log"

echo "=== ${MONTH} Step 5: Refresh final yearly cleaned partitions for this month ==="
remove_final_month_partitions
move_month_partitions_into_final_clean

echo "=== ${MONTH} Step 6: Cleanup monthly staging and optional local source ==="
cleanup_month_staging

cat <<EOF
Monthly GH Archive pipeline completed for ${MONTH}.

Local outputs:
  Profiles: ${MONTH_PROFILE_DIR}
  Samples:  ${MONTH_SAMPLE_DIR}
  Logs:     ${MONTH_LOG_DIR}

Final HDFS cleaned output:
  ${HDFS_CLEAN_DIR}
EOF
