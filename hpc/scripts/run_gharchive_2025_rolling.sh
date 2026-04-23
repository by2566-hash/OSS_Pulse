#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HPC_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${1:-${HPC_DIR}/env/oss_pulse_2025.env}"
START_MONTH_ARG="${2:-}"
END_MONTH_ARG="${3:-}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Environment file not found: ${ENV_FILE}" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"

START_MONTH="${START_MONTH_ARG:-${START_MONTH:-1}}"
END_MONTH="${END_MONTH_ARG:-${END_MONTH:-12}}"

mkdir -p "${LOCAL_LOG_DIR}" "${LOCAL_PROFILE_DIR}" "${LOCAL_SAMPLE_DIR}"

format_month() {
  printf "%04d-%02d" "${YEAR}" "$1"
}

for month_num in $(seq "${START_MONTH}" "${END_MONTH}"); do
  month_id="$(format_month "${month_num}")"

  echo "=============================="
  echo "Processing ${month_id}"
  echo "=============================="

  bash "${SCRIPT_DIR}/download_gharchive_month.sh" "${ENV_FILE}" "${month_id}" \
    2>&1 | tee "${LOCAL_LOG_DIR}/download_gharchive_${month_id}.log"

  bash "${SCRIPT_DIR}/upload_gharchive_month_to_hdfs.sh" "${ENV_FILE}" "${month_id}" \
    2>&1 | tee "${LOCAL_LOG_DIR}/upload_gharchive_${month_id}.log"

  bash "${SCRIPT_DIR}/run_gharchive_month_pipeline.sh" "${ENV_FILE}" "${month_id}" \
    2>&1 | tee "${LOCAL_LOG_DIR}/pipeline_gharchive_${month_id}.log"

  if [[ ! -f "${LOCAL_SAMPLE_DIR}/raw_gharchive_sample_${YEAR}.csv" ]]; then
    cp "${LOCAL_SAMPLE_DIR}/monthly/${month_id}/raw_gharchive_sample_${month_id}.csv" \
      "${LOCAL_SAMPLE_DIR}/raw_gharchive_sample_${YEAR}.csv"
  fi
done

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

cd "${PROJECT_CODE_DIR}"

echo "=== Final Step: Profile full-year cleaned Parquet dataset ==="
"${SPARK_SUBMIT_CMD}" "${SPARK_COMMON_ARGS[@]}" profiling/profile_gharchive.py \
  --input "${HDFS_CLEAN_DIR}" \
  --input-format parquet \
  --summary-output "${LOCAL_PROFILE_DIR}/gharchive_clean_profile_summary_${YEAR}.csv" \
  --event-output "${LOCAL_PROFILE_DIR}/gharchive_clean_event_type_distribution_${YEAR}.csv" \
  --null-output "${LOCAL_PROFILE_DIR}/gharchive_clean_null_counts_${YEAR}.csv" \
  --repo-output "${LOCAL_PROFILE_DIR}/gharchive_clean_top_repos_${YEAR}.csv" \
  --sample-output "${LOCAL_SAMPLE_DIR}/clean_gharchive_profile_sample_${YEAR}.csv" \
  2>&1 | tee "${LOCAL_LOG_DIR}/profile_clean_gharchive_${YEAR}.log"

cat <<EOF
Rolling GH Archive ${YEAR} pipeline completed.

Local outputs:
  Monthly profiles: ${LOCAL_PROFILE_DIR}/monthly
  Yearly cleaned profile: ${LOCAL_PROFILE_DIR}/gharchive_clean_*_${YEAR}.csv
  Samples: ${LOCAL_SAMPLE_DIR}
  Logs: ${LOCAL_LOG_DIR}

Final HDFS output:
  Cleaned yearly partitions: ${HDFS_CLEAN_DIR}
EOF
