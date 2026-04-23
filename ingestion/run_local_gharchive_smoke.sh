#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

TARGET_DATE="${1:-2025-01-15}"
START_HOUR="${2:-0}"
END_HOUR="${3:-23}"
DEFAULT_YBO_PYTHON="/Users/yubo/Downloads/Anaconda/anaconda3/envs/ybo/bin/python"
WRITE_PARTITIONS="${WRITE_PARTITIONS:-48}"

SOURCE_DIR="data/source/gharchive"
RAW_DIR="data/raw/gharchive"
CLEAN_DIR="data/cleaned/gharchive"
PROFILE_DIR="output/profiling"
SAMPLE_DIR="output/samples"

mkdir -p "${SOURCE_DIR}" "${RAW_DIR}" "${CLEAN_DIR}" "${PROFILE_DIR}" "${SAMPLE_DIR}"

if [[ -n "${PYTHON_BIN:-}" ]]; then
  if ! "${PYTHON_BIN}" -c "import pyspark" >/dev/null 2>&1; then
    echo "Configured PYTHON_BIN does not have pyspark: ${PYTHON_BIN}" >&2
    exit 1
  fi
elif python3 -c "import pyspark" >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif [[ -x "${DEFAULT_YBO_PYTHON}" ]] && "${DEFAULT_YBO_PYTHON}" -c "import pyspark" >/dev/null 2>&1; then
  PYTHON_BIN="${DEFAULT_YBO_PYTHON}"
else
  echo "No Python interpreter with pyspark was found." >&2
  echo "Set PYTHON_BIN=/path/to/python or activate an environment that has pyspark installed." >&2
  exit 1
fi

PYTHON_PREFIX="$(cd "$(dirname "${PYTHON_BIN}")/.." && pwd)"
if [[ -d "${PYTHON_PREFIX}/lib/jvm" ]]; then
  export JAVA_HOME="${PYTHON_PREFIX}/lib/jvm"
  export PATH="${JAVA_HOME}/bin:${PATH}"
fi

export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-6g}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}"
export SPARK_DRIVER_MAX_RESULT_SIZE="${SPARK_DRIVER_MAX_RESULT_SIZE:-1g}"
export SPARK_MAX_PARTITION_BYTES="${SPARK_MAX_PARTITION_BYTES:-64m}"
export SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-16}"

echo "Project root: ${PROJECT_ROOT}"
echo "Target date: ${TARGET_DATE}"
echo "Hour range: ${START_HOUR} to ${END_HOUR}"
echo "Python interpreter: ${PYTHON_BIN}"
if [[ -n "${JAVA_HOME:-}" ]]; then
  echo "JAVA_HOME: ${JAVA_HOME}"
fi
echo "SPARK_DRIVER_MEMORY: ${SPARK_DRIVER_MEMORY}"
echo "SPARK_EXECUTOR_MEMORY: ${SPARK_EXECUTOR_MEMORY}"
echo "WRITE_PARTITIONS: ${WRITE_PARTITIONS}"

echo
echo "=== Step 1: Download GH Archive hourly files ==="
for hour in $(seq "${START_HOUR}" "${END_HOUR}"); do
  file_name="${TARGET_DATE}-${hour}.json.gz"
  url="https://data.gharchive.org/${file_name}"
  output_path="${SOURCE_DIR}/${file_name}"

  echo "Downloading ${file_name}"
  curl -fL --retry 3 --retry-delay 2 -C - \
    -o "${output_path}" \
    "${url}"
done

echo
echo "=== Step 2: Ingest raw JSON.gz into Parquet ==="
"${PYTHON_BIN}" -m ingestion.ingest_gharchive \
  --input "${SOURCE_DIR}/${TARGET_DATE}-*.json.gz" \
  --write-partitions "${WRITE_PARTITIONS}"

echo
echo "=== Step 3: Profile raw Parquet dataset ==="
"${PYTHON_BIN}" -m profiling.profile_gharchive \
  --input "${RAW_DIR}" \
  --input-format parquet

echo
echo "=== Step 4: Clean raw Parquet dataset ==="
"${PYTHON_BIN}" -m cleaning.clean_gharchive \
  --input "${RAW_DIR}" \
  --input-format parquet

echo
echo "=== Step 5: Profile cleaned Parquet dataset ==="
"${PYTHON_BIN}" -m profiling.profile_gharchive \
  --input "${CLEAN_DIR}" \
  --input-format parquet \
  --summary-output "${PROFILE_DIR}/gharchive_clean_profile_summary.csv" \
  --event-output "${PROFILE_DIR}/gharchive_clean_event_type_distribution.csv" \
  --null-output "${PROFILE_DIR}/gharchive_clean_null_counts.csv" \
  --repo-output "${PROFILE_DIR}/gharchive_clean_top_repos.csv" \
  --sample-output "${SAMPLE_DIR}/clean_gharchive_profile_sample.csv"

echo
echo "Smoke test completed."
echo "Raw sample: ${SAMPLE_DIR}/raw_gharchive_sample.csv"
echo "Clean sample: ${SAMPLE_DIR}/clean_gharchive_sample.csv"
echo "Profiling outputs: ${PROFILE_DIR}"
