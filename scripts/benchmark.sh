#!/usr/bin/env bash
set -euo pipefail

INPUT_PATH=${1:-data/input.jsonl}
OUTPUT_BASE_DIR=${2:-results/output}
INVALID_BASE_DIR=${3:-results/invalid}
SHUFFLE_PARTITIONS_BASELINE=${4:-200}
SHUFFLE_PARTITIONS_TUNED=${5:-48}
REPARTITION_TUNED=${6:-48}

TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUTPUT_BASE="${OUTPUT_BASE_DIR}/${TIMESTAMP}"
INVALID_BASE="${INVALID_BASE_DIR}/${TIMESTAMP}"

spark-submit jobs/enrich_docs.py \
  --input "${INPUT_PATH}" \
  --output "${OUTPUT_BASE}/baseline" \
  --invalid_output "${INVALID_BASE}/baseline" \
  --mode baseline \
  --shuffle_partitions "${SHUFFLE_PARTITIONS_BASELINE}"

spark-submit jobs/enrich_docs.py \
  --input "${INPUT_PATH}" \
  --output "${OUTPUT_BASE}/tuned" \
  --invalid_output "${INVALID_BASE}/tuned" \
  --mode tuned \
  --shuffle_partitions "${SHUFFLE_PARTITIONS_TUNED}" \
  --repartition "${REPARTITION_TUNED}"
