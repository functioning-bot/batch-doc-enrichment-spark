#!/usr/bin/env bash
set -euo pipefail

INPUT=${1:-data/input.jsonl}

spark-submit jobs/enrich_docs.py \
  --input "$INPUT" \
  --output data/output_baseline \
  --invalid_output data/invalid_baseline \
  --mode baseline

spark-submit jobs/enrich_docs.py \
  --input "$INPUT" \
  --output data/output_tuned \
  --invalid_output data/invalid_tuned \
  --mode tuned \
  --shuffle_partitions 24 \
  --repartition 24
