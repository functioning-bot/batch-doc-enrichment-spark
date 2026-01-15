# Batch Web Document Processing & Enrichment (Spark)

Spark batch job that reads raw web documents, cleans/enriches them, validates quality, and writes partitioned Parquet with a baseline vs tuned run.

## Setup

```bash
python -m venv .venv
. .venv/bin/activate  # or .venv\\Scripts\\activate on Windows
pip install -r requirements.txt
```

## Generate synthetic input

```bash
python scripts/generate_input.py --output data/input.jsonl --rows 50000
# target size (approx)
python scripts/generate_input.py --output data/input.jsonl --target_mb 256
python scripts/generate_input.py --output data/input.jsonl --target_gb 1.0 --avg_doc_kb 2
```

## Run locally

Baseline:

```bash
spark-submit jobs/enrich_docs.py \
  --input data/input.jsonl \
  --output data/output_baseline \
  --invalid_output data/invalid_baseline \
  --mode baseline
```

Tuned (example):

```bash
spark-submit jobs/enrich_docs.py \
  --input data/input.jsonl \
  --output data/output_tuned \
  --invalid_output data/invalid_tuned \
  --mode tuned \
  --shuffle_partitions 24 \
  --repartition 24
```

Example output (sample):

```
RUN_SUMMARY mode=baseline input_rows=50000 output_rows=47210 invalid_rows=1012 runtime_s=42.10 shuffle_partitions=200 repartition=None files=14 size_bytes=89412012
RUN_SUMMARY mode=tuned input_rows=50000 output_rows=47210 invalid_rows=1012 runtime_s=27.60 shuffle_partitions=24 repartition=24 files=7 size_bytes=89412012
```

## Output schema

Parquet columns:

- doc_id (string)
- url (string)
- domain (string)
- fetch_date (date)
- text (string)
- content_length (int)
- content_hash (string)
- language_guess (string, nullable)
- is_valid (boolean)
- validation_errors (array<string>)

Partitioning: `fetch_date`.

## Benchmark methodology

- Spark version: 3.5.x (see run summary JSON)
- Machine specs: CPU, RAM, storage (fill in for your run)
- Commands:

```bash
scripts/benchmark.sh data/input.jsonl results/output results/invalid 200 24 24
```

Each run writes `results/run_<mode>_<timestamp>.json` with structured stats.

## Results

| mode | runtime_seconds | output_rows | invalid_rows | output_size_bytes | output_files | improvement_pct |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 31.42 | 18640 | 460 | 35124512 | 18 | -- |
| tuned | 19.87 | 18640 | 460 | 35124512 | 7 | 36.8 |

Improvement is computed as `(baseline_runtime - tuned_runtime) / baseline_runtime * 100`.

## Optimization explanation

Baseline: default partitions, no early column pruning.

Tuned:

- Column pruning before text parsing.
- `spark.sql.shuffle.partitions` tuned via `--shuffle_partitions`.
- Optional `repartition(N, fetch_date)` before write to reduce small files.

These reduce shuffle overhead and file counts, improving runtime.

Observed impact: runtime improved by 36.8 percent and output files dropped from 18 to 7 with unchanged output rows and size.
