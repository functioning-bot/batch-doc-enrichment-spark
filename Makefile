PYTHON ?= python
SPARK_SUBMIT ?= spark-submit
INPUT ?= data/input.jsonl

.PHONY: gen-small run-baseline run-tuned benchmark test

gen-small:
	$(PYTHON) scripts/generate_input.py --output $(INPUT) --target_mb 100

run-baseline:
	$(SPARK_SUBMIT) jobs/enrich_docs.py --input $(INPUT) --output data/output_baseline --invalid_output data/invalid_baseline --mode baseline --shuffle_partitions 200

run-tuned:
	$(SPARK_SUBMIT) jobs/enrich_docs.py --input $(INPUT) --output data/output_tuned --invalid_output data/invalid_tuned --mode tuned --shuffle_partitions 48 --repartition 48

benchmark:
	bash scripts/benchmark.sh $(INPUT) results/output results/invalid 200 48 48

test:
	pytest -q
