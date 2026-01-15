import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REQUIRED_INPUT_COLUMNS = {
    "doc_id",
    "url",
    "fetch_timestamp",
    "http_status",
    "content_type",
}

OUTPUT_COLUMNS = [
    "doc_id",
    "url",
    "domain",
    "fetch_date",
    "text",
    "content_length",
    "content_hash",
    "language_guess",
    "is_valid",
    "validation_errors",
]


def build_spark(app_name: str, shuffle_partitions: int | None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    master = os.environ.get("SPARK_MASTER")
    if master:
        builder = builder.master(master)
    spark = builder.getOrCreate()
    if shuffle_partitions:
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    return spark


def validate_required_columns(df, required: List[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def add_domain_column(df):
    normalized_url = F.when(
        F.col("url").contains("://"), F.col("url")
    ).otherwise(F.concat(F.lit("http://"), F.col("url")))
    host = F.parse_url(normalized_url, F.lit("HOST"))
    fallback = F.regexp_extract(F.col("url"), r"^(?:https?://)?([^/]+)", 1)
    domain = F.coalesce(host, fallback)
    return df.withColumn("domain", F.lower(F.trim(domain)))


def ensure_content_columns(df):
    if "raw_html" not in df.columns:
        df = df.withColumn("raw_html", F.lit(None).cast("string"))
    if "raw_text" not in df.columns:
        df = df.withColumn("raw_text", F.lit(None).cast("string"))
    return df


def add_text_column(df):
    html_text = F.regexp_replace(F.col("raw_html"), "<[^>]+>", " ")
    raw_text = F.col("raw_text")
    html_empty = F.col("raw_html").isNull() | (
        F.length(F.trim(F.col("raw_html"))) == 0
    )
    raw_text_empty = F.col("raw_text").isNull() | (
        F.length(F.trim(F.col("raw_text"))) == 0
    )
    html_clean = F.when(html_empty, F.lit(None)).otherwise(html_text)
    raw_text_clean = F.when(raw_text_empty, F.lit(None)).otherwise(raw_text)
    combined = F.coalesce(html_clean, raw_text_clean)
    cleaned = F.trim(F.regexp_replace(combined, "\\s+", " "))
    return df.withColumn("text", cleaned)


def add_validation_columns(df):
    doc_id_missing = F.col("doc_id").isNull() | (F.length(F.trim(F.col("doc_id"))) == 0)
    url_missing = F.col("url").isNull() | (F.length(F.trim(F.col("url"))) == 0)
    raw_html_empty = F.col("raw_html").isNull() | (
        F.length(F.trim(F.col("raw_html"))) == 0
    )
    raw_text_empty = F.col("raw_text").isNull() | (
        F.length(F.trim(F.col("raw_text"))) == 0
    )
    raw_missing = raw_html_empty & raw_text_empty
    text_empty = F.col("text").isNull() | (F.length(F.trim(F.col("text"))) == 0)

    errors = F.array_remove(
        F.array(
            F.when(doc_id_missing, F.lit("doc_id_missing")),
            F.when(url_missing, F.lit("url_missing")),
            F.when(raw_missing, F.lit("raw_content_missing")),
            F.when(F.col("content_length") <= 0, F.lit("content_length_non_positive")),
            F.when(text_empty, F.lit("text_empty")),
        ),
        F.lit(None),
    )
    return df.withColumn("validation_errors", errors).withColumn(
        "is_valid", F.size(F.col("validation_errors")) == F.lit(0)
    )


def compute_output_stats(spark: SparkSession, output_path: str) -> Tuple[int, int, int]:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = jvm.org.apache.hadoop.fs.Path(output_path)
    if not fs.exists(path):
        return 0, 0, 0

    def walk(p):
        part_file_count = 0
        total_size = 0
        partition_dirs = 0
        for status in fs.listStatus(p):
            if status.isDirectory():
                if "=" in status.getPath().getName():
                    partition_dirs += 1
                sub_files, sub_partitions, sub_size = walk(status.getPath())
                part_file_count += sub_files
                partition_dirs += sub_partitions
                total_size += sub_size
            else:
                name = status.getPath().getName()
                if name.startswith("part-"):
                    part_file_count += 1
                total_size += status.getLen()
        return part_file_count, partition_dirs, total_size

    files, partitions, size = walk(path)
    return files, partitions, size


def read_input(spark: SparkSession, input_path: str):
    lower = input_path.lower()
    if lower.endswith(".parquet"):
        return spark.read.parquet(input_path)
    return spark.read.json(input_path)


def validate_counts(input_rows: int, output_rows: int, null_doc_ids: int):
    if null_doc_ids > 0:
        raise ValueError(f"Validation failed: null doc_id count = {null_doc_ids}")
    if output_rows > input_rows:
        raise ValueError(
            f"Validation failed: output_rows ({output_rows}) > input_rows ({input_rows})"
        )


def enforce_output_schema(df):
    return df.select(*OUTPUT_COLUMNS)


def run_job(args):
    spark = build_spark("batch-doc-enrichment", args.shuffle_partitions)
    start = time.perf_counter()

    df = read_input(spark, args.input)
    validate_required_columns(df, REQUIRED_INPUT_COLUMNS)
    if "raw_html" not in df.columns and "raw_text" not in df.columns:
        raise ValueError("Input must contain raw_html or raw_text")
    df = ensure_content_columns(df)

    input_rows = df.count()
    df = df.filter(F.col("http_status") == F.lit(200))

    if args.mode == "tuned":
        df = df.select(
            "doc_id",
            "url",
            "fetch_timestamp",
            "http_status",
            "content_type",
            "raw_html",
            "raw_text",
        )

    df = add_domain_column(df)
    df = add_text_column(df)

    df = df.withColumn(
        "fetch_date", F.to_date(F.to_timestamp(F.col("fetch_timestamp")))
    ).withColumn("content_length", F.length(F.col("text")))

    df = df.withColumn("content_hash", F.sha2(F.col("text"), 256))
    df = df.withColumn("language_guess", F.lit(None).cast("string"))

    df = add_validation_columns(df)

    if args.repartition and args.mode == "tuned":
        df = df.repartition(args.repartition, "fetch_date")

    valid_df = df.filter(F.col("is_valid") == F.lit(True))
    invalid_df = df.filter(F.col("is_valid") == F.lit(False))

    null_doc_ids = valid_df.filter(F.col("doc_id").isNull()).count()
    output_rows = valid_df.count()
    invalid_rows = invalid_df.count()
    validate_counts(input_rows, output_rows, null_doc_ids)

    output_df = enforce_output_schema(valid_df)
    output_df.write.mode("overwrite").partitionBy("fetch_date").parquet(args.output)

    if args.invalid_output:
        invalid_df.write.mode("overwrite").parquet(args.invalid_output)

    elapsed = time.perf_counter() - start
    files, partitions, size = compute_output_stats(spark, args.output)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    summary = {
        "mode": args.mode,
        "spark_version": spark.version,
        "input_path": args.input,
        "output_path": args.output,
        "invalid_output_path": args.invalid_output,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "invalid_rows": invalid_rows,
        "runtime_seconds": round(elapsed, 2),
        "shuffle_partitions": args.shuffle_partitions,
        "repartition": args.repartition if args.mode == "tuned" else None,
        "output_files_count": files,
        "output_size_bytes": size,
        "timestamp_utc": timestamp,
    }

    results_dir = Path("results")
    results_dir.mkdir(parents=True, exist_ok=True)
    summary_path = results_dir / f"run_{args.mode}_{timestamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(
        " ".join(
            [
                "RUN_SUMMARY",
                f"mode={args.mode}",
                f"input_rows={input_rows}",
                f"output_rows={output_rows}",
                f"invalid_rows={invalid_rows}",
                f"runtime_s={elapsed:.2f}",
                f"shuffle_partitions={args.shuffle_partitions}",
                f"repartition={args.repartition if args.mode == 'tuned' else None}",
                f"files={files}",
                f"size_bytes={size}",
            ]
        )
    )

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--invalid_output")
    parser.add_argument("--shuffle_partitions", type=int)
    parser.add_argument("--repartition", type=int)
    parser.add_argument("--mode", choices=["baseline", "tuned"], default="baseline")
    run_job(parser.parse_args())
