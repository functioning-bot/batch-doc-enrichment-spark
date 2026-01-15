import json
import os
from argparse import Namespace
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession

from jobs.enrich_docs import run_job


def write_input(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def test_job_smoke():
    os.environ["SPARK_MASTER"] = "local[2]"
    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        input_path = base / "input.jsonl"
        output_path = base / "output"
        invalid_path = base / "invalid"

        rows = []
        start = datetime.utcnow()
        for i in range(200):
            rows.append(
                {
                    "doc_id": "" if i % 10 == 0 else f"doc_{i}",
                    "url": "" if i % 15 == 0 else "example.com/page",
                    "fetch_timestamp": (start + timedelta(seconds=i)).isoformat(),
                    "http_status": 200 if i % 7 != 0 else 404,
                    "content_type": "text/html",
                    "raw_html": "" if i % 12 == 0 else "<p>hello world</p>",
                    "raw_text": "fallback text" if i % 12 == 0 else "",
                }
            )

        rows[5]["raw_html"] = ""
        rows[5]["raw_text"] = ""

        write_input(input_path, rows)

        args = Namespace(
            input=str(input_path),
            output=str(output_path),
            invalid_output=str(invalid_path),
            shuffle_partitions=4,
            repartition=None,
            mode="baseline",
        )
        run_job(args)

        assert output_path.exists()
        assert any(output_path.rglob("part-*") )
        assert invalid_path.exists()

        spark = SparkSession.builder.master("local[1]").appName("test-read").getOrCreate()
        output_df = spark.read.parquet(str(output_path))
        invalid_df = spark.read.parquet(str(invalid_path))
        required = {
            "doc_id",
            "url",
            "domain",
            "fetch_date",
            "text",
            "content_length",
            "content_hash",
            "is_valid",
        }
        assert required.issubset(set(output_df.columns))
        output_rows = output_df.count()
        invalid_rows = invalid_df.count()
        input_rows = (
            spark.read.json(str(input_path))
            .filter("http_status = 200")
            .count()
        )
        assert output_rows + invalid_rows == input_rows
        spark.stop()
