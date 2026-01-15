import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.enrich_docs import add_validation_columns, validate_required_columns


def test_validation_rules():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame(
        [
            (None, "", "", "", 0),
            ("doc_1", "https://example.com", "<p>text</p>", "", 4),
        ],
        ["doc_id", "url", "raw_html", "raw_text", "content_length"],
    )
    df = add_validation_columns(df)
    rows = df.select("is_valid").collect()
    assert rows[0]["is_valid"] is False
    assert rows[1]["is_valid"] is True
    spark.stop()


def test_missing_required_columns():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([(1,)], ["doc_id"])
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_required_columns(df, ["doc_id", "url"])
    spark.stop()


def test_raw_content_missing_is_invalid():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame(
        [("doc_1", "https://example.com", "", "", "")],
        ["doc_id", "url", "raw_html", "raw_text", "text"],
    ).withColumn("content_length", F.length(F.col("text")))
    df = add_validation_columns(df)
    row = df.select("is_valid").collect()[0]
    assert row["is_valid"] is False
    spark.stop()
