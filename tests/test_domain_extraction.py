from pyspark.sql import SparkSession
from jobs.enrich_docs import add_domain_column


def test_domain_extraction():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame(
        [
            ("https://Example.COM/page",),
            ("example.org/path",),
            ("http://sub.domain.net",),
        ],
        ["url"],
    )
    rows = add_domain_column(df).select("domain").collect()
    assert rows[0]["domain"] == "example.com"
    assert rows[1]["domain"] == "example.org"
    assert rows[2]["domain"] == "sub.domain.net"
    spark.stop()
