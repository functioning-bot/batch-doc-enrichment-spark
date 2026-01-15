from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def test_hashing_consistency():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([("hello",)], ["text"])
    value = df.select(F.sha2(F.col("text"), 256).alias("h")).collect()[0]["h"]
    assert value == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    spark.stop()
