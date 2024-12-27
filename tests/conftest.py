# conftest.py
import pytest
from pyspark.sql import SparkSession
import uuid
import os

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName(f"pytest_spark_{uuid.uuid4()}") \
        .master(os.environ.get("SPARK_MASTER_URL", "local")) \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.num.executors", "1") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    print("Fixture setup at session level")
    yield spark
    spark.stop()
    print("Fixture teardown")
