import os
import uuid

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session", autouse=True)
def spark_session() -> SparkSession:
    """
    This method creates and returns a new spark session configured to run localy in docker containers.

    :param app_name: (str), Name of the application

    returns: (SparkSession), new spark session
    """
    spark = SparkSession.builder \
        .appName(f"test_{uuid.uuid4()}") \
        .config("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://localhost:7077")) \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.num.executors", "1") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.eventLog.dir", "file:///opt/spark/work-dir/spark-events") \
        .config("spark.default.output.path", "/opt/spark/work-dir/data/output") \
        .config("spark.default.input.path", "/opt/spark/work-dir/data/input") \
        .config("spark.eventLog.enabled", "false") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.hadoop.security.authorization", "false") \
        .getOrCreate()
    yield spark
    spark.stop()
