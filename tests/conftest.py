from pyspark.sql import SparkSession
from typing import Generator
from functools import lru_cache
import pytest
import uuid
import os

@pytest.fixture
def spark_session() -> Generator[SparkSession, None, None]:
    """
    This method creates and returns a new spark session configured to run locally in docker

    :param app_name: (str), Name of the application

    returns: (Generator[SparkSession, None, None]), new spark session
    """
    spark = SparkSession.builder \
        .appName("test") \
        .master(os.environ.get("SPARK_MASTER_URL", "local")) \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.executor.instances", "1") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
    yield spark
    spark.stop()
