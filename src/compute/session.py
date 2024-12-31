"""
SparkSession creator
"""
import os
from typing import Optional

from pyspark.sql import SparkSession


def session(app_name: str, conf: Optional[dict] = None) -> SparkSession:  # pragma: no cover
    """
    This function is used to create a Spark session with the provided configuration.

    :param app_name: (str), Name of the Spark application.
    :param conf: (dict), Configuration options for the Spark session.

    :returns: (SparkSession), Spark session instance.
    """
    default_session = SparkSession.builder \
        .appName(app_name) \
        .master(os.environ.get("SPARK_MASTER_URL", default="local")) \
        .config("spark.eventLog.enabled", "false") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.eventLog.dir", "file:///opt/spark/work-dir/spark-events") \
        .config("spark.default.output.path", "/opt/bitnami/spark") \
        .config("spark.default.input.path", "/opt/bitnami/spark")
    session: SparkSession = default_session.config(map=conf) if conf else default_session
    return session.getOrCreate()
