import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

spark = SparkSession.builder \
        .appName("GUIDON") \
        .config("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://localhost:7077")) \
        .config("spark.eventLog.enabled", "false") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.eventLog.dir", "file:///opt/spark/work-dir/spark-events") \
        .config("spark.default.output.path", "/opt/spark/work-dir/data") \
        .config("spark.default.input.path", "/opt/spark/work-dir/data") \
        .getOrCreate()


spark.read.csv("file:///opt/spark/work-dir/data/input/random_data.csv", header=True, inferSchema=True).show()
