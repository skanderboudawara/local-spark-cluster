from pyspark.sql import DataFrame, SparkSession
import os

spark = SparkSession.builder \
        .appName("GUIDON") \
        .config("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://localhost:7077")) \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.eventLog.enabled", "false") \
        .config("spark.eventLog.dir", "file:///opt/spark/work-dir/spark-events") \
        .config("spark.default.output.path", "/opt/spark/work-dir/data") \
        .config("spark.default.input.path", "/opt/spark/work-dir/data") \
        .getOrCreate()

print(spark.sparkContext.appName)

df = spark.createDataFrame([("a", 1), ("b", 2)], ["letter", "number"])

df.coalesce(1).write.mode("overwrite").csv("/opt/spark/work-dir/data/output/sebsi.csv")