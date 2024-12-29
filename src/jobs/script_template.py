import sys

sys.path.append("src/")

from pyspark.sql.functions import rand

from spark_session import get_spark_session

app_name = "YourSparkApplicationName"
spark_session = get_spark_session(app_name)

# Create a random DataFrame
df = spark_session.range(0, 100).withColumn("random", rand())
# Show the DataFrame
df.show()

# Save the DataFrame to a CSV file
df.write_dataframe(format="csv")

# Load the dataframe
processed_df = spark_session.read_dataframe(
    "data/output/YourSparkApplicationName",
    file_extension="csv",
    header=True,
    inferSchema=True,
)

# Show the first three rows of the DataFrame
processed_df.limit(3).show()

# Stop the Spark session
spark_session.stop()
