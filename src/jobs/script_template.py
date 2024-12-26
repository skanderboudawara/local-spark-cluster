from spark_session import get_spark_session

app_name = "YourSparkApplicationName"
spark_session = get_spark_session(app_name)

# Load data
df = spark_session.read_dataframe("random_data.csv", header=True, inferSchema=True)
# Show the DataFrame
df.show()
# Stop the Spark session
spark_session.stop()
