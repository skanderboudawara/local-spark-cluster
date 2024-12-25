from spark_session import get_spark_session

app_name = "YourSparkApplicationName"
spark_session = get_spark_session(app_name)

# Load data
df = spark_session.read_dataframe("random_data.csv", header=True, inferSchema=True)

# Process your data (example operation)
processed_df = df.select("ID", "Name")

# Write output
# Change this to "json" or "parquet" as needed
processed_df.write_dataframe("csv")

# Stop the Spark session
spark_session.stop()