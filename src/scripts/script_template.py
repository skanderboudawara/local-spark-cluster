from spark_session import get_spark_session

app_name = "YourSparkApplicationName"
spark = get_spark_session(app_name)

# Load data
df = spark.read.csv(f"./data/input/{app_name}/your_input_file.csv", header=True, inferSchema=True)

# Process your data (example operation)
processed_df = df.select("column1", "column2")

# Write output
# Change this to "json" or "parquet" as needed
df.write_dataframe("csv")

# Stop the Spark session
spark.stop()