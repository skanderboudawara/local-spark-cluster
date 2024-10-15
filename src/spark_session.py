from pyspark.sql import DataFrame, SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """
    This method creates and returns a new spark session configured to run localy in docker containers.

    :param app_name: (str), Name of the application

    returns: (SparkSession), new spark session
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "file:///opt/spark/work-dir/spark-events") \
        .config("spark.default.output.path", "/opt/spark/work-dir/data/output") \
        .config("spark.default.input.path", "/opt/spark/work-dir/data/input") \
        .getOrCreate()


def write_dataframe(df: DataFrame, format: str) -> None:
    """
    This method writes the output DataFrame locally in the specified format (csv, json, or parquet).

    :param df: (DataFrame), DataFrame to write locally

    :param format: (str), Output file format (csv, json, or parquet)

    :returns: None
    """
    # Get the current Spark session
    spark_session = df.sql_ctx.sparkSession

    # Get the default output path from Spark configuration
    output_path = spark_session.conf.get("spark.default.output.path")

    # Set the full output path based on the application name
    full_output_path = f"{output_path}/{spark_session.sparkContext.appName}"

    # Write the DataFrame to the specified format
    if format == "parquet":
        df.write.mode("overwrite").parquet(full_output_path)
    elif format == "csv":
        df.write.mode("overwrite").csv(full_output_path, header=True)
    elif format == "json":
        df.write.mode("overwrite").json(full_output_path)
    else:
        raise ValueError(f"Unsupported format: {format}. Please choose 'csv', 'json', or 'parquet'.")



DataFrame.write_dataframe = write_dataframe