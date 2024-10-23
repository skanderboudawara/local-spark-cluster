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


def read_dataframe(spark_session: SparkSession, file_name: str, **kwargs) -> DataFrame:
    """
    This method reads input data into a DataFrame from a file.
    
    The format is determined from the file extension (csv, json, or parquet).
    
    Additional Spark read options can be specified using keyword arguments.

    :param spark_session: (SparkSession), Current spark session

    :param file_name: (str), Name of the input file to read (including the extension).
    
    :param kwargs: Additional keyword arguments to pass to the Spark read method.

    :returns: (DataFrame), DataFrame containing the input data.
    
    :raises ValueError: If the file format is unsupported or if no file extension is provided.
    """
    # Extract the file format from the file extension
    file_extension = file_name.split('.')[-1].lower()
    
    # Get the default input path from Spark configuration
    input_path = spark_session.conf.get("spark.default.input.path")
    
    # Set the full input path
    full_input_path = f"{input_path}/{file_name}"

    # Read the DataFrame from the specified format
    if file_extension == "parquet":
        return spark_session.read.parquet(full_input_path, **kwargs)
    elif file_extension == "csv":
        return spark_session.read.csv(full_input_path, **kwargs)
    elif file_extension == "json":
        return spark_session.read.json(full_input_path, **kwargs)
    else:
        raise ValueError(f"Unsupported format: {file_extension}. Please choose 'csv', 'json', or 'parquet'.")


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
SparkSession.read_dataframe = read_dataframe