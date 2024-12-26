import os

from pyspark.sql import DataFrame, SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """
    This method creates and returns a new spark session configured to run localy in docker containers.

    :param app_name: (str), Name of the application

    returns: (SparkSession), new spark session
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master("local") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.eventLog.dir", "file:///opt/spark/work-dir/spark-events") \
        .config("spark.default.output.path", "/opt/bitnami/spark/data") \
        .config("spark.default.input.path", "/opt/bitnami/spark/data") \
        .getOrCreate()


def read_dataframe(spark_session: SparkSession, file_name: str, file_extension: str, **kwargs) -> DataFrame:
    """
    This method reads input data into a DataFrame from a file.

    The format is determined from the file extension (csv, json, or parquet).

    Additional Spark read options can be specified using keyword arguments.

    :param spark_session: (SparkSession), Current spark session

    :param file_name: (str), Name of the input file to read (including the extension).

    :param file_extension: (str), File extension of the input file (csv, json, or parquet).

    :param kwargs: Additional keyword arguments to pass to the Spark read method.

    :returns: (DataFrame), DataFrame containing the input data.

    :raises ValueError: If the file format is unsupported or if no file extension is provided.
    """
    # Extract the file format from the file extension
    file_extension = file_extension if file_extension else file_name.split(".")[-1].lower()

    # Read the DataFrame from the specified format
    if file_extension == "parquet":
        return spark_session.read.parquet(file_name, **kwargs)
    if file_extension == "csv":
        return spark_session.read.csv(file_name, **kwargs)
    if file_extension == "json":
        return spark_session.read.json(file_name, **kwargs)
    raise ValueError(f"Unsupported format: {file_extension}. Please choose 'csv', 'json', or 'parquet'.")


def write_dataframe(df: DataFrame, format: str, custom_name: str=None) -> None:
    """
    This method writes the output DataFrame locally in the specified format (csv, json, or parquet).

    :param df: (DataFrame), DataFrame to write locally

    :param format: (str), Output file format (csv, json, or parquet)

    :returns: None
    """
    # Get the current Spark session
    spark_session = df.sql_ctx.sparkSession

    # Set the full output path based on the application name
    full_output_path = f"data/output/{spark_session.sparkContext.appName}"

    full_output_path = f"{full_output_path}/{custom_name}" if custom_name else full_output_path

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
