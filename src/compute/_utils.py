from typing import Any
from pyspark.sql import SparkSession, DataFrame
import re
import os

NON_ALPHA_NUMERIC = r"[^a-zA-Z0-9_]+"

def filter_kwargs(kwargs: dict, type: Any) -> dict:
    """
    This function filters the kwargs dictionary by the types of the values.
    
    :param kwargs: (dict) The dictionary to filter.
    :param type: (type) The type to filter by.
    
    :return: (dict) The filtered dictionary.
    """
    return {k: v for k, v in kwargs.items() if isinstance(v, type)}

def spark_session(app_name: str, conf: dict=None) -> SparkSession:
    """
    This function is used to create a Spark session with the provided configuration.
    
    :param app_name: (str), Name of the Spark application.
    :param conf: (dict), Configuration options for the Spark session.
    
    :returns: (SparkSession), Spark session instance.
    """
    default_session = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.eventLog.enabled", "false") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.eventLog.dir", "file:///opt/spark/work-dir/spark-events") \
        .config("spark.default.output.path", "/opt/spark/work-dir/data") \
        .config("spark.default.input.path", "/opt/spark/work-dir/data")
    session = default_session.config(map=conf) if conf else default_session
    return session.getOrCreate()

def get_file_extension(path) -> str:
    """
    This property is used to get the file extension of the input file.
    
    :param: None
    
    :return: (str), File extension of the input file.
    """
    return path.split('.')[-1].lower()

def write_to_one_csv(path: str) -> None:
    os.system(f"cat {path}/*p*.csv >> {path}.csv")
    os.system(f"rm -rf {path}")
    
def sanitize_columns(df: DataFrame) -> DataFrame:
    """
    This function is used to sanitize the column names of a DataFrame.
    
    :param df: (DataFrame), DataFrame to sanitize.
    
    :return: (DataFrame), v DataFrame.
    """
    df = df.toDF(*[re.sub(NON_ALPHA_NUMERIC, "_", c) for c in df.columns])
    return df

def extract_file_name(path: str) -> str:
    """
    This function extracts the file name from the provided path.
    
    :param path: (str), File path.
    
    :return: (str), File name.
    """
    # Regex to match the file name dynamically
    match = re.search(r'/([^/]+?)(\.[^/.]+)?$', path)
    return match.group(1) if match else None

def list_folder_contents(folder_path: str) -> list:
    """
    Lists all files and directories in the specified folder.

    :param folder_path: Path to the folder.
    :return: List of folder contents.
    """
    try:
        # Perform the ls equivalent
        folder_contents = os.listdir(folder_path)
        return folder_contents
    except FileNotFoundError:
        print(f"Error: The folder '{folder_path}' does not exist.")
        return []
    except PermissionError:
        print(f"Error: Permission denied for folder '{folder_path}'.")
        return []