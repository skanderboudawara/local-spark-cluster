"""
This module provides utility functions for file handling and DataFrame manipulation.
"""
from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame


def filter_kwargs(kwargs: dict, type: Any) -> dict:
    """
    This function filters the kwargs dictionary by the types of the values.

    :param kwargs: (dict) The dictionary to filter.
    :param type: (type) The type to filter by.

    :return: (dict) The filtered dictionary.

    Examples:
    >>> filter_kwargs({'a': 1, 'b': 'string', 'c': 3.0, 'd': [1, 2, 3]}, int)
    {'a': 1}

    >>> filter_kwargs({'a': 1, 'b': 'string', 'c': 3.0, 'd': [1, 2, 3]}, float)
    {'c': 3.0}

    >>> filter_kwargs({'a': 1, 'b': 'string', 'c': 3.0, 'd': [1, 2, 3]}, list)
    {'d': [1, 2, 3]}
    """
    return {k: v for k, v in kwargs.items() if isinstance(v, type)}


def get_file_extension(path: str) -> str | None:
    """
    This property is used to get the file extension of the input file.

    :param: None

    :return: (str), File extension of the input file.

    Examples:
    >>> get_file_extension("/path/file.exe")
    'exe'

    >>> get_file_extension("/path/file.tar.gz")
    'gz'

    >>> get_file_extension("/path/file") is None
    True

    >>> get_file_extension(None) is None
    True
    """
    if not path or not isinstance(path, str):
        return None

    base_name: str = os.path.basename(p=path)
    _, extension = os.path.splitext(p=base_name)
    return extension.lstrip(".") if extension else None


def sanitize_columns(df: DataFrame) -> DataFrame:
    """
    This function is used to sanitize the column names of a DataFrame.

    :param df: (DataFrame), DataFrame to sanitize.

    :return: (DataFrame), v DataFrame.

    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> sanitized_df = sanitize_columns(
    ...     spark.createDataFrame(
    ...             data=[("value1", "value2")],
    ...             schema=["col 1", "col-2"]
    ...     ))
    >>> sanitized_df_cols = sanitized_df.columns
    >>> spark.stop()
    >>> print(sanitized_df_cols)
    ['col_1', 'col_2']
    """
    df = df.toDF(*[re.sub(pattern=r"[^a-zA-Z0-9_]+", repl="_", string=c) for c in df.columns])
    return df


def get_filename(full_path: str) -> str | None:
    """
    This function extracts the file name from the provided full_path.

    :param full_path: (str), File full_path.

    :return: (str), File name.

    Examples:
    >>> get_filename("/path/to/file.exe")
    'file'

    >>> get_filename("/path/to/file.tar.gz")
    'file.tar'

    >>> get_filename("/path/to/file.tar.gz")
    'file.tar'

    >>> get_filename(2) is None
    True

    >>> get_filename(None) is None
    True
    """
    if not full_path or not isinstance(full_path, str):
        return None

    base_name: str = os.path.basename(p=full_path)
    name, _ = os.path.splitext(p=base_name)
    return name if name else None


def list_folder_contents(folder_path: str) -> list:
    """
    Lists all files and directories in the specified folder.

    :param folder_path: Path to the folder
    
    :return: List of folder contents.
    """
    folder_contents: list[str] = list(os.listdir(path=folder_path))
    return folder_contents
