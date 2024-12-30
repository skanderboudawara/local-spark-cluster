from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def filter_kwargs(kwargs: dict, type: Any) -> dict:
    """
    This function filters the kwargs dictionary by the types of the values.

    :param kwargs: (dict) The dictionary to filter.
    :param type: (type) The type to filter by.

    :return: (dict) The filtered dictionary.
    """
    return {k: v for k, v in kwargs.items() if isinstance(v, type)}


def get_file_extension(path: str) -> str | None:
    """
    This property is used to get the file extension of the input file.

    :param: None

    :return: (str), File extension of the input file.
    """
    return path.split(sep=".")[-1].lower()


def sanitize_columns(df: DataFrame) -> DataFrame:
    """
    This function is used to sanitize the column names of a DataFrame.

    :param df: (DataFrame), DataFrame to sanitize.

    :return: (DataFrame), v DataFrame.
    """
    df = df.toDF(*[re.sub(pattern=r"[^a-zA-Z0-9_]+", repl="_", string=c) for c in df.columns])
    return df


def extract_file_name(path: str) -> str | None:
    """
    This function extracts the file name from the provided path.

    :param path: (str), File path.

    :return: (str), File name.
    """
    match: re.Match[str] | None = re.search(pattern=r"/?([^/]+?)(\.[^/.]+)?$", string=path)
    return match.group(1) if match else None


def list_folder_contents(folder_path: str) -> list:
    """
    Lists all files and directories in the specified folder.

    :param folder_path: Path to the folder.
    :return: List of folder contents.
    """
    try:
        # Perform the ls equivalent
        folder_contents: list[str] = os.listdir(path=folder_path)
        return folder_contents
    except FileNotFoundError:
        return []
    except PermissionError:
        return []
