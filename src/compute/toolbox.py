from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def filter_kwargs(unflitred_dict: dict, type: Any) -> dict:
    """
    This function filters the unflitred_dict dictionary by the types of the values.

    :param unflitred_dict: (dict) The dictionary to filter.
    :param type: (type) The type to filter by.

    :return: (dict) The filtered dictionary.
    """
    return {k: v for k, v in unflitred_dict.items() if isinstance(v, type)}


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


def get_filename(full_path: str) -> str | None:
    """
    This function extracts the file name from the provided full_path.

    :param full_path: (str), File full_path.

    :return: (str), File name.
    """
    if not full_path or not isinstance(full_path, str):
        return None

    base_name: str = os.path.basename(p=full_path)
    name, _ = os.path.splitext(p=base_name)
    return name if name else None


def list_folder_contents(folder_path: str) -> list:
    """
    Lists all files and directories in the specified folder.

    :param folder_path: Path to the folder.
    :return: List of folder contents.
    """
    folder_contents: list[str] = list(os.listdir(path=folder_path))
    return folder_contents
