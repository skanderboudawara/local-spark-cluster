"""
This module provides the DataStorage class for reading and writing data to a specified file path.
"""
from __future__ import annotations

from functools import cached_property

from pyspark.sql import SparkSession

from compute._utils import (
    extract_file_name,
    get_file_extension,
    list_folder_contents,
)


class DataStorage:
    """
    Datastorage class
    """
    def __init__(self, path: str, extension: str | None = None) -> None:  # pragma: no cover
        """
        This class is used to read and write data to a specified file path.

        :param extension: (str), File extension ('csv', 'json', or 'parquet'). (default: None)
            In case the extension is not provided, it will be inferred from the file path.

        :returns: None
        """
        if not isinstance(path, str):
            raise TypeError("path must be a string")
        if not isinstance(extension, (str, type(None))):
            raise TypeError("Argument 'extension' must be a string.")
        if extension and extension not in {"csv", "json", "parquet"}:
            raise ValueError("Argument 'extension' must be either 'csv', 'json', or 'parquet'.")
        self._extension: str | None = extension
        self.path: str = path

    @cached_property
    def filename(self) -> str | None:  # pragma: no cover
        """
        This property is used to get the file name of the input file.

        :param: None

        :return: (str), File name of the input file.
        """
        return extract_file_name(path=self.path)

    @property
    def ls(self) -> list:  # pragma: no cover
        """
        This property is used to list all files and directories in the specified folder.

        :param: None

        :return: (list), List of folder contents.
        """
        return list_folder_contents(folder_path=self.path)

    @cached_property
    def session(self) -> SparkSession:  # pragma: no cover
        """
        This property is used to create a Spark session.

        :param: None

        :return: (SparkSession), Spark session instance.
        """
        return SparkSession.builder.getOrCreate()

    @cached_property
    def extension(self) -> str | None:  # pragma: no cover
        """
        This property is used to get the file extension of the input file.

        :param: None

        :return: (str), File extension of the input file.
        """
        return self._extension if self._extension else get_file_extension(path=self.path)
