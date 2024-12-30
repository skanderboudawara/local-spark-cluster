"""
This module provides the Output class for writing DataFrames to local file paths in various formats.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, DataFrameWriter

from compute._logger import run_logger
from compute._utils import (
    sanitize_columns,
)
from compute.datastorage import DataStorage


class Output(DataStorage):
    """
    Output class
    """
    def __init__(
        self,
        path: str,
        extension: str | None = None,
    ) -> None:
        """
        This class is used to write data to a specified file path.

        :param path: (str), Path to the output file.

        :returns: None
        """
        super().__init__(path=path, extension=extension)

    def append(self, df: DataFrame, partitionBy: int | str | list | None = None) -> None:  # noqa: N803
        """
        Writes the DataFrame to a local path in the specified format.

        :param df: (DataFrame), DataFrame to write.
        :param format: (str), Output file format ('csv', 'json', or 'parquet').
        :param partitionBy: (int | str | list), Column to partition by.

        :returns: None
        """
        self.__prepare_dump(df, "append", partitionBy)

    def write(self, df: DataFrame, partitionBy: int | str | list | None = None) -> None:  # noqa: N803
        """
        Writes the DataFrame to a local path in the specified format.

        :param df: (DataFrame), DataFrame to write.
        :param format: (str), Output file format ('csv', 'json', or 'parquet').
        :param partitionBy: (int | str | list), Column to partition by.

        :returns: None
        """
        self.__prepare_dump(df, "overwrite", partitionBy)

    def __prepare_dump(
        self,
        df: DataFrame,
        mode: str,
        partitionBy: int | str | list | None = None,  # noqa: N803
    ) -> None:
        """
        This method prepares the DataFrame for writing to a file.

        :param df: (DataFrame), DataFrame to write.
        :param mode: (str), Write mode ('append' or 'overwrite').
        :param partitionBy: (int | str | list), Column to partition by.

        :returns: None
        """
        if not isinstance(df, DataFrame):
            raise ValueError("Argument 'df' must be a DataFrame.")
        if not isinstance(mode, str):
            raise ValueError("Argument 'mode' must be a string.")
        if mode not in {"append", "overwrite"}:
            raise ValueError("Argument 'mode' must be either 'append' or 'overwrite'.")
        if not isinstance(partitionBy, (int, str, list, type(None))):
            raise ValueError("Argument 'partitionBy' must be an integer, string, list, or None.")
        df = sanitize_columns(df)
        if partitionBy:
            partitionBy = partitionBy if isinstance(partitionBy, list) else [partitionBy]  # noqa: N806
            df = df.repartition(*partitionBy)
        writer = df.write.mode("overwrite")
        self.__dump_file(writer)
        run_logger.info(f"file saved: {self.path}")

    def __dump_file(self, writer: DataFrameWriter) -> None:
        """
        Write the DataFrame to the specified format.

        :param writer: (DataFrameWriter), instance to write data.
        :param format: (str), Output file format ('csv', 'json', or 'parquet').

        :returns: None
        """
        run_logger.info(f"dumping data to: {self.path}")
        # Validate the format and write accordingly
        if self.extension == "parquet":
            writer.format(self.extension).save(self.path)
            return
        if self.extension == "csv":
            writer.format(self.extension).option("header", "true").save(self.path[:-4])
            return
        if self.extension == "json":
            writer.format(self.extension).save(self.path)
            return
        raise ValueError(
            f"Unsupported format: '{self.extension}'. Please choose 'csv', 'json', or 'parquet'.",
        )
