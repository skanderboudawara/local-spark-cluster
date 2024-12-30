"""
This module provides the Input class for reading data from files into a DataFrame using PySpark.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.types import StructType

from compute._logger import run_logger
from compute.datastorage import DataStorage

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, DataFrameReader
    from pyspark.sql.types import StructType


class Input(DataStorage):
    """
    Input Class
    """
    def __init__(
        self,
        path: str,
        schema: StructType | None = None,
        extension: str | None = None,
        **kwargs: Any,
    ) -> None:
        """
        This class is used to read data from a specified file path.

        :param path: (str), Path to the input file.
        :param schema: (StructType), Schema of the input data.
        :param extension: (str), File extension ('csv', 'json', or 'parquet'). (default: None)
        :param kwargs: (dict), Additional keyword arguments for reading the file.

        :returns: None
        """
        super().__init__(path=path, extension=extension)
        self.schema: StructType | None = schema
        self.kwargs: dict[str, Any] = kwargs

    def dataframe(self) -> DataFrame:
        """
        This method is used to read the input file and return a DataFrame.

        :param: None

        :return: (DataFrame), DataFrame created from the input file.
        :raises ValueError: If the file format is unsupported.
        """
        run_logger.info(msg=f"Loading data from: {self.path}")
        # Create a DataFrame reader with optional schema
        reader = self.session.read
        if self.schema:
            reader: DataFrameReader = reader.schema(schema=self.schema)
        df: DataFrame = self.__read_file(reader=reader)
        if df.isEmpty():
            raise ValueError(f"File '{self.path}' is empty.")
        return df

    def __read_file(self, reader: DataFrameReader) -> DataFrame:
        """
        This method reads the input file based on the file extension.

        :param reader: (DataFrameReader), DataFrame reader instance.

        :return: (DataFrame), DataFrame created from the input file.
        """
        if self.extension == "parquet":
            return reader.parquet(path=self.path, **self.kwargs)
        if self.extension == "csv":
            return reader.csv(path=self.path, **self.kwargs)
        if self.extension == "json":
            return reader.json(path=self.path, **self.kwargs)

        # Raise error if the file format is unsupported
        raise ValueError(
            f"Unsupported format: '{self.extension}'. Please choose 'csv', 'json', or 'parquet'.",
        )
