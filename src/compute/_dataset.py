from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

from compute._logger import run_logger
from compute._utils import extract_file_name, get_file_extension, list_folder_contents, sanitize_columns

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


class DataStorage:

    def __init__(self, extension: str | None = None) -> None:  # pragma: no cover
        """
        This class is used to read and write data to a specified file path.

        :param extension: (str), File extension ('csv', 'json', or 'parquet'). (default: None)
            In case the extension is not provided, it will be inferred from the file path.

        :returns: None
        """
        if not isinstance(extension, (str, type(None))):
            raise ValueError("Argument 'extension' must be a string.")
        if extension and extension not in {"csv", "json", "parquet"}:
            raise ValueError("Argument 'extension' must be either 'csv', 'json', or 'parquet'.")
        self._extension = extension

    @cached_property
    def filename(self):  # pragma: no cover
        """
        This property is used to get the file name of the input file.

        :param: None

        :return: (str), File name of the input file.
        """
        return extract_file_name(self.path)

    @property
    def ls(self):  # pragma: no cover
        """
        This property is used to list all files and directories in the specified folder.

        :param: None

        :return: (list), List of folder contents.
        """
        return list_folder_contents(self.path)

    @cached_property
    def session(self):  # pragma: no cover
        """
        This property is used to create a Spark session.

        :param: None

        :return: (SparkSession), Spark session instance.
        """
        return SparkSession.builder.getOrCreate()

    @cached_property
    def extension(self):  # pragma: no cover
        """
        This property is used to get the file extension of the input file.

        :param: None

        :return: (str), File extension of the input file.
        """
        return self._extension if self._extension else get_file_extension(self.path)


class Input(DataStorage):
    def __init__(
        self,
        path: str,
        branch: str | None = None,
        schema: StructType | None = None,
        extension: str | None = None,
        **kwargs: dict | None,
    ) -> None:
        """
        This class is used to read data from a specified file path.

        :param path: (str), Path to the input file.
        :param schema: (StructType), Schema of the input data.
        :param branch: (str), Branch name for the input file. (default: None)
        :param extension: (str), File extension ('csv', 'json', or 'parquet'). (default: None)
        :param kwargs: (dict), Additional keyword arguments for reading the file.

        :returns: None
        """
        super().__init__(extension=extension)
        branch = branch.lower().strip("/") if branch else branch
        self.path = f"{branch}/{path}" if branch else path
        self.schema = schema
        self.kwargs = kwargs

    def dataframe(self) -> DataFrame:
        """
        This method is used to read the input file and return a DataFrame.

        :param: None

        :return: (DataFrame), DataFrame created from the input file.
        :raises ValueError: If the file format is unsupported.
        """
        run_logger.info(f"Loading data from: {self.path}")
        # Create a DataFrame reader with optional schema
        reader = self.session.read
        if self.schema:
            reader = reader.schema(self.schema)
        df = self.__read_file(reader)
        if df.isEmpty():
            raise ValueError(f"File '{self.path}' is empty.")
        return df

    def __read_file(self, reader) -> DataFrame:
        """
        This method reads the input file based on the file extension.

        :param reader: (DataFrameReader), DataFrame reader instance.

        :return: (DataFrame), DataFrame created from the input file.
        """
        if self.extension == "parquet":
            return reader.parquet(self.path, **self.kwargs)
        if self.extension == "csv":
            return reader.csv(self.path, **self.kwargs)
        if self.extension == "json":
            return reader.json(self.path, **self.kwargs)

        # Raise error if the file format is unsupported
        raise ValueError(f"Unsupported format: '{self.extension}'. Please choose 'csv', 'json', or 'parquet'.")


class Output(DataStorage):
    def __init__(
        self,
        path: str,
        branch: str | None = None,
        extension: str | None = None,
    ) -> None:
        """
        This class is used to write data to a specified file path.

        :param path: (str), Path to the output file.

        :returns: None
        """
        super().__init__(extension=extension)
        branch = branch.lower().strip("/") if branch else branch
        self.path = f"{branch}/{path}" if branch else path

    def append(self, df: DataFrame, partitionBy: int | str | list | None = None) -> None:
        """
        Writes the DataFrame to a local path in the specified format.

        :param df: (DataFrame), DataFrame to write.
        :param format: (str), Output file format ('csv', 'json', or 'parquet').
        :param partitionBy: (int | str | list), Column to partition by.

        :returns: None
        """
        self.__prepare_dump(df, "append", partitionBy)

    def write(self, df: DataFrame, partitionBy: int | str | list | None = None) -> None:
        """
        Writes the DataFrame to a local path in the specified format.

        :param df: (DataFrame), DataFrame to write.
        :param format: (str), Output file format ('csv', 'json', or 'parquet').
        :param partitionBy: (int | str | list), Column to partition by.

        :returns: None
        """
        self.__prepare_dump(df, "overwrite", partitionBy)

    def __prepare_dump(self, df: DataFrame, mode: str, partitionBy: int | str | list | None = None) -> None:
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
            partitionBy = partitionBy if isinstance(partitionBy, list) else [partitionBy]
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
        raise ValueError(f"Unsupported format: '{self.extension}'. Please choose 'csv', 'json', or 'parquet'.")
