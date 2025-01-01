from pyspark.sql import DataFrame, DataFrameWriter
from pyspark.sql.session import SparkSession
import pytest

from compute.output import Output
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

class MockDumpFile:
    def __init__(self):
        self.called = False

    def __call__(self, writer):
        self.called = True

@pytest.fixture
def mock_logger(monkeypatch):
    class MockLogger:
        def info(self, msg) -> None:
            pass
    logger = monkeypatch.setattr(target="compute.output.run_logger", name=MockLogger())
    return logger

@pytest.fixture
def mock__prepare_files(monkeypatch):
    # Define a wrapper class to track calls to the private method
    class MockPrepareDump:
        def __init__(self):
            self.called = False
            self.args = None
            self.kwargs = None

        def __call__(self, df, mode, partitionBy):
            self.called = True
            self.args = (df, mode, partitionBy)
            return "write"

    # Create an instance of the mock and patch the private method
    mock_prepare_dump = MockPrepareDump()
    monkeypatch.setattr(target=Output, name="_Output__prepare_dump", value=mock_prepare_dump)
    return mock_prepare_dump

@pytest.fixture
def mock_session(monkeypatch, spark_session):
    return monkeypatch.setattr(target=Output, name="session", value=spark_session)

class MockRepartition:
    def __init__(self, df):
        self.df = df
        self.called = False
        self.args = None
        print("Repartition called with args:", self.args)  # Debug print statement

    def __call__(self, *args):
        self.called = True
        self.args = args
        return self.df

class TestOutput:
    @pytest.mark.parametrize("method, mode", [
        ("write", "overwrite"),
        ("append", "append")
    ])
    def test_Output_methods(mock_session, mock_logger, mock__prepare_files, monkeypatch, spark_session, method, mode) -> None:
        output_instance = Output(path="dummy_path", extension="csv")
        df: DataFrame = spark_session.createDataFrame(data=[(1, "test")], schema=["id", "value"])
        getattr(output_instance, method)(df=df)
        assert mock__prepare_files.called, f"The private method __prepare_dump was not called for method {method}."
        assert mock__prepare_files.args[1] == mode, f"Expected '{mode}' as the second argument for method {method}, got {mock__prepare_files.args[1]} instead."

    def test___prepare_dump_no_repartition(mock_session, mock_logger, monkeypatch, spark_session) -> None:
        output_instance = Output(path="dummy_path", extension="csv")
        df: DataFrame = spark_session.createDataFrame(data=[(1, "test")], schema=["id", "value"])
        
        mock_dump_file = MockDumpFile()
        monkeypatch.setattr(output_instance, "_Output__dump_file", mock_dump_file)
        mock_repartition = MockRepartition(df)
        monkeypatch.setattr(DataFrame, "repartition", mock_repartition)
        output_instance._Output__prepare_dump(df=df, mode="overwrite", partitionBy=None)
        # Assertions
        assert mock_repartition.called is False, "df.repartition was called."
        assert mock_dump_file.called, "__dump_file was not called."
    
    def test___prepare_dump_with_repartition(mock_session, mock_logger, monkeypatch, spark_session) -> None:
        output_instance = Output(path="dummy_path", extension="csv")
        df: DataFrame = spark_session.createDataFrame(data=[(1, "test")], schema=["id", "value"])
        mock_dump_file = MockDumpFile()
        monkeypatch.setattr(output_instance, "_Output__dump_file", mock_dump_file)
        mock_repartition = MockRepartition(df)
        monkeypatch.setattr(DataFrame, "repartition", mock_repartition)
        output_instance._Output__prepare_dump(df=df, mode="overwrite", partitionBy=2)
        # Assertions
        assert mock_repartition.called, "df.repartition was not called."
        assert mock_repartition.args == (2,), f"Expected repartition to be called with ('id',), got {mock_repartition.args} instead."
        assert mock_dump_file.called, "__dump_file was not called."

    def test___prepare_dump_raises(mock_repartition, mock_session, mock_logger, monkeypatch, spark_session) -> None:
        output_instance = Output(path="dummy_path", extension="csv")
        df: DataFrame = spark_session.createDataFrame(data=[(1, "test")], schema=["id", "value"])
        with pytest.raises(TypeError, match="Argument 'df' must be a DataFrame."):
            output_instance._Output__prepare_dump(df=1, mode="overwrite", partitionBy="id")
        with pytest.raises(TypeError, match="Argument 'mode' must be a string."):
            output_instance._Output__prepare_dump(df=df, mode=1, partitionBy="id")
        with pytest.raises(ValueError, match="Argument 'mode' must be either 'append' or 'overwrite'."):
            output_instance._Output__prepare_dump(df=df, mode="rewrite", partitionBy="id")
        with pytest.raises(TypeError, match="Argument 'partitionBy' must be an integer, string, list, or None."):
            output_instance._Output__prepare_dump(df=df, mode="overwrite", partitionBy=3.2)

    @pytest.mark.parametrize("extension, expected_format, path_in, expected_path", [
        ("csv", "csv", "dummy_path.csv", "dummy_path"),
        ("csv", "csv", "dummy_path", "dummy_path"),
        ("parquet", "parquet", "dummy_path.parquet", "dummy_path"),
        ("parquet", "parquet", "dummy_path", "dummy_path"),
        ("json", "json", "dummy_path.json", "dummy_path"),
        ("json", "json", "dummy_path", "dummy_path"),
    ])
    def test___dump_file(mock_session, mock_logger, monkeypatch, spark_session, extension, expected_format, path_in, expected_path) -> None:
        output_instance = Output(path=path_in, extension=extension)
        df: DataFrame = spark_session.createDataFrame(data=[(1, "test")], schema=["id", "value"])
        
        class MockWriterMode:
            def __init__(self):
                self.called = False
                self.args = None

            def format(self, source):
                assert source == expected_format, f"Expected format '{expected_format}', but got {source}"
                return self

            def option(self, key, value):
                if expected_format == "csv":
                    assert key == "header" and value == "true", f"Expected 'header'='true', but got {key}={value}"
                return self

            def save(self, path):
                assert path == expected_path, f"Expected path '{expected_path}', but got {path}"
                self.called = True
                return self

        mock_writer = MockWriterMode()
        monkeypatch.setattr(DataFrameWriter, "save", mock_writer.save)
        output_instance._Output__dump_file(writer=mock_writer)
        assert mock_writer.called, "__dump_file was not called or save was not called correctly."
