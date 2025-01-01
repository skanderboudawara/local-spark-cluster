from typing import Literal
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
import pytest

from compute.input import Input
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

@pytest.fixture
def mock_logger(monkeypatch):
    class MockLogger:
        def info(self, msg) -> None:
            pass
    logger = monkeypatch.setattr(target="compute.input.run_logger", name=MockLogger())
    return logger

class TestInput:
    def test_Input_dataframe(mock_logger, monkeypatch: pytest.MonkeyPatch, spark_session: SparkSession) -> None:
        monkeypatch.setattr(target=Input, name="session", value=spark_session)
        # Mock the __read_file method to return a non-empty DataFrame
        def mock_read_file(self, reader) -> DataFrame:
            return spark_session.createDataFrame(data=[(1, "test")], schema=["id", "value"])

        monkeypatch.setattr(target=Input, name="_Input__read_file", value=mock_read_file)

        input_instance = Input(path="dummy_path", extension="csv")
        df: DataFrame = input_instance.dataframe()

        assert not df.isEmpty()
        assert df.count() == 1
        assert df.columns == ["id", "value"]

    def test_Input_dataframe_with_schema(mock_logger, monkeypatch: pytest.MonkeyPatch, spark_session: SparkSession) -> None:
        monkeypatch.setattr(target=Input, name="session", value=spark_session)
        schema = StructType(fields=[
            StructField(name="id", dataType=IntegerType(), nullable=True),
            StructField(name="value", dataType=StringType(), nullable=True)
        ])
        
        instance_schema = Input(path="dummy_path", extension="csv", schema=schema)

        def mock_read_file_with_schema(self, reader) -> DataFrame:
            return spark_session.createDataFrame(data=[(1, "test")], schema=schema)

        monkeypatch.setattr(target=Input, name="_Input__read_file", value=mock_read_file_with_schema)

        df_schema: DataFrame = instance_schema.dataframe()

        assert df_schema.count() == 1
        assert df_schema.columns == ["id", "value"]

        # Check each field's type
        for field, expected_field in zip(df_schema.schema.fields, schema):
            assert field.name == expected_field.name, f"Expected field name {expected_field.name}, got {field.name}"
            assert field.dataType == expected_field.dataType, f"Expected field dataType {expected_field.dataType}, got {field.dataType}"

    def test_Input_empty_dataframe(mock_logger, monkeypatch: pytest.MonkeyPatch, spark_session: SparkSession) -> None:
        monkeypatch.setattr(target=Input, name="session", value=spark_session)
        schema = StructType(fields=[
            StructField(name="id", dataType=IntegerType(), nullable=True),
            StructField(name="value", dataType=StringType(), nullable=True)
        ])
        
        instance_empty = Input(path="dummy_path", extension="csv", schema=schema)

        def mock_empty_file(self, reader) -> DataFrame:
            return spark_session.createDataFrame([], schema=schema)

        monkeypatch.setattr(target=Input, name="_Input__read_file", value=mock_empty_file)

        with pytest.raises(expected_exception=ValueError, match="File 'dummy_path' is empty"):
            instance_empty.dataframe()

    @pytest.mark.parametrize("extension, expected_output", [
        ("csv", "a csv file"),
        ("parquet", "a parquet file"),
        ("json", "a json file")
    ])
    def test_Input__read_file(mock_logger, monkeypatch: pytest.MonkeyPatch, spark_session: SparkSession, extension: str, expected_output: str) -> None:
        monkeypatch.setattr(target=Input, name="session", value=spark_session)
        input_instance = Input(path="dummy_path", extension=extension)
        
        class Reader:
            def csv(self, path, **kwargs) -> Literal['a csv file']:
                return "a csv file"
            def parquet(self, path, **kwargs) -> Literal['a parquet file']:
                return "a parquet file"
            def json(self, path, **kwargs) -> Literal['a json file']:
                return "a json file"
        
        reader = Reader()
        df: str = input_instance._Input__read_file(reader) # type: ignore
        assert df == expected_output
    
    def test_Input__read_file_invalid(mock_logger, monkeypatch: pytest.MonkeyPatch, spark_session: SparkSession) -> None:
        monkeypatch.setattr(target=Input, name="session", value=spark_session)
        random_instance = Input(path="dummy_path.random")
        class RandomReader:
            def json(self, path, **kwargs) -> Literal['a json file']:
                return "a json file"
        reader = RandomReader()
        with pytest.raises(expected_exception=ValueError, match="Unsupported format: 'random'. Please choose 'csv', 'json', or 'parquet'"):
            random_instance._Input__read_file(reader)
