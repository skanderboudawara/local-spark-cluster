from typing import Literal
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
import pytest

from compute.input import Input
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

def test_Input_dataframe(monkeypatch: pytest.MonkeyPatch, spark_session: SparkSession) -> None:
    # Mock the logger to avoid actual logging
    class MockLogger:
        def info(self, msg):
            pass

    monkeypatch.setattr(target="compute.input.run_logger", name=MockLogger())

    # Mock the session property to return the fixture's Spark session
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

def test_Input__read_file(monkeypatch: pytest.MonkeyPatch, spark_session: SparkSession) -> None:
    # Mock the logger to avoid actual logging
    class MockLogger:
        def info(self, msg) -> None:
            pass

    monkeypatch.setattr(target="compute.input.run_logger", name=MockLogger())

    # Mock the session property to return the fixture's Spark session
    monkeypatch.setattr(target=Input, name="session", value=spark_session)

    csv_instance = Input(path="dummy_path", extension="csv")
    class CsvReader:
        def csv(self, path, **kwargs) -> Literal['a csv file']:
            return "a csv file"
    reader = CsvReader()
    df: str = csv_instance._Input__read_file(reader) # type: ignore
    assert df == "a csv file"
    
    parquet_instance = Input(path="dummy_path", extension="parquet")
    class ParquetReader:
        def parquet(self, path, **kwargs) -> Literal['a parquet file']:
            return "a parquet file"
    reader = ParquetReader()
    df: str = parquet_instance._Input__read_file(reader)
    assert df == "a parquet file"
  
    json_instance = Input(path="dummy_path", extension="json")
    class JsonReader:
        def json(self, path, **kwargs) -> Literal['a json file']:
            return "a json file"
    reader = JsonReader()
    df: str = json_instance._Input__read_file(reader)
    assert df == "a json file"
    
    random_instance = Input(path="dummy_path.random")
    class RandomReader:
        def json(self, path, **kwargs) -> Literal['a json file']:
            return "a json file"
    reader = RandomReader()
    with pytest.raises(expected_exception=ValueError, match="Unsupported format: 'random'. Please choose 'csv', 'json', or 'parquet'"):
        random_instance._Input__read_file(reader)
