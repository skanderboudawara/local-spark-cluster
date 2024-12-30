import pytest

from compute.input import Input
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

def test_Input_dataframe(monkeypatch, spark_session):
    # Mock the logger to avoid actual logging
    class MockLogger:
        def info(self, msg):
            pass

    monkeypatch.setattr("compute._dataset.run_logger", MockLogger())

    # Mock the session property to return the fixture's Spark session
    monkeypatch.setattr(Input, "session", spark_session)

    # Mock the __read_file method to return a non-empty DataFrame
    def mock_read_file(self, reader):
        return spark_session.createDataFrame([(1, "test")], ["id", "value"])

    monkeypatch.setattr(Input, "_Input__read_file", mock_read_file)

    input_instance = Input(path="dummy_path", extension="csv")
    df = input_instance.dataframe()

    assert not df.isEmpty()
    assert df.count() == 1
    assert df.columns == ["id", "value"]

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", StringType(), True)
    ])
    
    instance_schema = Input(path="dummy_path", extension="csv", schema=schema)

    def mock_read_file_with_schema(self, reader):
        return spark_session.createDataFrame([(1, "test")], schema=schema)

    monkeypatch.setattr(Input, "_Input__read_file", mock_read_file_with_schema)

    df_schema = instance_schema.dataframe()

    assert df_schema.count() == 1
    assert df_schema.columns == ["id", "value"]

    # Check each field's type
    for field, expected_field in zip(df_schema.schema.fields, schema):
        assert field.name == expected_field.name, f"Expected field name {expected_field.name}, got {field.name}"
        assert field.dataType == expected_field.dataType, f"Expected field dataType {expected_field.dataType}, got {field.dataType}"

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", StringType(), True)
    ])
    
    instance_empty = Input(path="dummy_path", extension="csv", schema=schema)

    def mock_empty_file(self, reader):
        return spark_session.createDataFrame([], schema=schema)

    monkeypatch.setattr(Input, "_Input__read_file", mock_empty_file)

    with pytest.raises(ValueError, match="File 'dummy_path' is empty"):
        instance_empty.dataframe()

def test_Input__read_file(monkeypatch, spark_session):
    # Mock the logger to avoid actual logging
    class MockLogger:
        def info(self, msg):
            pass

    monkeypatch.setattr("compute._dataset.run_logger", MockLogger())

    # Mock the session property to return the fixture's Spark session
    monkeypatch.setattr(Input, "session", spark_session)

    csv_instance = Input(path="dummy_path", extension="csv")
    class MockReader:
        def csv(self, path, **kwargs):
            return "a csv file"
    reader = MockReader()
    df = csv_instance._Input__read_file(reader)
    assert df == "a csv file"
    
    parquet_instance = Input(path="dummy_path", extension="parquet")
    class MockReader:
        def parquet(self, path, **kwargs):
            return "a parquet file"
    reader = MockReader()
    df = parquet_instance._Input__read_file(reader)
    assert df == "a parquet file"
  
    json_instance = Input(path="dummy_path", extension="json")
    class MockReader:
        def json(self, path, **kwargs):
            return "a json file"
    reader = MockReader()
    df = json_instance._Input__read_file(reader)
    assert df == "a json file"
    
    random_instance = Input(path="dummy_path.random")
    class MockReader:
        def json(self, path, **kwargs):
            return "a json file"
    reader = MockReader()
    with pytest.raises(ValueError, match="Unsupported format: 'random'. Please choose 'csv', 'json', or 'parquet'"):
        random_instance._Input__read_file(reader)
