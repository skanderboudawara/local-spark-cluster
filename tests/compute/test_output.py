import pytest

from compute.output import Output
from pyspark.sql.types import StructField, StructType, IntegerType, StringType


def test_Output_write(monkeypatch, spark_session):
    # Mock the logger to avoid actual logging
    class MockLogger:
        def info(self, msg):
            pass

    monkeypatch.setattr("compute._dataset.run_logger", MockLogger())

    # Mock the session property to return the fixture's Spark session
    monkeypatch.setattr(Output, "session", spark_session)

    # Mock the __dump_file method to avoid actual file writing
    def mock_dump_file(self, writer):
        pass

    monkeypatch.setattr(Output, "_Output__dump_file", mock_dump_file)

    output_instance = Output(path="dummy_path", extension="csv")
    df = spark_session.createDataFrame([(1, "test")], ["id", "value"])
    output_instance.write(df)

    # No assertions needed as we are testing the method execution without errors
