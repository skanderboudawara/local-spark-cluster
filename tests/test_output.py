from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
import pytest

from compute.output import Output
from pyspark.sql.types import StructField, StructType, IntegerType, StringType


def test_Output_write(monkeypatch: pytest.MonkeyPatch, spark_session: SparkSession) -> None:
    # Mock the logger to avoid actual logging
    class MockLogger:
        def info(self, msg) -> None:
            pass

    monkeypatch.setattr(target="compute.output.run_logger", name=MockLogger())

    # Mock the session property to return the fixture's Spark session
    monkeypatch.setattr(target=Output, name="session", value=spark_session)

    # Mock the __dump_file method to avoid actual file writing
    def mock_dump_file(self, writer) -> None:
        pass

    monkeypatch.setattr(target=Output, name="_Output__dump_file", value=mock_dump_file)

    output_instance = Output(path="dummy_path", extension="csv")
    df: DataFrame = spark_session.createDataFrame(data=[(1, "test")], schema=["id", "value"])
    output_instance.write(df=df)

    # No assertions needed as we are testing the method execution without errors
