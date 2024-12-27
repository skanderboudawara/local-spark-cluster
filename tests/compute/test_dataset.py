import src.compute._dataset as D
import pytest

def test_Input_dataframe(monkeypatch, spark_session):
    # Mock thelogger to avoid actual logging
    class MockLogger:
        def info(self, msg):
            pass

    monkeypatch.setattr("compute._dataset.logger", MockLogger())

    # Mock the session property to return the fixture's Spark session
    monkeypatch.setattr(D.Input, "session", spark_session)

    # Mock the __read_file method to return a non-empty DataFrame
    def mock_read_file(self, reader):
        return spark_session.createDataFrame([(1, "test")], ["id", "value"])

    monkeypatch.setattr(D.Input, "_Input__read_file", mock_read_file)

    input_instance = D.Input(path="dummy_path", extension="csv")
    df = input_instance.dataframe()

    assert not df.isEmpty()
    assert df.count() == 1
    assert df.columns == ["id", "value"]


def test_Input__read_file(monkeypatch, spark_session):
    # Mock the logger to avoid actual logging
    class MockLogger:
        def info(self, msg):
            pass

    monkeypatch.setattr("compute._dataset.logger", MockLogger())

    # Mock the session property to return the fixture's Spark session
    monkeypatch.setattr(D.Input, "session", spark_session)

    input_instance = D.Input(path="dummy_path", extension="csv")

    # Mock the reader to simulate reading a CSV file
    class MockReader:
        def csv(self, path, **kwargs):
            return spark_session.createDataFrame([(1, "test")], ["id", "value"])

    reader = MockReader()
    df = input_instance._Input__read_file(reader)

    assert not df.isEmpty()
    assert df.count() == 1
    assert df.columns == ["id", "value"]

def test_Output_write(monkeypatch, spark_session):
    # Mock the logger to avoid actual logging
    class MockLogger:
        def info(self, msg):
            pass

    monkeypatch.setattr("compute._dataset.logger", MockLogger())

    # Mock the session property to return the fixture's Spark session
    monkeypatch.setattr(D.Output, "session", spark_session)

    # Mock the __dump_file method to avoid actual file writing
    def mock_dump_file(self, writer):
        pass

    monkeypatch.setattr(D.Output, "_Output__dump_file", mock_dump_file)

    output_instance = D.Output(path="dummy_path", extension="csv")
    df = spark_session.createDataFrame([(1, "test")], ["id", "value"])
    output_instance.write(df)

    # No assertions needed as we are testing the method execution without errors
