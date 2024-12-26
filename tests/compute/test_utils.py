import os
import shutil

from pyspark.sql import SparkSession

import src.compute._utils as utils


class TestFilterKwargs:
    def test_empty_dict(self):
        assert utils.filter_kwargs({}, int) == {}

    def test_mixed_types(self):
        kwargs = {"a": 1, "b": "string", "c": 3.0, "d": 4}
        expected = {"a": 1, "d": 4}
        assert utils.filter_kwargs(kwargs, int) == expected

    def test_all_match(self):
        kwargs = {"a": 1, "b": 2, "c": 3}
        expected = {"a": 1, "b": 2, "c": 3}
        assert utils.filter_kwargs(kwargs, int) == expected

    def test_none_match(self):
        kwargs = {"a": "string", "b": 3.0, "c": []}
        assert utils.filter_kwargs(kwargs, int) == {}

    def test_different_type(self):
        kwargs = {"a": 1, "b": "string", "c": 3.0, "d": [1, 2, 3]}
        expected = {"d": [1, 2, 3]}
        assert utils.filter_kwargs(kwargs, list) == expected


class TestSparkSession:

    def test_spark_session_default(self):
        session = utils.spark_session("test_app")
        assert isinstance(session, SparkSession)
        assert session.sparkContext.appName == "test_app"
        assert session.conf.get("spark.master") == os.environ.get("SPARK_MASTER_URL", "spark://localhost:7077")
        session.stop()

    def test_spark_session_with_conf(self):
        conf = {
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
        }
        session = utils.spark_session("test_app_with_conf", conf)
        assert isinstance(session, SparkSession)
        assert session.sparkContext.appName == "test_app_with_conf"
        assert session.conf.get("spark.executor.memory") == "2g"
        assert session.conf.get("spark.executor.cores") == "2"
        session.stop()


def test_get_file_extension():
    assert utils.get_file_extension("example.txt") == "txt"
    assert utils.get_file_extension("example.TXT") == "txt"
    assert utils.get_file_extension("example.tar.gz") == "gz"
    assert utils.get_file_extension("example") is None
    assert utils.get_file_extension("example.") == ""
    assert utils.get_file_extension(".hiddenfile") == "hiddenfile"


def test_write_to_one_csv(monkeypatch):
    def mock_os_system(command):
        if "cat" in command:
            assert command == "cat test_path/*p*.csv >> test_path.csv"
        elif "rm -rf" in command:
            assert command == "rm -rf test_path"
        return 0

    def mock_shutil_rmtree(path):
        assert path == "test_path"

    monkeypatch.setattr(os, "system", mock_os_system)
    monkeypatch.setattr(shutil, "rmtree", mock_shutil_rmtree)


def test_sanitize_columns(spark_session):
    data = [("value1", "value2")]
    columns = ["col@1", "col#2"]
    df = spark_session.createDataFrame(data, columns)

    sanitized_df = utils.sanitize_columns(df)

    expected_columns = ["col_1", "col_2"]
    assert sanitized_df.columns == expected_columns


def test_extract_file_name():
    assert utils.extract_file_name("/path/to/file.txt") == "file"
    assert utils.extract_file_name("/another/path/to/file.tar.gz") == "file.tar"
    assert utils.extract_file_name("/yet/another/path/to/file") == "file"
    assert utils.extract_file_name("file.txt") == "file"
    assert utils.extract_file_name("/path/to/.hiddenfile") == ".hiddenfile"
    assert utils.extract_file_name("/path/to/") is None
    assert utils.extract_file_name("") is None


def test_list_folder_contents(monkeypatch):
    def mock_listdir(path):
        if path == "existing_folder":
            return ["file1.txt", "file2.txt", "subfolder"]
        if path == "non_existing_folder":
            raise FileNotFoundError
        if path == "no_permission_folder":
            raise PermissionError
        return None

    monkeypatch.setattr(os, "listdir", mock_listdir)

    # Test existing folder
    assert utils.list_folder_contents("existing_folder") == ["file1.txt", "file2.txt", "subfolder"]

    # Test non-existing folder
    assert utils.list_folder_contents("non_existing_folder") == []

    # Test folder with no permission
    assert utils.list_folder_contents("no_permission_folder") == []
