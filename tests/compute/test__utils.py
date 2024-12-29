import pytest
import os
from compute._utils import (
    filter_kwargs,
    get_file_extension,
    sanitize_columns,
    extract_file_name,
    list_folder_contents
)


@pytest.mark.parametrize("kwargs, expected, type_filter", [
    ({}, {}, int),
    ({"a": 1, "b": "string", "c": 3.0, "d": 4}, {"a": 1, "d": 4}, int),
    ({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 3}, int),
    ({"a": "string", "b": 3.0, "c": []}, {}, int),
    ({"a": 1, "b": "string", "c": 3.0, "d": [1, 2, 3]}, {"d": [1, 2, 3]}, list),
])
def test_filter_kwargs(kwargs, expected, type_filter):
    assert filter_kwargs(kwargs, type_filter) == expected


@pytest.mark.parametrize("filename, expected_extension", [
    ("example.txt", "txt"),
    ("example.TXT", "txt"),
    ("example.tar.gz", "gz"),
    ("example", None),
    ("example.", ""),
    (".hiddenfile", "hiddenfile"),
])
def test_get_file_extension(filename, expected_extension):
    assert get_file_extension(filename) == expected_extension


# # # def test_write_to_one_csv(monkeypatch):
# # #     def mock_os_system(command):
# # #         if "cat" in command:
# # #             assert command == "cat test_path/*p*.csv >> test_path.csv"
# # #         elif "rm -rf" in command:
# # #             assert command == "rm -rf test_path"
# # #         return 0

# # #     def mock_shutil_rmtree(path):
# # #         assert path == "test_path"

# # #     monkeypatch.setattr(os, "system", mock_os_system)
# # #     monkeypatch.setattr(shutil, "rmtree", mock_shutil_rmtree)


@pytest.mark.parametrize("columns, expected_columns", [
    (["col@1", "col#2"], ["col_1", "col_2"]),
    (["col 1", "col 2"], ["col_1", "col_2"]),
    (["col-1", "col.2"], ["col_1", "col_2"]),
])
def test_sanitize_columns(spark_session, columns, expected_columns):
    df = spark_session.createDataFrame([("value1", "value2")], columns)
    sanitized_df = sanitize_columns(df)
    assert sanitized_df.columns == expected_columns

@pytest.mark.parametrize("path, expected_file_name", [
    ("/path/to/file.txt", "file"),
    ("/another/path/to/file.tar.gz", "file.tar"),
    ("/yet/another/path/to/file", "file"),
    ("file.txt", "file"),
    ("/path/to/.hiddenfile", ".hiddenfile"),
    ("/path/to/", None),
    ("", None),
])
def test_extract_file_name(path, expected_file_name):
    assert extract_file_name(path) == expected_file_name


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
    assert list_folder_contents("existing_folder") == ["file1.txt", "file2.txt", "subfolder"]

    # Test non-existing folder
    assert list_folder_contents("non_existing_folder") == []

    # Test folder with no permission
    assert list_folder_contents("no_permission_folder") == []
