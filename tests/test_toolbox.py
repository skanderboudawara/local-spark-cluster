import pytest
import os
from compute.toolbox import get_filename, filter_kwargs, get_file_extension, sanitize_columns, list_folder_contents

def test_filter_kwargs():
    kwargs = {'a': 1, 'b': 'string', 'c': 3.0, 'd': [1, 2, 3]}
    expected = {'a': 1, 'c': 3.0}
    assert filter_kwargs(kwargs, int) == {'a': 1}
    assert filter_kwargs(kwargs, float) == {'c': 3.0}
    assert filter_kwargs(kwargs, list) == {'d': [1, 2, 3]}

@pytest.mark.parametrize("filename, expected_extension", [
    ("example.txt", "txt"),
    ("example.TXT", "txt"),
    ("example.tar.gz", "gz"),
    ("example", "example"),
    ("example.", ""),
    (".hiddenfile", "hiddenfile"),
])
def test_get_file_extension(filename, expected_extension):
    assert get_file_extension(filename) == expected_extension

def test_sanitize_columns(spark_session):
    data = [("value1", "value2")]
    columns = ["col 1", "col-2"]
    expected_columns = ["col_1", "col_2"]
    df = spark_session.createDataFrame(data, schema=columns)
    sanitized_df = sanitize_columns(df)
    assert sanitized_df.columns == expected_columns

# def test_list_folder_contents(monkeypatch):
#     def mock_listdir(path):
#         if path == "existing_folder":
#             return ["file1.txt", "file2.txt", "subfolder"]
#         if path == "non_existing_folder":
#             raise FileNotFoundError
#         if path == "no_permission_folder":
#             raise PermissionError
#         return []

#     monkeypatch.setattr(os, "listdir", mock_listdir)

#     assert list_folder_contents("existing_folder") == ["file1.txt", "file2.txt", "subfolder"]
#     assert list_folder_contents("non_existing_folder") == []
#     assert list_folder_contents("no_permission_folder") == []


@pytest.mark.parametrize("path, expected_file_name", [
    ("/path/to/file.txt", "file"),
    ("/another/path/to/file.tar.gz", "file.tar"),
    ("/yet/another/path/to/file", "file"),
    ("file.txt", "file"),
    ("/path/to/.file", ".file"),
    ("/path/to/", None),
    ("", None),
])
def test_get_filename(path, expected_file_name):
    assert get_filename(path) == expected_file_name


# def test_b():
#     print("c")

