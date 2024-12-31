import pytest
import os
from compute.toolbox import list_folder_contents

def test_list_folder_contents(monkeypatch):
    def mock_listdir(path):
        if path == "existing_folder":
            return ["file1.txt", "file2.txt", "subfolder"]
        if path == "non_existing_folder":
            raise FileNotFoundError
        return []

    monkeypatch.setattr(os, "listdir", mock_listdir)

    assert list_folder_contents("existing_folder") == ["file1.txt", "file2.txt", "subfolder"]
    with pytest.raises(FileNotFoundError):
        list_folder_contents("non_existing_folder")


