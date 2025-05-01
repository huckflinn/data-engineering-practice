import pytest
import zipfile
import requests
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.main import downloads_dir_exists, get_data, get_csv_name, csv_in_zip_archive

def test_downloads_dir_exists(mocker):
    # Create mock of Path object to simulate when downloads dir does not exist.
    mock_path = mocker.MagicMock(spec = Path)
    mock_path.exists.return_value = False
    mock_path.__truediv__.return_value = mock_path

    # Confirm that downloads folder was successfully created.
    downloads_dir_exists(mock_path)
    mock_path.mkdir.assert_called_once()

    # Confirm that func doesn't run when dir exists.
    mock_path.reset_mock()
    mock_path.exists.return_value = True
    downloads_dir_exists(mock_path)
    mock_path.mkdir.assert_not_called()

    # Test error cases
    mock_path.reset_mock()
    mock_path.exists.return_value = False
    mock_path.__truediv__.return_value = mock_path
    mock_path.mkdir.side_effect = FileExistsError("Exists")
    with pytest.raises(FileExistsError):
        downloads_dir_exists(mock_path)


@pytest.mark.parametrize("uri,expected", [
    ("http://example.com/example.zip", "example.csv"),
    ("http://example.com/longer/path/example.zip", "example.csv"),
    ("http://example.com/example.csv", "example.csv")
])
def test_get_csv_name(uri, expected):
    assert get_csv_name(uri) == expected


def test_csv_in_zip_archive(mocker):
    mock_zip = mocker.MagicMock(spec = zipfile.ZipFile)
    mock_zip.namelist.return_value = [
        "test1.csv",
        "subdir/test2.csv",
        "other.txt"
    ]

    assert csv_in_zip_archive(mock_zip, "test1.csv") is True

    assert csv_in_zip_archive(mock_zip, "test2.csv") is True

    assert csv_in_zip_archive(mock_zip, "missing.csv") is False

    mock_zip.namelist.assert_called()


# def test_get_data_success(mocker):
#     # Create a mock zip archive to be returned from get request
#     mock_res = mocker.MagicMock(spec = requests.Response)
#     mock_res.content = b"zip_content" # Set its contents to byte content to feed into BytesIO
#     mock_res.raise_for_status.return_value = None
#     # Ensure our mock response is returned when requests.get() is invoked
#     mocker.patch("requests.get", return_value = mock_res)

#     # Create a mock ZipFile and ensure it is returned when zipfile.ZipFile() is invoked
#     mock_zip = mocker.MagicMock(spec = zipfile.ZipFile)
#     mocker.patch("zipfile.ZipFile", return_value = mock_zip)

#     # Create a mock Path object and modify the behavior of the "/" operator so it returns a mock Path object
#     mock_path = mocker.MagicMock(spec = Path)
#     mock_path.exists.return_value = False
#     mock_path.__truediv__.return_value = mock_path

#     # Ensure that when the function calls csv_in_zip_archive, it returns True
#     mocker.patch("src.main.csv_in_zip_archive", return_value = True)

#     mocker.patch("src.main.get_csv_name", return_value = "example.csv")

#     print(f"Expected CSV name: {get_csv_name("http://example.com/example.zip")}")

#     # Call the function being tested
#     get_data("http://example.com/example.zip", mock_path)

#     requests.get.assert_called_once_with("http://example.com/example.zip")
#     mock_zip.extract.assert_called_once_with(
#         "example.csv",  # This must match
#         path = mock_path / "downloads"
#     )

def test_get_data_success(mocker):
    # 1. Setup all mocks FIRST
    mock_res = mocker.MagicMock()
    mock_res.content = b"zip_content"
    mock_res.raise_for_status.return_value = None
    mocker.patch("requests.get", return_value=mock_res)

    mock_zip = mocker.MagicMock()
    mocker.patch("zipfile.ZipFile", return_value=mock_zip)

    # 2. CRITICAL: Mock Path behavior PROPERLY
    mock_path = mocker.MagicMock()
    mock_path.__truediv__.side_effect = lambda x: mock_path  # Simulate path joining
    mock_path.exists.return_value = False  # File doesn't exist
    
    # 3. Mock ALL helper functions
    mocker.patch("src.main.get_csv_name", return_value="example.csv")
    mocker.patch("src.main.csv_in_zip_archive", return_value=True)

    # 4. Execute
    get_data("http://example.com/file.zip", mock_path)

    # 5. Verify
    mock_zip.extract.assert_called_once_with(
        "example.csv", 
        path=mock_path / "downloads"
    )


def test_get_data_http_error(mocker):
    mock_res = mocker.MagicMock(spec = requests.Response)
    mock_res.raise_for_status.side_effect = requests.HTTPError("404 Not Found")

    mocker.patch("requests.get", return_value = mock_res)

    mock_path = mocker.MagicMock(spec = Path)

    get_data("http://example.com/example.zip", mock_path)

    mock_res.raise_for_status.assert_called_once()