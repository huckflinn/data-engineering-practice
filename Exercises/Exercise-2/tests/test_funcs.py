import sys
import os
import pytest
import requests
import pandas as pd
from io import StringIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from main import find_csv_filename, get_csv, get_highest_hourly_dry_bulb_temp

def test_find_csv_filename(mocker):
    mock_res = mocker.MagicMock(spec = requests.Response)
    mock_res.content = b'<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">\n<html>\n <head>\n  <title>Index of /data/local-climatological-data/access/2021</title>\n </head>\n <body>\n<h1>Index of /data/local-climatological-data/access/2021</h1>\n  <table>\n   <tr><th><a href="?C=N;O=D">Name</a></th><th><a href="?C=M;O=A">Last modified</a></th><th><a href="?C=S;O=A">Size</a></th><th><a href="?C=D;O=A">Description</a></th></tr>\n   <tr><th colspan="4"><hr></th></tr>\n<tr><td><a href="/data/local-climatological-data/access/">Parent Directory</a></td><td>&nbsp;</td><td align="right">  - </td><td>&nbsp;</td></tr>\n<tr><td><a href="01001099999.csv">01001099999.csv</a></td><td align="right">2024-01-19 09:51  </td><td align="right">4.0M</td><td>&nbsp;</td></tr>'
    mock_res.raise_for_status.return_value = None

    mocker.patch("requests.get", return_value = mock_res)

    res = find_csv_filename("http://www.example.com", "2024-01-19 09:51  ")

    requests.get.assert_called_once_with("http://www.example.com")
    
    assert res == "01001099999.csv"


def test_get_csv(mocker):
    mock_res = mocker.MagicMock(spec = requests.Response)
    mock_res.content = b"Don't get testy with me."
    mock_res.raise_for_status.return_value = None

    mocker.patch("requests.get", return_value = mock_res)

    res = get_csv("http://example.com/", "testfile.csv")

    requests.get.assert_called_once_with("http://example.com/testfile.csv")

    assert res.content == b"Don't get testy with me."


def test_get_highest_hourly_dry_bulb_temp(mocker):
    mock_res = mocker.MagicMock(spec = requests.Response)
    mock_res.content = b'"col1","HourlyDryBulbTemperature"\n"test1",20\n"test2",1'

    mock_df = pd.DataFrame({"col1": ["test1", "test2"], "HourlyDryBulbTemperature": [20, 1]})

    mocker.patch("pandas.read_csv", return_value = mock_df)

    res = get_highest_hourly_dry_bulb_temp(mock_res)

    expected = pd.DataFrame({"col1": ["test1"], "HourlyDryBulbTemperature": [20]})
    pd.testing.assert_frame_equal(res, expected)