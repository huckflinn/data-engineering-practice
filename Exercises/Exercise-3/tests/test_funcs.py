import boto3
from moto import mock_aws
import pytest
import sys
import os
import gzip
from io import BytesIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.main import get_archive, extract_uri, display_cc_content

@pytest.fixture
def s3_mock():
    with mock_aws():
        yield boto3.client("s3", region_name = "us-east-1")

def test_get_archive(s3_mock):
    mock_bucket = "test_bucket"
    mock_key = "test_key"
    mock_body = b"test content"

    s3_mock.create_bucket(Bucket = mock_bucket)
    s3_mock.put_object(Bucket = mock_bucket, Key = mock_key, Body = mock_body)

    res = get_archive(mock_bucket, mock_key)

    assert res is not None
    assert res["Body"].read() == mock_body


def test_extract_uri():
    content = b"uri\nyou test me so good\n"
    compressed_content = gzip.compress(content)

    mock_res = {"Body": BytesIO(compressed_content)}

    uri = extract_uri(mock_res)

    assert uri == "uri"


def test_display_cc_content():
    content = b"It is time\nTo write tests\nOur rhymes\Are the worst\n"
    compressed_content = gzip.compress(content)

    mock_res = {"Body": BytesIO(compressed_content)}

    