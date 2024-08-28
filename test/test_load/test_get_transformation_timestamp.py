import pytest
import boto3
from botocore.exceptions import ClientError
from moto import mock_aws
from unittest.mock import patch
from src.load.get_transformation_timestamp import get_transformation_timestamp
import pandas as pd
from datetime import datetime
from io import StringIO

@pytest.fixture(scope="function")
def s3_client():
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket="smith-morra-transformation-bucket",
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        yield s3

@pytest.mark.it("should return the latest timestamp from the CSV")
def test_get_transformation_timestamp_success(s3_client):
    table_name = "test_table"
    csv_content = "Date\n2024-08-14_14.09.01\n2024-08-15_15.10.02"
    s3_client.put_object(Bucket="smith-morra-transformation-bucket", Key=f"{table_name}/timestamps.csv", Body=csv_content)

    timestamp = get_transformation_timestamp(table_name)
    expected_timestamp = datetime.strptime("2024-08-15_15.10.02", "%Y-%m-%d_%H.%M.%S")
    assert timestamp == expected_timestamp

@pytest.mark.it("should return default timestamp if no CSV exists")
def test_get_transformation_timestamp_no_csv(s3_client):
    table_name = "non_existent_table"
    timestamp = get_transformation_timestamp(table_name)
    expected_timestamp = datetime.strptime("0001-01-01_01.01.01", "%Y-%m-%d_%H.%M.%S")
    assert timestamp == expected_timestamp

@pytest.mark.it("should handle ClientError for missing file")
@patch('boto3.client')
def test_get_transformation_timestamp_client_error(mock_boto_client):
    mock_boto_client.return_value.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}},
        "GetObject"
    )
    table_name = "test_table"
    timestamp = get_transformation_timestamp(table_name)
    expected_timestamp = datetime.strptime("0001-01-01_01.01.01", "%Y-%m-%d_%H.%M.%S")
    assert timestamp == expected_timestamp

@pytest.mark.it("should raise exception for unexpected errors")
@patch('src.load.get_transformation_timestamp.boto3.client')
def test_get_transformation_timestamp_unexpected_error(mock_boto_client):
    mock_boto_client.return_value.get_object.side_effect = Exception("Unexpected error")
    table_name = "test_table"
    with pytest.raises(Exception, match="Unexpected error"):
        get_transformation_timestamp(table_name)
