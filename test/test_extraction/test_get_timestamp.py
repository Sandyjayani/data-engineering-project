from src.extraction.get_timestamp import get_timestamp as gt
from botocore.exceptions import ClientError
from datetime import datetime
import pytest
from moto import mock_aws
import os
import boto3
from unittest.mock import patch


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEAFULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="smith-morra-ingestion-bucket",
            CreateBucketConfiguration={
                "LocationConstraint": "eu-west-2",
            },
        )
        yield s3_client


@pytest.mark.it("Test returns datetime timestamp")
def test_returns_datetime(s3_client):
    table_name = "test_table"
    with open("test/test_extraction/test_timestamps.csv", "r", encoding="utf-8") as f:
        s3_client.put_object(
            Bucket="smith-morra-ingestion-bucket",
            Key=f"{table_name}/timestamps.csv",
            Body=f.read(),
        )
    assert gt(table_name) == datetime(2024, 8, 19, 9, 55, 38)


@pytest.mark.it("Returns 0 AD if no timestamps")
def test_no_timestamps(s3_client):
    table_name = "test_table"
    assert gt(table_name) == datetime(1, 1, 1, 1, 1, 1)


@pytest.mark.it("Test ClientError handling")
def test_client_error(s3_client):
    with patch("boto3.client") as mock_client:
        mock_client.return_value.get_object.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "NoSuchBucket",
                    "Message": "The specified bucket does not exist",
                }
            },
            operation_name="GetObject",
        )

        table_name = "test_table"
        with pytest.raises(ClientError):
            gt(table_name)


@pytest.mark.it("Handles unexpected exceptions")
def test_unexpected_exception(s3_client):
    with patch("boto3.client", side_effect=Exception("Unexpected error")):
        table_name = "test_table"
        with pytest.raises(Exception):
            gt(table_name)
