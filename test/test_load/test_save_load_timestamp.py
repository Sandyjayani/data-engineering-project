import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch, MagicMock
from io import StringIO
from src.load.save_load_timestamp import save_timestamps  # Replace with the actual import path

@pytest.fixture(scope="function")
def s3_client():
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket="smith-morra-transformation-bucket",
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        yield s3

@pytest.mark.it("should save a new timestamp to an existing CSV")
def test_save_timestamps_existing_csv(s3_client):
    table_name = "test_table"
    initial_csv_content = "Date\n2024-08-14_14.09.01"
    s3_client.put_object(Bucket="smith-morra-transformation-bucket", Key=f"{table_name}/load_timestamp.csv", Body=initial_csv_content)

    new_timestamp = "2024-08-15_15.10.02"
    save_timestamps(table_name, new_timestamp)

    response = s3_client.get_object(Bucket="smith-morra-transformation-bucket", Key=f"{table_name}/load_timestamp.csv")
    updated_csv_content = response['Body'].read().decode('utf-8')
    expected_csv_content = "Date\n2024-08-14_14.09.01\n2024-08-15_15.10.02\n"
    assert updated_csv_content == expected_csv_content

@pytest.mark.it("should create a new CSV if none exists")
def test_save_timestamps_no_existing_csv(s3_client):
    table_name = "new_table"
    new_timestamp = "2024-08-15_15.10.02"
    save_timestamps(table_name, new_timestamp)

    response = s3_client.get_object(Bucket="smith-morra-transformation-bucket", Key=f"{table_name}/load_timestamp.csv")
    updated_csv_content = response['Body'].read().decode('utf-8')
    expected_csv_content = "Date\n2024-08-15_15.10.02\n"
    assert updated_csv_content == expected_csv_content

