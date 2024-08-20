import pytest
from src.extraction.upload_to_s3_util_func import upload_tables_to_s3, save_timestamps
from moto import mock_aws
import boto3
from botocore.exceptions import ClientError
from unittest.mock import patch, Mock
import pandas as pd
import os
from datetime import datetime
from io import StringIO


@pytest.fixture()
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def mock_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")


class TestUploadToS3:
    @pytest.mark.it("Test if the correct confirmation message is returned")
    @patch("src.extraction.upload_to_s3_util_func.datetime")
    def test_confirmation_message_is_returned(self, mock_datetime, mock_client):

        test_table = "test_table"
        test_bucket = "test_bucket"
        test_key = "test_table/2024/8/13/16-57/test_table-2024-08-13_16.57.00.csv"

        mock_client.create_bucket(
            Bucket=test_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_now = datetime(2024, 8, 13, 16, 57, 00)
        mock_datetime.now.return_value = mock_now

        mock_df = Mock(spec=pd.DataFrame)
        mock_df.to_csv.return_value = "mock_csv"

        expected = (
            f"Table {test_table} has been uploaded to {test_bucket} "
            f"with key {test_key}."
        )
        assert upload_tables_to_s3(mock_df, test_table, test_bucket) == expected

    @pytest.mark.it("Test if csv file being uploaded to the given bucket")
    @patch("src.extraction.upload_to_s3_util_func.datetime")
    def test_csv_files_being_uploaded(self, mock_datetime, mock_client):
        test_table = "test_table"
        test_bucket = "test_bucket"
        test_key = "test_table/2024/8/13/16-57/test_table-2024-08-13_16.57.00.csv"

        mock_client.create_bucket(
            Bucket=test_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_now = datetime(2024, 8, 13, 16, 57, 00)
        mock_datetime.now.return_value = mock_now

        mock_df = Mock(spec=pd.DataFrame)
        mock_df.to_csv.return_value = "mock_csv"

        upload_tables_to_s3(mock_df, test_table, test_bucket)

        response = mock_client.list_objects_v2(Bucket=test_bucket)

        assert response["KeyCount"] == 2
        assert response["Contents"][0]["Key"] == test_key

        mock_now = datetime(2024, 8, 13, 17, 27)
        mock_datetime.now.return_value = mock_now

        test_key2 = "test_table/2024/8/13/17-27/test_table-2024-08-13_17.27.00.csv"

        upload_tables_to_s3(mock_df, test_table, test_bucket)

        response2 = mock_client.list_objects_v2(Bucket=test_bucket)
        assert response2["KeyCount"] == 3
        assert response2["Contents"][1]["Key"] == test_key2

    @pytest.mark.it("Test if upload as parquet files if transform in bucket name")
    @patch("src.extraction.upload_to_s3_util_func.datetime")
    def test_parquet_files_being_uploaded_if(self, mock_datetime, mock_client):
        test_table = "test_table"
        test_bucket = "transformation_bucket"
        test_key = "test_table/2024/8/13/16-57/test_table-2024-08-13_16.57.00.parquet"

        mock_client.create_bucket(
            Bucket=test_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_now = datetime(2024, 8, 13, 16, 57, 00)
        mock_datetime.now.return_value = mock_now

        mock_df = Mock(spec=pd.DataFrame)
        mock_df.to_csv.return_value = "mock_csv"

        upload_tables_to_s3(mock_df, test_table, test_bucket)

        response = mock_client.list_objects_v2(Bucket=test_bucket)

        assert response["KeyCount"] == 2
        assert response["Contents"][0]["Key"] == test_key

        mock_now = datetime(2024, 8, 13, 17, 27)
        mock_datetime.now.return_value = mock_now

        test_key2 = "test_table/2024/8/13/17-27/test_table-2024-08-13_17.27.00.parquet"

        upload_tables_to_s3(mock_df, test_table, test_bucket)

        response2 = mock_client.list_objects_v2(Bucket=test_bucket)
        assert response2["KeyCount"] == 3
        assert response2["Contents"][1]["Key"] == test_key2

    @pytest.mark.it("Excepts and raises client error if raised during execution")
    @patch("src.extraction.upload_to_s3_util_func.boto3")
    def test_excepts_raises_client_error(self, mock_boto_3, mock_client):
        mock_boto_3.client.side_effect = ClientError(
            error_response={"Error": {"Code": "DecryptionFailureException"}},
            operation_name="Test",
        )
        test_table = "test_table"
        test_bucket = "test_bucket"
        with pytest.raises(ClientError):
            upload_tables_to_s3(None, test_table, test_bucket)


class TestSaveTimestamps:
    @pytest.mark.it("Test new_timestamp is created when no timestamp table")
    def test_new_timestamp(self, mock_client):
        test_table = "test_table_timestamp"
        test_bucket = "test_bucket_timestamp"
        test_timestamp = "2024-08-13_17-27"

        mock_client.create_bucket(
            Bucket=test_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        save_timestamps(test_table, test_timestamp, test_bucket)

        response = mock_client.get_object(
            Bucket=test_bucket, Key=f"{test_table}/timestamps.csv"
        )
        content = response["Body"].read().decode("utf-8")
        expected_df = pd.DataFrame({"Date": [test_timestamp]})
        expected_csv = expected_df.to_csv(index=False)
        assert content == expected_csv

    @pytest.mark.it("Test if it appends to an existing timestamp table")
    def test_append_timestamp(self, mock_client):
        test_table = "test_table_timestamp"
        test_bucket = "test_bucket_timestamp"
        test_initial_timestamp = "2024-08-13_17-27"
        test_new_timestamp = "2024-08-13_17-57"

        mock_client.create_bucket(
            Bucket=test_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        initial_df = pd.DataFrame({"Date": [test_initial_timestamp]})

        initial_csv_buffer = StringIO()
        initial_df.to_csv(initial_csv_buffer, index=False)
        initial_csv_buffer.seek(0)

        save_timestamps(test_table, test_initial_timestamp, test_bucket)

        new_df = pd.DataFrame({"Date": [test_new_timestamp]})
        new_csv_buffer = StringIO()
        new_df.to_csv(new_csv_buffer, index=False)
        new_csv_buffer.seek(0)

        save_timestamps(test_table, test_new_timestamp, test_bucket)

        response = mock_client.get_object(
            Bucket=test_bucket, Key=f"{test_table}/timestamps.csv"
        )
        content = response["Body"].read().decode("utf-8")
        expected_df = pd.DataFrame(
            {"Date": [test_initial_timestamp, test_new_timestamp]}
        )
        expected_csv = expected_df.to_csv(index=False)
        assert content == expected_csv
