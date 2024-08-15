from moto import mock_aws
from src.lambda_handler import lambda_handler
import os
import pytest
from requests import Response
from unittest.mock import patch
import pandas as pd
import boto3


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEAFULT_REGION"] = "eu-west-2"

@pytest.fixture(scope="function")
def mock_aws_client(aws_creds):
    """
    starts mock_aws, creates a mock bucket within it, and yields a mock s3 client
    """
    with mock_aws():
        s3_client = boto3.client('s3')
        s3_client.create_bucket(
            Bucket='smith-morra-ingestion-bucket',
            CreateBucketConfiguration={
                "LocationConstraint": "eu-west-2"
            }
        )
        yield s3_client


@pytest.fixture
def mock_connection():
    with patch("src.lambda_handler.create_connection") as conn:
        yield conn

@pytest.fixture
def mock_table():
    with patch("src.lambda_handler.get_table") as table:
        test_series = pd.Series([1,2,3,4,5])
        test_dataframe = pd.DataFrame({'table data': test_series})
        table.return_value = test_dataframe
        yield table

@pytest.fixture
def mock_upload():
    with patch("src.lambda_handler.upload_tables_to_s3") as upload:
        yield upload

@pytest.fixture
def mock_timestamp():
    with patch("src.lambda_handler.get_timestamp") as timestamp:
        yield timestamp

@pytest.fixture
def mock_logger():
    with patch("src.lambda_handler.setup_logger") as logger:
        yield logger


class TestOutput:


    # check lambda handler returns status code and message
    def test__returns_a_response_object(
        self, mock_aws_client, mock_connection, mock_table, mock_upload, mock_timestamp, mock_logger
    ):
        response = lambda_handler({}, {})
        test_response = Response()
        assert type(test_response) == type(response)

    def test_returns_status_code_201(
        self, mock_aws_client, mock_connection, mock_table, mock_upload, mock_timestamp, mock_logger
    ):
        response = lambda_handler({}, {})
        assert response.status_code == 201


    # def test_returns_success_message(mock_aws_client):
    #     response = lambda_handler({},{})
    #     assert response.text == 'new data was successfully uploaded'


# mock util functions to check they are running
class TestCallUtils:

    def test_db_connection_running(
        self, mock_aws_client, mock_connection, mock_table, mock_upload, mock_timestamp, mock_logger
    ):
        lambda_handler({}, {})
        mock_connection.assert_called()

    def test_get_table_is_running(
        self, mock_aws_client, mock_connection, mock_table, mock_upload, mock_timestamp, mock_logger
    ):
        lambda_handler({}, {})
        mock_table.assert_called()

    def test_upload_is_running(
        self, mock_aws_client, mock_connection, mock_table, mock_upload, mock_timestamp, mock_logger
    ):
        lambda_handler({}, {})
        mock_upload.assert_called()

    def test_timestamp_is_running(
        self, mock_aws_client, mock_connection, mock_table, mock_upload, mock_timestamp, mock_logger
    ):
        lambda_handler({}, {})
        mock_timestamp.assert_called()

    def test_logger_is_running(
        self, mock_aws_client, mock_connection, mock_table, mock_upload, mock_timestamp, mock_logger
    ):
        lambda_handler({}, {})
        mock_logger.assert_called()


# mock s3 bucket to check that it is uploading data
class TestLambdaResult:
    
    def test_uploads_to_s3_bucket(self,mock_aws_client,mock_connection,mock_table,mock_timestamp,mock_logger):
        lambda_handler({}, {})
        bucket_content = mock_aws_client.list_objects(Bucket='smith-morra-ingestion-bucket')
        assert len(bucket_content["Contents"]) > 0

        