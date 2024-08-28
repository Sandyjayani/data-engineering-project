from moto import mock_aws
from src.extraction.extraction import lambda_handler
import os
import pytest
from unittest.mock import patch
import pandas as pd
import boto3
from pg8000.native import DatabaseError


@pytest.fixture
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEAFULT_REGION"] = "eu-west-2"


@pytest.fixture
def mock_aws_client(aws_creds):
    """
    starts mock_aws, creates a mock bucket within it, and yields a mock s3 client
    """
    with mock_aws():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="smith-morra-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3_client


@pytest.fixture
def mock_connection():
    with patch("src.extraction.extraction.create_connection") as conn:
        yield conn


@pytest.fixture
def mock_table():
    with patch("src.extraction.extraction.get_table") as table:
        test_series = pd.Series([1, 2, 3, 4, 5])
        test_dataframe = pd.DataFrame({"table data": test_series})
        table.return_value = test_dataframe
        yield table


@pytest.fixture
def mock_upload():
    with patch("src.extraction.extraction.upload_tables_to_s3") as upload:
        yield upload


@pytest.fixture
def mock_timestamp():
    with patch("src.extraction.extraction.get_timestamp") as timestamp:
        yield timestamp


@pytest.fixture
def mock_logger():
    with patch("src.extraction.extraction.setup_logger") as logger:
        yield logger


# check lambda handler returns status code and message
class TestOutput:

    def test_returns_status_code_200(
        self,
        mock_aws_client,
        mock_connection,
        mock_table,
        mock_upload,
        mock_timestamp,
        mock_logger,
    ):
        response = lambda_handler({}, {})
        assert response["statusCode"] == 200

    def test_returns_success_message(
        self,
        mock_aws_client,
        mock_connection,
        mock_table,
        mock_upload,
        mock_timestamp,
        mock_logger,
    ):
        response = lambda_handler({}, {})
        assert (
            response["body"]
            == "Extraction lambda executed successfully: new data ingested"
        )

    def test_response_if_no_data_ingested(
        self,
        mock_aws_client,
        mock_connection,
        mock_table,
        mock_upload,
        mock_timestamp,
        mock_logger,
    ):
        mock_table.return_value = None
        response = lambda_handler({}, {})
        assert (
            response["body"]
            == "Extraction lambda executed successfully: no new data ingested"
        )


# mock util functions to check they are running
class TestCallUtils:
    def test_db_connection_running(
        self,
        mock_aws_client,
        mock_connection,
        mock_table,
        mock_upload,
        mock_timestamp,
        mock_logger,
    ):
        lambda_handler({}, {})
        mock_connection.assert_called()

    def test_get_table_is_running(
        self,
        mock_aws_client,
        mock_connection,
        mock_table,
        mock_upload,
        mock_timestamp,
        mock_logger,
    ):
        lambda_handler({}, {})
        mock_table.assert_called()

    def test_upload_is_running(
        self,
        mock_aws_client,
        mock_connection,
        mock_table,
        mock_upload,
        mock_timestamp,
        mock_logger,
    ):
        lambda_handler({}, {})
        mock_upload.assert_called()

    def test_timestamp_is_running(
        self,
        mock_aws_client,
        mock_connection,
        mock_table,
        mock_upload,
        mock_timestamp,
        mock_logger,
    ):
        lambda_handler({}, {})
        mock_timestamp.assert_called()

    def test_logger_is_running(
        self,
        mock_aws_client,
        mock_connection,
        mock_table,
        mock_upload,
        mock_timestamp,
        mock_logger,
    ):
        lambda_handler({}, {})
        mock_logger.assert_called()


# mock s3 bucket to check that it is uploading data
class TestLambdaResult:
    def test_uploads_to_s3_bucket(
        self, mock_aws_client, mock_connection, mock_table, mock_timestamp, mock_logger
    ):
        lambda_handler({}, {})
        bucket_content = mock_aws_client.list_objects(
            Bucket="smith-morra-ingestion-bucket"
        )
        assert len(bucket_content["Contents"]) > 0


# mock errors & logger to check that errors are raised & logged
class TestSadPath:
    def test_raises_error_for_error(
        self,
        mock_aws_client,
        mock_connection,
        mock_logger,
        mock_table,
        mock_timestamp,
        mock_upload,
    ):
        mock_table.side_effect = DatabaseError()
        with pytest.raises(DatabaseError):
            assert lambda_handler({}, {})

    def test_logs_errors(
        self,
        mock_aws_client,
        mock_connection,
        mock_logger,
        mock_table,
        mock_timestamp,
        mock_upload,
    ):

        class LogStorage:
            info_logs = []
            warning_logs = []
            critical_logs = []

            def info(self, message):
                self.info_logs.append(message)

            def warning(self, message):
                print("this is being called")
                self.warning_logs.append(message)

            def critical(self, message):
                self.critical_logs.append(message)

        log_storage = LogStorage()
        mock_logger.return_value = log_storage

        mock_table.side_effect = DatabaseError()
        with pytest.raises(DatabaseError):
            lambda_handler({}, {})

        assert "Critical error: " in log_storage.critical_logs[0]
