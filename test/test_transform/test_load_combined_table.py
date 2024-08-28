from src.transform.load_combined_tables import load_combined_tables, extract_timestamp
import pytest
from moto import mock_aws
import boto3
from unittest.mock import patch, Mock
from io import BytesIO, StringIO
import os
import pandas as pd
from datetime import datetime


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


class TestExtractTimestamp:
    @pytest.mark.it("Test if extract_timestamp return correct timestamp when valid")
    def test_valid_timestamp(self):
        assert extract_timestamp("dim_staff-2024-08-13_15.57.00.parquet") == datetime(
            2024, 8, 13, 15, 57, 0
        )

    @pytest.mark.it("Test if extract_timestamp return None when invalid")
    def test_invalid_timestamp(self):
        assert extract_timestamp("dim_staff-2024-08-13_15.57.parquet") == datetime(
            1, 1, 1, 0, 0
        )
        assert extract_timestamp("dim_staff-2024-08-13_15.57.00.csv") == datetime(
            1, 1, 1, 0, 0
        )


class TestLoadCombinte:
    @pytest.mark.it(
        "Test if the function return the same table as df with one parquet file"
    )
    def test_same_table_as_df_one_parquet_file(self, mock_client):
        bucket_name = "smith-morra-transformation-bucket"
        table_name = "dim_staff"

        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        expected_df = pd.DataFrame({"id": [1, 2, 3], "char": ["a", "b", "c"]})

        buffer = BytesIO()
        expected_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        test_key1 = "dim_staff/2024/8/13/16-57/dim_staff-2024-08-13_16.57.00.parquet"

        mock_client.put_object(
            Bucket=bucket_name, Key=test_key1, Body=buffer.getvalue()
        )

        result = load_combined_tables(table_name)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert result["id"].tolist() == [1, 2, 3]
        assert result["char"].tolist() == ["a", "b", "c"]

    @pytest.mark.it(
        "Test if the function return the same table as df with one csv file"
    )
    def test_same_table_as_df_one_csv_file(self, mock_client):
        bucket_name = "smith-morra-ingestion-bucket"
        table_name = "department"

        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        expected_df = pd.DataFrame({"id": [1, 2, 3], "char": ["a", "b", "c"]})

        buffer = StringIO()
        expected_df.to_csv(buffer, index=False)
        buffer.seek(0)

        test_key1 = "department/2024/8/13/16-57/department-2024-08-13_16.57.00.csv"

        mock_client.put_object(
            Bucket=bucket_name, Key=test_key1, Body=buffer.getvalue()
        )

        result = load_combined_tables(table_name, bucket_type="ingest")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert result["id"].tolist() == [1, 2, 3]
        assert result["char"].tolist() == ["a", "b", "c"]

    @pytest.mark.it("Test if the function return the same table as df with mulit files")
    def test_same_table_as_df_multi_files(self, mock_client):
        bucket_name = "smith-morra-transformation-bucket"
        table_name = "dim_staff"

        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        expected_df = pd.DataFrame({"id": [1, 2, 3], "char": ["a", "b", "c"]})

        buffer = BytesIO()
        expected_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        test_key1 = "dim_staff/2024/8/13/16-57/dim_staff-2024-08-13_16.57.00.parquet"

        mock_client.put_object(
            Bucket=bucket_name, Key=test_key1, Body=buffer.getvalue()
        )

        expected_df = pd.DataFrame({"id": [4, 5, 6], "char": ["d", "e", "f"]})

        buffer = BytesIO()
        expected_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        test_key2 = "dim_staff/2024/8/13/16-57/dim_staff-2024-08-13_17.27.00.parquet"

        mock_client.put_object(
            Bucket=bucket_name, Key=test_key2, Body=buffer.getvalue()
        )

        result = load_combined_tables(table_name)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 6
        assert result["id"].tolist() == [1, 2, 3, 4, 5, 6]
        assert result["char"].tolist() == ["a", "b", "c", "d", "e", "f"]

        expected_df = pd.DataFrame({"id": [7, 8, 9], "char": ["g", "h", "i"]})

        buffer = BytesIO()
        expected_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        test_key3 = "dim_staff/2024/8/13/16-57/dim_staff-2024-08-13_17.57.00.parquet"

        mock_client.put_object(
            Bucket=bucket_name, Key=test_key3, Body=buffer.getvalue()
        )

        result = load_combined_tables(table_name)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 9
        assert result["id"].tolist() == [1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert result["char"].tolist() == ["a", "b", "c", "d", "e", "f", "g", "h", "i"]

    @pytest.mark.it(
        "Test if the function return the only load data from the specified table"
    )
    def test_same_table_as_df_only_read_from_specified_folder(self, mock_client):
        bucket_name = "smith-morra-ingestion-bucket"
        table_name = "department"

        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        expected_df = pd.DataFrame({"id": [1, 2, 3], "char": ["a", "b", "c"]})

        buffer = StringIO()
        expected_df.to_csv(buffer, index=False)
        buffer.seek(0)

        test_key1 = "department/2024/8/13/16-57/department-2024-08-13_16.57.00.csv"

        mock_client.put_object(
            Bucket=bucket_name, Key=test_key1, Body=buffer.getvalue()
        )

        expected_df2 = pd.DataFrame({"id": [4, 5, 6], "char": ["d", "e", "f"]})

        test_key2 = "other/2024/8/13/16-57/departmet-2024-08-13_16.57.00.csv"

        mock_client.put_object(
            Bucket=bucket_name, Key=test_key2, Body=buffer.getvalue()
        )

        result = load_combined_tables(table_name, bucket_type="ingest")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert result["id"].tolist() == [1, 2, 3]
        assert result["char"].tolist() == ["a", "b", "c"]

    @pytest.mark.it("Test if an empty dataframe is return when no files in the s3")
    def test_no_files(self, mock_client):
        bucket_name = "smith-morra-transformation-bucket"
        table_name = "dim_staff"

        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        assert load_combined_tables("dim_staff").empty
