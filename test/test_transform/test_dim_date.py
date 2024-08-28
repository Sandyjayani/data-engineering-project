from src.transform.dim_date import generate_date_table, transform_date
from moto import mock_aws
import pytest
import pandas as pd
import boto3
import unittest
import os
from unittest.mock import patch


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


class TestGenerateDate:
    @pytest.mark.it(
        "should create a date dimension DataFrame with the correct structure"
    )
    def test_generate_date_table(self):
        df = generate_date_table()

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert list(df.columns) == [
            "date_id",
            "year",
            "month",
            "day",
            "day_of_week",
            "day_name",
            "month_name",
            "quarter",
        ]
        assert df["date_id"].min() == pd.to_datetime("2022-01-01")
        assert df["date_id"].max() == pd.to_datetime("2024-12-31")
        assert df["year"].min() == 2022
        assert df["year"].max() == 2024


class TestTransformDate:
    @pytest.mark.it("test if it return None when the date folder exists")
    def test_transform_date_table_not_exist(self, mock_client):
        bucket_name = "smith-morra-transformation-bucket"
        folder_prefix = "dim_date/"

        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        mock_client.put_object(
            Bucket=bucket_name, Key=f"{folder_prefix}test_file.parqueet"
        )

        result = transform_date()
        assert result == None

    @pytest.mark.it("test if it return df when the date folder does not exist")
    @patch("src.transform.dim_date.generate_date_table")
    def test_transform_date_table_exist(self, mock_generate_date_data, mock_client):
        bucket_name = "smith-morra-transformation-bucket"
        folder_prefix = "dim_date/"

        mock_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_df = pd.DataFrame({"date": pd.date_range(start="2024-01-01", periods=365)})

        mock_generate_date_data.return_value = mock_df

        result = transform_date()
        pd.testing.assert_frame_equal(result, mock_df)
