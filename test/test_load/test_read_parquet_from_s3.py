import pytest
from unittest.mock import patch, Mock
from datetime import datetime
import pandas as pd
import boto3
from moto import mock_aws
from io import BytesIO
from src.load.read_parquet_from_s3 import read_parquet_from_s3


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


# ... (keep the existing imports and fixtures)


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-2")  # Change to 'eu-west-2'
        yield s3


@pytest.fixture
def mock_get_load_timestamp():
    with patch(
        "src.load.read_parquet_from_s3.get_load_timestamp"
    ) as mock_load_timestamp:
        yield mock_load_timestamp


@pytest.fixture
def mock_get_transformation_timestamp():
    with patch(
        "src.load.read_parquet_from_s3.get_transformation_timestamp"
    ) as mock_transformation_timestamp:
        yield mock_transformation_timestamp


@pytest.fixture
def mock_logger():
    with patch("src.load.read_parquet_from_s3.setup_logger") as mock:
        mock.return_value = Mock()
        yield mock.return_value


@pytest.fixture
def mock_pandas():
    with patch("src.load.read_parquet_from_s3.pd") as mock_panda:
        mock_panda.read_parquet.return_value = pd.DataFrame({"column": [1, 2, 3]})
        yield mock_panda


def create_test_parquet(s3, bucket_name, table_name):
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    try:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    try:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    s3.put_object(
        Bucket=bucket_name,
        Key=f"{table_name}/2024/1/2/12-0/{table_name}-2024-01-02_12.00.00.parquet",
        Body=parquet_buffer.getvalue(),
    )


@pytest.mark.it("Returns a dictionary with correct number of keys")
def test_returns_dictionary(
    s3,
    mock_get_transformation_timestamp,
    mock_get_load_timestamp,
    mock_pandas,
    mock_logger,
):
    mock_get_transformation_timestamp.return_value = datetime(2024, 1, 2, 12, 0, 0)
    mock_get_load_timestamp.return_value = datetime(2024, 1, 1, 12, 0, 0)
    bucket_name = "smith-morra-transformation-bucket"
    table_list = [
        "dim_date",
        "dim_location",
        "dim_design",
        "dim_currency",
        "dim_counterparty",
        "dim_staff",
        "fact_sales_order",
    ]
    for table_name in table_list:
        create_test_parquet(s3, bucket_name, table_name)
    with patch("boto3.client", return_value=s3):
        result = read_parquet_from_s3()
    assert isinstance(result, dict)
    assert len(result) == len(table_list)
    for table_name in table_list:
        assert table_name in result
        assert isinstance(result[table_name], pd.DataFrame)


@pytest.mark.it("Should not read parquet files when no new data is available")
def test_read_parquet_from_s3_no_new_data(
    s3, mock_logger, mock_get_transformation_timestamp, mock_get_load_timestamp
):
    mock_get_transformation_timestamp.return_value = datetime(2023, 1, 2, 12, 0, 0)
    mock_get_load_timestamp.return_value = datetime(2024, 1, 1, 12, 0, 0)
    bucket_name = "smith-morra-transformation-bucket"
    table_list = [
        "dim_date",
        "dim_location",
        "dim_design",
        "fact_sales_order",
        "dim_currency",
        "dim_counterparty",
        "dim_staff",
    ]
    for table_name in table_list:
        create_test_parquet(s3, bucket_name, table_name)
    with patch("boto3.client", return_value=s3):
        result = read_parquet_from_s3()
    assert len(result) == 0
    # Update this assertion to check for any table, not just dim_staff
    mock_logger.info.assert_any_call(
        "No new data to transform for fact_sales_order table."
    )


# @pytest.mark.it("should successfully read parquet files from S3 when new data is available")
# def test_read_parquet_from_s3_success(s3, mock_setup_logger, mock_get_load_timestamp, mock_get_transformation_timestamp):
#     # Setup mock return values
#     mock_get_transformation_timestamp.return_value = datetime(2023, 1, 2, 12, 0, 0)
#     mock_get_load_timestamp.return_value = datetime(2024, 1, 1, 12, 0, 0)
#     # Mock S3 object

#     mock_s3_object = Mock()
#     mock_s3_object.read.return_value = b'mock_parquet_data'
#     s3.get_object.side_effect = [{'Body': mock_s3_object}] * 7
#     # Mock pandas read_parquet
#     mock_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
#     with patch('pandas.read_parquet', return_value=mock_df):
#         result = read_parquet_from_s3()
#     # Assertions
#     assert isinstance(result, dict)
#     assert len(result) == 7  # Number of tables in table_list
#     for table_name in ["dim_date", "dim_location", "dim_design", "dim_currency", "dim_counterparty", "dim_staff", "fact_sales_order"]:
#         assert table_name in result
#         assert isinstance(result[table_name], pd.DataFrame)
#         assert result[table_name].equals(mock_df)


@pytest.mark.it("should read parquet files from S3 when new data is available")
def test_read_parquet_from_s3_new_data(
    s3,
    mock_get_transformation_timestamp,
    mock_get_load_timestamp,
    mock_logger,
    mock_pandas,
):
    mock_get_transformation_timestamp.return_value = datetime(2024, 1, 2, 12, 0, 0)
    mock_get_load_timestamp.return_value = datetime(2024, 1, 1, 12, 0, 0)
    bucket_name = "smith-morra-transformation-bucket"
    table_list = [
        "dim_date",
        "dim_location",
        "dim_design",
        "fact_sales_order",
        "dim_currency",
        "dim_counterparty",
        "dim_staff",
    ]
    for table_name in table_list:
        create_test_parquet(s3, bucket_name, table_name)
    objects_s3 = s3.list_objects_v2(Bucket=bucket_name)
    with patch("boto3.client", return_value=s3):
        result = read_parquet_from_s3()
    assert len(result) == 7
    for table_name in table_list:
        assert table_name in result.keys()
        assert isinstance(result[table_name], pd.DataFrame)
    assert mock_pandas.read_parquet.call_count == 7
    mock_logger.info.assert_called()


@pytest.mark.it("should raise an exception when an error occurs during get_object")
def test_read_parquet_from_s3_error(s3):
    bucket_name = "smith-morra-transformation-bucket"
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},  # Add this line
    )
    with patch("boto3.client", return_value=s3):  # Add this line
        with patch.object(s3, "get_object", side_effect=Exception("S3 error")):
            with pytest.raises(Exception) as exc_info:
                read_parquet_from_s3()
    assert str(exc_info.value) == "S3 error"
