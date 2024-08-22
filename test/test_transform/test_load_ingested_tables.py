import pytest
from src.transform.load_ingested_tables import load_ingested_tables
import pandas as pd
import os
import boto3
from moto import mock_aws
from io import BytesIO
from unittest.mock import patch
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

@patch("src.transform.load_ingested_tables.get_transformation_timestamp")
@patch("src.transform.load_ingested_tables.get_ingestion_timestamp")
def test_load_multiple_tables_from_s3(mock_get_timestamp, mock_get_transformation_timestamp, mock_client):
    test_bucket = "smith-morra-ingestion-bucket"
    mock_get_transformation_timestamp.return_value = datetime(2003, 8, 13, 16, 57, 00)
    mock_get_timestamp.return_value = datetime(2024, 8, 13, 16, 57, 00)
    mock_client.create_bucket(
        Bucket=test_bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

    # test_key = "test_table/2024/8/13/16-57/test_table-2024-08-13_16.57.00.csv"
    TABLE_NAMES = [
        "payment",
        "payment_type",
        "currency",
        "staff",
        "department",
        "purchase_order",
        "transaction",
        "sales_order",
        "design",
        "address",
        "counterparty",
    ]

    for table in TABLE_NAMES:
        df = pd.DataFrame({
            'id':[1,2,3],
            'value':['A', 'B', 'C']
        })

        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        mock_client.put_object(Bucket=test_bucket, Key=f'{table}/2024/8/13/16-57/{table}-2024-08-13_16.57.00.csv', Body=csv_buffer.getvalue())  # noqa: E501

    response = load_ingested_tables()

    assert isinstance(response, dict)
    for table in TABLE_NAMES:
        df = response[table]
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert list(df.columns) == ['id', 'value']
        assert df['id'].to_list() == [1,2,3]
        assert df['value'].tolist() == ['A', 'B', 'C']


@patch("src.transform.load_ingested_tables.get_transformation_timestamp")
@patch("src.transform.load_ingested_tables.get_ingestion_timestamp")
def test_if_timestamp_are_same(mock_get_timestamp, mock_get_transformation_timestamp, mock_client, caplog):
    test_bucket = "smith-morra-ingestion-bucket"
    mock_get_transformation_timestamp.return_value = datetime(2024, 8, 13, 16, 57, 00)
    mock_get_timestamp.return_value = datetime(2024, 8, 13, 16, 57, 00)

    mock_client.create_bucket(
    Bucket=test_bucket,
    CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    # test_key = "test_table/2024/8/13/16-57/test_table-2024-08-13_16.57.00.csv"
    TABLE_NAMES = [
        "payment"
    ]

    for table in TABLE_NAMES:
        df = pd.DataFrame({
            'id':[1,2,3],
            'value':['A', 'B', 'C']
        })
    data_dict = load_ingested_tables()

    assert data_dict == {}
    assert "No new data to transform" in caplog.text


def test_error():
    with pytest.raises(Exception):
        load_ingested_tables()
