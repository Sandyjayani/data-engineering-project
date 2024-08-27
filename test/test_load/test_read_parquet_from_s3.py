import pytest
from unittest.mock import patch, Mock
from datetime import datetime
import pandas as pd
import boto3
from moto import mock_aws
from io import BytesIO
from src.load.read_parquet_from_s3 import read_parquet_from_s3

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'

# ... (keep the existing imports and fixtures)

@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_aws():
        s3 = boto3.client('s3', region_name='eu-west-2')  # Change to 'eu-west-2'
        yield s3

@pytest.fixture
def mock_logger():
    return Mock()

@pytest.fixture
def mock_timestamps():
    with patch('src.load.read_parquet_from_s3.get_transformation_timestamp') as mock:
        mock.return_value = datetime(2023, 1, 2, 12, 0, 0)
        yield mock

@pytest.fixture
def mock_pandas_read_parquet(mocker):
    return mocker.patch('pandas.read_parquet')

def create_test_parquet(s3, bucket_name, table_name):
    df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}  # Add this line
    )
    s3.put_object(
        Bucket=bucket_name,
        Key=f"{table_name}/2023/1/2/12-0/{table_name}-2023-01-02_12.00.00.parquet",
        Body=parquet_buffer.getvalue()
    )

@pytest.mark.it("should read parquet files from S3 when new data is available")
def test_read_parquet_from_s3_new_data(s3, mock_logger, mock_timestamps, mock_pandas_read_parquet):
    bucket_name = "smith-morra-transformation-bucket"
    table_list = ["dim_date", "dim_location", "dim_design", "fact_sales_order", "dim_currency", "dim_counterparty", "dim_staff"]
    
    for table_name in table_list:
        create_test_parquet(s3, bucket_name, table_name)

    with patch('boto3.client', return_value=s3):  # Add this line
        result = read_parquet_from_s3()

    assert len(result) == 7  # Expecting data for all 7 tables
    for table_name in table_list:
        assert table_name in result
        assert isinstance(result[table_name], pd.DataFrame)
    
    assert mock_pandas_read_parquet.call_count == 7

@pytest.mark.it("should raise an exception when an error occurs")
def test_read_parquet_from_s3_error(s3, mock_logger, mock_timestamps):
    bucket_name = "smith-morra-transformation-bucket"
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}  # Add this line
    )
    
    with patch('boto3.client', return_value=s3):  # Add this line
        with patch.object(s3, 'get_object', side_effect=Exception("S3 error")):
            with pytest.raises(Exception) as exc_info:
                read_parquet_from_s3()

    assert str(exc_info.value) == "S3 error"




