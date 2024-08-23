import pytest
from src.transform.transformation_lambda import lambda_handler
from unittest.mock import patch
import pandas as pd
import json
import os
from moto import mock_aws
import boto3

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
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket="smith-morra-transformation-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        s3.create_bucket(
            Bucket="smith-morra-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        yield 


@pytest.fixture
def test_new_data_dict():
    return {
        'address': pd.read_csv(
            "test/test_transform/test_data/test_address.csv"
        ),
        'counterparty': pd.read_csv(
            "test/test_transform/test_data/test_counterparty.csv"
        ),
        'currency': pd.read_csv(
            "test/test_transform/test_data/test_currency.csv"
        ),
        'department': pd.read_csv(
            "test/test_transform/test_data/test_department.csv"
        ),
        'design': pd.read_csv(
            "test/test_transform/test_data/test_design.csv"
        ),
        'payment_type': pd.read_csv(
            "test/test_transform/test_data/test_payment_type.csv"
        ),
        'payment': pd.read_csv(
            "test/test_transform/test_data/test_payment.csv"
        ),
        'purchase_order': pd.read_csv(
            "test/test_transform/test_data/test_purchase_order.csv"
        ),
        'sales_order': pd.read_csv(
            "test/test_transform/test_data/test_sales_order.csv"
        ),     
        'staff': pd.read_csv(
            "test/test_transform/test_data/test_staff.csv"
        ),
        'transaction': pd.read_csv(
            "test/test_transform/test_data/test_transaction.csv"
        )        
        }


@patch("src.transform.transformation_lambda.upload_to_transformation_s3")
@patch("src.transform.transformation_lambda.load_ingested_tables")
def test_return_statement(mock_load_ingested_tables, upload_to_transformation_s3, mock_client):
    mock_load_ingested_tables.return_value = {}
    response = lambda_handler({},{})
    assert response == {
            "statusCode": 200,
            "body": json.dumps("Transformation Lambda executed successfully"),
        }


@patch("src.transform.transformation_lambda.upload_to_transformation_s3")
@patch("src.transform.transformation_lambda.load_ingested_tables", side_effect = RuntimeError())
def test_logs_exception(mock_load_ingested_tables, upload_to_transformation_s3, caplog):
    with pytest.raises(RuntimeError):
        lambda_handler({},{})
    assert "Critical error" in caplog.text


@patch("src.transform.transformation_lambda.upload_to_transformation_s3")
@patch("src.transform.transformation_lambda.load_ingested_tables")
def test_not_call_upload_function_if_no_new_data(mock_load_ingested_tables, upload_to_transformation_s3, mock_client):
    mock_load_ingested_tables.return_value = {}
    with patch("src.transform.transformation_lambda.transform_date", return_value=None) as mock_transform_date:
        response = lambda_handler({},{})
        print("Upload function called:", upload_to_transformation_s3.called)
        upload_to_transformation_s3.assert_not_called()


@patch("src.transform.transformation_lambda.upload_to_transformation_s3")
@patch("src.transform.transformation_lambda.load_ingested_tables")
def test_calls_upload_function_if_new_data(mock_load_ingested_tables, upload_to_transformation_s3, test_new_data_dict, mock_client):
    mock_load_ingested_tables.return_value = test_new_data_dict
    with patch("src.transform.transformation_lambda.transform_counterparty", return_value=None) as mock_transform_counterparty:
        response = lambda_handler({},{})
        upload_call_args = upload_to_transformation_s3.call_args_list
        table_call_args = [call.args[1] for call in upload_call_args]
        print(table_call_args)
        expected_calls = ['dim_currency', 'dim_design', 'dim_location', 'fact_sales_order', 'dim_staff']
        assert all(call in table_call_args for call in expected_calls)



@patch("src.transform.transformation_lambda.upload_to_transformation_s3")
@patch("src.transform.transformation_lambda.load_ingested_tables")
def test_logs_start_and_completion_of_execution(mock_load_ingested_tables, 
                                                upload_to_transformation_s3, 
                                                test_new_data_dict,
                                                caplog,
                                                mock_client):
    mock_load_ingested_tables.return_value = test_new_data_dict
    with patch("src.transform.transformation_lambda.transform_counterparty", return_value=None) as mock_transform_counterparty:
        response = lambda_handler({},{})

    assert 'Starting transformation process' in caplog.text
    assert 'Transformation process complete' in caplog.text
