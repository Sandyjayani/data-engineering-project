import pytest
from src.transform.transformation_lambda import lambda_handler
from unittest.mock import patch
import pandas as pd
import json


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
def test_return_statement(mock_load_ingested_tables, upload_to_transformation_s3):
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
def test_not_call_upload_function_if_no_new_data(mock_load_ingested_tables, upload_to_transformation_s3):
    mock_load_ingested_tables.return_value = {}
    response = lambda_handler({},{})
    upload_to_transformation_s3.assert_not_called()


@patch("src.transform.transformation_lambda.upload_to_transformation_s3")
@patch("src.transform.transformation_lambda.load_ingested_tables")
def test_calls_upload_function_if_new_data(mock_load_ingested_tables, upload_to_transformation_s3, test_new_data_dict):
    mock_load_ingested_tables.return_value = test_new_data_dict
    response = lambda_handler({},{})
    upload_call_args = upload_to_transformation_s3.call_args_list
    table_call_args = [call.args[1] for call in upload_call_args]
    expected_calls = ['currency', 'design', 'location', 'sales_order']
    assert all(call in table_call_args for call in expected_calls)



