import pytest
from src.transform.transformation_lambda import lambda_handler
from unittest.mock import patch
import pandas as pd
import json

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


