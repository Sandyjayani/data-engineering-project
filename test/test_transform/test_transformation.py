import pytest
from src.transform.transformation import lambda_handler
from unittest.mock import patch
import pandas as pd
import json

@patch("src.transform.transformation.load_ingestion_data")
def test_return_statement(mock_load):
    mock_load.return_value = {}
    response = lambda_handler({},{})

    assert response == {
            "statusCode": 200,
            "body": json.dumps("Transformation Lambda executed successfully"),
        }


@patch("src.transform.transformation.load_ingestion_data", side_effect = RuntimeError())
def test_exception(mock_load, caplog):
    with pytest.raises(RuntimeError):
        lambda_handler({},{})

    assert "Critical error" in caplog.text
