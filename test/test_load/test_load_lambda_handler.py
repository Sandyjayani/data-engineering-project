import pytest
from unittest.mock import patch, MagicMock
from src.load.load_lambda_handler import lambda_handler
from pg8000.native import Connection, Error, DatabaseError
import pandas as pd


@pytest.mark.it("Should return correct dictionary")
@patch("src.load.load_lambda_handler.read_parquet_from_s3")
@patch("src.load.load_lambda_handler.insert_dim")
def test_lambda_handler_success(mock_insert_dim, mock_read_parquet):
    # Arrange
    event = {}
    context = {}
    mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    mock_read_parquet.return_value = {"test_dim_table": mock_df}
    # Act
    result = lambda_handler(event, context)
    # Assert
    mock_insert_dim.assert_called_once_with(mock_df, "test_dim_table")
    assert result == {"statusCode": 200, "body": "Load process completed successfully"}


@pytest.mark.it("Should handle exception when reading parquet fails")
@patch("src.load.load_lambda_handler.read_parquet_from_s3")
def test_lambda_handler_read_parquet_error(mock_read_parquet):
    mock_read_parquet.side_effect = Exception("Failed to read parquet")
    with pytest.raises(Exception):
        lambda_handler("E", "C")


@pytest.mark.it("Should handle exception when inserting dim data fails")
@patch("src.load.load_lambda_handler.read_parquet_from_s3")
@patch("src.load.load_lambda_handler.insert_dim")
def test_lambda_handler_insert_dim_data_error(
    mock_insert_dim, mock_read_from_parquet, caplog
):
    mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    mock_read_from_parquet.return_value = {"test_dim_table": mock_df}
    mock_insert_dim.side_effect = DatabaseError(
        {
            "S": "FATAL",
            "V": "FATAL",
            "C": "53300",
            "M": "Too many silly students connecting to the db at once!",
            "F": "postinit.c",
            "L": "846",
            "R": "InitPostgres",
        }
    )

    # Act
    with pytest.raises(DatabaseError):
        lambda_handler("E", "C")
    assert "Too many silly students connecting to the db at once!" in caplog.text


@pytest.mark.it("Should handle exception when inserting fact data fails")
@patch("src.load.load_lambda_handler.read_parquet_from_s3")
@patch("src.load.load_lambda_handler.insert_fact")
def test_lambda_handler_insert_fact_data_error(
    mock_insert_fact, mock_read_from_parquet, caplog
):
    mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    mock_read_from_parquet.return_value = {"test_fact_table": mock_df}
    mock_insert_fact.side_effect = DatabaseError(
        {
            "S": "FATAL",
            "V": "FATAL",
            "C": "53300",
            "M": "Too many silly students connecting to the db at once!",
            "F": "postinit.c",
            "L": "846",
            "R": "InitPostgres",
        }
    )

    # Act
    with pytest.raises(DatabaseError):
        lambda_handler("E", "C")
    assert "Too many silly students connecting to the db at once!" in caplog.text
