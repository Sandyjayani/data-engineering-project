import pytest
from unittest.mock import patch, MagicMock
from src.load.load_lambda_handler import lambda_handler
from pg8000.native import Connection, Error
import pandas as pd


@pytest.mark.it("Should successfully process the event and load data")
@patch("src.load.load_lambda_handler.read_parquet_from_s3")
@patch("src.load.load_lambda_handler.create_connection")
@patch("src.load.load_lambda_handler.insert_data_into_tables")
def test_lambda_handler_success(
    mock_insert_data, mock_create_connection, mock_read_parquet
):
    # Arrange
    event = {"table_name": "test_table"}
    context = {}

    mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    mock_read_parquet.return_value = mock_df

    mock_conn = MagicMock(spec=Connection)
    mock_create_connection.return_value = mock_conn

    # Act
    result = lambda_handler(event, context)

    # Assert
    mock_read_parquet.assert_called_once_with("test_table")
    mock_create_connection.assert_called_once_with("load")
    mock_insert_data.assert_called_once_with(mock_df, "test_table", mock_conn)
    assert result == {"statusCode": 200, "body": "Data loaded successfully"}


@pytest.mark.it("Should handle exception when reading parquet fails")
@patch("src.load.load_lambda_handler.read_parquet_from_s3")
def test_lambda_handler_read_parquet_error(mock_read_parquet):
    # Arrange
    event = {"table_name": "test_table"}
    context = {}

    mock_read_parquet.side_effect = Exception("Failed to read parquet")

    # Act
    result = lambda_handler(event, context)

    # Assert
    assert result == {"statusCode": 500, "body": "Failed to read parquet"}


@pytest.mark.it("Should handle exception when database connection fails")
@patch("src.load.load_lambda_handler.read_parquet_from_s3")
@patch("src.load.load_lambda_handler.create_connection")
def test_lambda_handler_db_connection_error(mock_create_connection, mock_read_parquet):
    # Arrange
    event = {"table_name": "test_table"}
    context = {}

    mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    mock_read_parquet.return_value = mock_df

    mock_create_connection.side_effect = Error("Database connection failed")

    # Act
    result = lambda_handler(event, context)

    # Assert
    assert result == {"statusCode": 500, "body": "Database connection failed"}


@pytest.mark.it("Should handle exception when inserting data fails")
@patch("src.load.load_lambda_handler.read_parquet_from_s3")
@patch("src.load.load_lambda_handler.create_connection")
@patch("src.load.load_lambda_handler.insert_data_into_tables")
def test_lambda_handler_insert_data_error(
    mock_insert_data, mock_create_connection, mock_read_parquet
):
    # Arrange
    event = {"table_name": "test_table"}
    context = {}

    mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    mock_read_parquet.return_value = mock_df

    mock_conn = MagicMock(spec=Connection)
    mock_create_connection.return_value = mock_conn

    mock_insert_data.side_effect = Error("Failed to insert data")

    # Act
    result = lambda_handler(event, context)

    # Assert
    assert result == {"statusCode": 500, "body": "Failed to insert data"}
