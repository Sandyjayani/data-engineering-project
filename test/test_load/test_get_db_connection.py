import pytest
import json
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from pg8000.native import Connection, Error
from src.load.get_db_connection import create_connection

# Mock secrets
MOCK_EXTRACTION_SECRET = json.dumps(
    {
        "user": "extract_user",
        "password": "extract_pass",
        "host": "extract_host",
        "database": "extract_db",
        "port": 5432,
    }
)

MOCK_LOAD_SECRET = json.dumps(
    {
        "user": "load_user",
        "password": "load_pass",
        "host": "load_host",
        "database": "load_db",
        "port": 5433,
    }
)


@pytest.mark.it("should create a connection for extraction stage")
@patch("src.load.get_db_connection.get_secret")
@patch("src.load.get_db_connection.Connection")
def test_create_connection_extraction(mock_connection, mock_get_secret):
    mock_get_secret.return_value = MOCK_EXTRACTION_SECRET
    mock_conn = MagicMock()
    mock_connection.return_value = mock_conn

    conn = create_connection("extraction")

    mock_get_secret.assert_called_once_with("DataSource_PostgresDB_Credentials")
    mock_connection.assert_called_once_with(
        user="extract_user",
        password="extract_pass",
        host="extract_host",
        database="extract_db",
        port=5432,
    )
    assert conn == mock_conn


@pytest.mark.it("should create a connection for load stage")
@patch("src.load.get_db_connection.get_secret")
@patch("src.load.get_db_connection.Connection")
def test_create_connection_load(mock_connection, mock_get_secret):
    mock_get_secret.return_value = MOCK_LOAD_SECRET
    mock_conn = MagicMock()
    mock_connection.return_value = mock_conn

    conn = create_connection("load")

    mock_get_secret.assert_called_once_with("DataTarget_PostgresDB_Credentials")
    mock_connection.assert_called_once_with(
        user="load_user",
        password="load_pass",
        host="load_host",
        database="load_db",
        port=5433,
    )
    assert conn == mock_conn


@pytest.mark.it("should handle ClientError from get_secret")
@patch("src.load.get_db_connection.get_secret")
def test_create_connection_secret_error(mock_get_secret):
    mock_get_secret.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "Secret not found"}},
        "GetSecretValue",
    )

    with pytest.raises(ClientError):
        create_connection("extraction")


@pytest.mark.it("should handle Connection error")
@patch("src.load.get_db_connection.get_secret")
@patch("src.load.get_db_connection.Connection")
def test_create_connection_db_error(mock_connection, mock_get_secret):
    mock_get_secret.return_value = MOCK_EXTRACTION_SECRET
    mock_connection.side_effect = Error("Connection failed")

    with pytest.raises(Error):
        create_connection("extraction")


@pytest.mark.it("should handle JSON decoding error")
@patch("src.load.get_db_connection.get_secret")
def test_create_connection_json_error(mock_get_secret):
    mock_get_secret.return_value = "Invalid JSON"

    with pytest.raises(json.JSONDecodeError):
        create_connection("extraction")
