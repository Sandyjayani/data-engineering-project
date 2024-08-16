import pytest
import pandas as pd
import datetime
from pg8000.native import DatabaseError
from unittest.mock import Mock
from src.util_functions.get_table import get_table as gt
from src.util_functions.get_db_connection import create_connection


@pytest.fixture(scope="function")
def connection():
    connection = Mock()
    currency_columns = [
        {"name": "currency_id"},
        {"name": "currency_code"},
        {"name": "created_at"},
        {"name": "last_updated"},
    ]
    connection.columns = currency_columns
    return connection


@pytest.mark.it("Returns data frame")
def test_returns_data_frame(connection):
    connection.run.return_value = [
        [
            1,
            "GBP",
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        ],
        [
            2,
            "USD",
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        ],
        [
            3,
            "EUR",
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        ],
    ]
    timestamp = datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)
    output = gt("currency", connection, timestamp)
    assert isinstance(output, pd.DataFrame)


@pytest.mark.it("Returns none if query finds no rows with last_updated after timestamp")
def test_returns_none(connection):
    timestamp = datetime.datetime(2024, 11, 3, 14, 20, 49, 962000)
    data = [
        [
            1,
            "GBP",
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        ],
        [
            2,
            "USD",
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        ],
        [
            3,
            "EUR",
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        ],
    ]

    def return_result(query, table_name):
        return [row for row in data if row[3] > timestamp]

    connection.run.side_effect = return_result
    output = gt("currency", connection, timestamp)
    assert output == None


@pytest.mark.it(
    "Returns dataframe that only includes rows where last_updated after timestamp"
)
def test_returns_only_new_rows(connection):
    timestamp = datetime.datetime(2023, 11, 3, 14, 20, 49, 962000)
    data = [
        [
            1,
            "GBP",
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        ],
        [
            2,
            "USD",
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
        ],
        [
            3,
            "EUR",
            datetime.datetime(2024, 11, 3, 14, 20, 49, 962000),
            datetime.datetime(2024, 11, 3, 14, 20, 49, 962000),
        ],
    ]

    def return_result(query, table_name):
        return [row for row in data if row[3] > timestamp]

    connection.run.side_effect = return_result
    output = gt("currency", connection, timestamp)
    last_updated = list(output["last_updated"])
    assert all([time > timestamp for time in last_updated])


@pytest.mark.it("Logs exception when error raised")
def test_logs_error(connection, caplog):
    timestamp = datetime.datetime(2023, 11, 3, 14, 20, 49, 962000)
    connection.run.side_effect = DatabaseError(
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
    with pytest.raises(DatabaseError):
        gt("currency", connection, timestamp)
    assert "query failed" in caplog.text


@pytest.mark.it('Run is called with expected params')
def test_run_invoked_with_correct_params(connection):
    timestamp = datetime.datetime(2025, 11, 3, 14, 20, 49, 962000)
    connection.run.return_value = None
    gt("currency", connection, timestamp)
    connection.run.assert_called_once_with("SELECT * FROM currency\n        WHERE last_updated > '2025-11-03 14:20:49.962000'::timestamp", 
                                           table_name='currency'
    )