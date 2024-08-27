import pytest
import pandas as pd
from unittest.mock import patch, Mock
from pg8000.native import DatabaseError
from src.load.insert_dim import insert_dim

@pytest.fixture()
def dim_date():
    return pd.read_parquet("test/test_load/test_data/dim_date-2024-08-23_11.05.48.parquet")

@pytest.fixture()
def dim_currency():
    return pd.read_parquet("test/test_load/test_data/currency-2024-08-23_10.36.53.parquet")

@pytest.fixture()
def dim_counterparty():
    return pd.read_parquet("test/test_load/test_data/dim_counterparty-2024-08-23_14.21.28.parquet")


@pytest.fixture()
def dim_design():
    return pd.read_parquet("test/test_load/test_data/dim_design-2024-08-23_11.41.32.parquet")  # noqa: E501

@pytest.fixture()
def dim_location():
    return pd.read_parquet("test/test_load/test_data/dim_location-2024-08-23_11.41.33.parquet")  # noqa: E501

@pytest.fixture()
def dim_staff():
    return pd.read_parquet("test/test_load/test_data/dim_staff-2024-08-23_11.41.36.parquet")



DIM_COUNTERPARTY_COLUMNS = [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number"
        ]

DIM_DATE_COLUMNS = ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']

DIM_CURRENCY_COLUMNS = ['currency_id', 'currency_code', 'currency_name']

DIM_DESIGN_COLUMNS = ['design_id', 'design_name', 'file_location', 'file_name']

DIM_LOCATION_COLUMNS = ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']

DIM_STAFF_COLUMNS = ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']


@patch("src.load.insert_dim.create_connection")
def test_successful_insert_date_data(mock_connection, dim_date, caplog):
    insert_dim(dim_date, 'dim_date')

    assert list(dim_date.columns) == DIM_DATE_COLUMNS, "Column names do not match for dim_date"
    assert "1096 rows inserted into table dim_date successfully" in caplog.text
    assert mock_connection.return_value.run.call_count == 1096


@patch("src.load.insert_dim.create_connection")
def test_successful_insert_currency_data(mock_connection, dim_currency, caplog):
    insert_dim(dim_currency, "currency")
    
    assert "3 rows inserted into table currency successfully" in caplog.text
    assert mock_connection.return_value.run.call_count == 3
    assert list(dim_currency.columns) == DIM_CURRENCY_COLUMNS


@patch("src.load.insert_dim.create_connection")
def test_successful_insert_dim_counterparty(mock_connection, dim_counterparty, caplog):
    insert_dim(dim_counterparty, "dim_counterparty")
    
    assert list(dim_counterparty.columns) == DIM_COUNTERPARTY_COLUMNS, "Column names do not match for dim_counterparty"


@patch("src.load.insert_dim.create_connection")
def test_successful_insertion_dim_design(mock_connection, dim_design, caplog):
    insert_dim(dim_design, "dim_design")

    assert list(dim_design.columns) == DIM_DESIGN_COLUMNS, "Column names do not match for dim_design"
    assert "425 rows inserted into table dim_design successfully" in caplog.text


@patch("src.load.insert_dim.create_connection")
def test_successful_insertion_dim_location(mock_connection, dim_location, caplog):
    insert_dim(dim_location, "dim_location")

    assert list(dim_location.columns) == DIM_LOCATION_COLUMNS, "Column names do not match for dim_location"
    assert "30 rows inserted into table dim_location successfully" in caplog.text


@patch("src.load.insert_dim.create_connection")
def test_successful_insertion_dim_staff(mock_connection, dim_staff, caplog):
    insert_dim(dim_staff, "dim_staff")

    assert list(dim_staff.columns) == DIM_STAFF_COLUMNS, "Column names do not match for dim_staff"
    assert "20 rows inserted into table dim_staff successfully" in caplog.text


@patch("src.load.insert_dim.create_connection")
def test_catches_and_returns_exception(mock_connection, dim_staff, caplog):
    mock_conn = Mock()
    mock_conn.run.side_effect = DatabaseError(
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
    mock_connection.return_value = mock_conn
    with pytest.raises(DatabaseError):
        insert_dim(dim_staff, "dim_staff")
    assert "Too many silly students connecting to the db at once" in caplog.text




