import pytest
import pandas as pd
from unittest.mock import patch, Mock
from pg8000.native import DatabaseError
from src.load.insert_fact import insert_fact


@pytest.fixture()
def fact_sales_order():
    return pd.read_parquet(
        "test/test_load/test_data/fact_sales_order-2024-08-23_11.41.35.parquet"
    )


FACT_SALES_ORDER_COLUMNS = [
    "sales_order_id",
    "created_date",
    "created_time",
    "last_updated_date",
    "last_updated_time",
    "sales_staff_id",
    "counterparty_id",
    "units_sold",
    "unit_price",
    "currency_id",
    "design_id",
    "agreed_payment_date",
    "agreed_delivery_date",
    "agreed_delivery_location_id",
]


@patch("src.load.insert_fact.create_connection")
def test_successful_insertion_fact_sales_order(
    mock_connection, fact_sales_order, caplog
):
    insert_fact(fact_sales_order, "fact_sales_order")
    assert (
        list(fact_sales_order.columns) == FACT_SALES_ORDER_COLUMNS
    ), "Column names do not match for fact_sales_order"
    assert "9869 rows inserted into table fact_sales_order successfully" in caplog.text


@patch("src.load.insert_fact.create_connection")
def test_catches_and_returns_exception(mock_connection, fact_sales_order, caplog):
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
        insert_fact(fact_sales_order, "fact_sales_order")
    assert "Too many silly students connecting to the db at once" in caplog.text
