from src.transform.facts_sales_order import transform_sales_order
import pytest
import pandas as pd
from copy import deepcopy
from datetime import date, time


@pytest.fixture
def test_dataframe():
    test_columns = [
        "sales_order_id",
        "created_at",
        "last_updated",
        "design_id",
        "staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "agreed_delivery_date",
        "agreed_payment_date",
        "agreed_delivery_location_id",
    ]
    test_row_1 = [
        2,
        "2015-10-03 14:20:52.186",
        "2015-10-03 14:20:52.186",
        3,
        19,
        8,
        42972,
        3.94,
        2,
        "2015-10-07",
        "2015-10-08",
        8,
    ]
    test_row_2 = [
        3,
        "2018-03-03 14:20:52.188",
        "2018-03-03 14:20:52.188",
        4,
        10,
        4,
        65839,
        2.91,
        3,
        "2018-03-06",
        "2018-03-07",
        19,
    ]
    test_row_3 = [
        4,
        "2021-06-03 14:20:52.188",
        "2021-06-03 14:20:52.188",
        4,
        10,
        16,
        32069,
        3.89,
        2,
        "2021-06-05",
        "2021-06-07",
        15,
    ]
    test_row_4 = [
        5,
        "2022-11-03 14:20:52.186",
        "2022-11-03 14:20:52.186",
        7,
        18,
        4,
        49659,
        2.41,
        3,
        "2022-11-05",
        "2022-11-08",
        25,
    ]
    test_row_5 = [
        6,
        "2023-08-04 11:37:10.341",
        "2023-08-04 11:37:10.341",
        3,
        13,
        18,
        83908,
        3.99,
        3,
        "2023-08-04",
        "2023-08-07",
        17,
    ]
    test_frame = pd.DataFrame(
        data=[
            pd.Series(data=test_row_1, index=test_columns),
            pd.Series(data=test_row_2, index=test_columns),
            pd.Series(data=test_row_3, index=test_columns),
            pd.Series(data=test_row_4, index=test_columns),
            pd.Series(data=test_row_5, index=test_columns),
        ],
        columns=test_columns,
    )
    return test_frame


def test_returns_dataframe(test_dataframe):
<<<<<<< HEAD:test/test_transform/test_facts_table.py
    rando_df = pd.DataFrame([1, 2, 3])
    result = facts_table(test_dataframe)
=======
    rando_df = pd.DataFrame([1,2,3])
    result = transform_sales_order(test_dataframe)
>>>>>>> main:test/test_transform/test_facts_sales_order.py
    assert type(result) == type(rando_df)


def test_does_not_mutate_input(test_dataframe):
    expected = deepcopy(test_dataframe)
    transform_sales_order(test_dataframe)
    assert test_dataframe.to_dict() == expected.to_dict()


def test_returns_new_dataframe(test_dataframe):
    result = transform_sales_order(test_dataframe)
    assert result is not test_dataframe
    assert test_dataframe.to_dict() != result.to_dict()


def test_df_has_expected_amount_of_columns(test_dataframe):
    result = transform_sales_order(test_dataframe)
    assert len(result.columns) == 14


def test_df_has_expected_column_names(test_dataframe):
    expected = [
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
    result = transform_sales_order(test_dataframe)
    assert list(result.columns) == expected


def test_df_values_are_expected_types(test_dataframe):
    result = transform_sales_order(test_dataframe)
    for index in range(5):
        row = result.loc[index]
        for int_i in [0, 5, 6, 7, 9, 10, 13]:
            assert type(int(row.iloc[int_i])) == int
        for date_i in [1, 3, 11, 12]:
            assert type(row.iloc[date_i]) == date
        for time_i in [2, 4]:
            assert type(row.iloc[time_i]) == time
        assert type(float(row.iloc[8])) == float  # I think?? "numeric(10, 2)"


def test_df_values_are_within_expected_range(test_dataframe):
    result = transform_sales_order(test_dataframe)
    for index in range(5):
        row = result.loc[index]
        for id_index in [0, 5, 6, 9, 10, 13]:
            assert row.iloc[id_index] >= 0 and row.iloc[id_index] < 30
        assert row.iloc[7] >= 0 and row.iloc[7] < 100000
        assert row.iloc[8] >= 0 and row.iloc[8] < 1000
        for date_index in [1, 3, 11, 12]:
            assert row.iloc[date_index].year > 1900 and row.iloc[date_index].year < 2100


def test_returns_same_amount_of_rows_as_input(test_dataframe):
    result = transform_sales_order(test_dataframe)
    assert len(result.index) == len(test_dataframe.index)


def test_raises_error_for_invalid_input(test_dataframe):
    rando_df = pd.DataFrame([1, 2, 3])
    with pytest.raises(ValueError):
<<<<<<< HEAD:test/test_transform/test_facts_table.py
        assert facts_table(rando_df)
=======
        assert transform_sales_order(rando_df)
>>>>>>> main:test/test_transform/test_facts_sales_order.py
