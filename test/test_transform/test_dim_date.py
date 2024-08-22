from src.transform.dim_date import transform_date
import pytest
from datetime import date
import pandas as pd
from copy import deepcopy


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
    result = transform_date(test_dataframe)
    assert type(result) == type(test_dataframe)


def test_does_not_mutate_input_df(test_dataframe):
    original_df = deepcopy(test_dataframe)
    transform_date(test_dataframe)
    assert test_dataframe.to_dict() == original_df.to_dict()


def test_returns_new_df(test_dataframe):
    result = transform_date(test_dataframe)
    assert result is not test_dataframe


def test_returns_df_with_8_columns(test_dataframe):
    expected_columns = [
        "date_id",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "quarter",
    ]
    result = transform_date(test_dataframe)
    assert len(result.columns) == len(expected_columns)


def test_returns_df_with_expected_column_names(test_dataframe):
    expected_columns = [
        "date_id",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "quarter",
    ]
    result = transform_date(test_dataframe)
    assert list(result.columns) == expected_columns


def test_returns_df_with_expected_column_value_types(test_dataframe):
    result = transform_date(test_dataframe)
    row = result.loc[0]
    assert type(row.iloc[0]) == date
    for int_index in [1, 2, 3, 4, 7]:
        assert type(int(row.iloc[int_index])) == int
    for string_index in [5, 6]:
        print(row.iloc[string_index])
        assert type(row.iloc[string_index]) == str


def test_returns_dataframe_with_expected_kind_of_values(test_dataframe):
    week_days = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    months = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    result = transform_date(test_dataframe)
    for index in range(4):
        row = result.loc[index]

        string_year = str(row.iloc[1])
        if row.iloc[2] < 10:
            string_month = "0" + str(row.iloc[2])
        else:
            string_month = str(row.iloc[2])
        if row.iloc[3] < 10:
            string_day = "0" + str(row.iloc[3])
        else:
            string_day = str(row.iloc[3])
        assert (
            row.iloc[0].strftime("%Y-%m-%d")
            == string_year + "-" + string_month + "-" + string_day
        )

        assert row.iloc[1] >= 1900 and row.iloc[1] < 2100
        assert row.iloc[2] >= 1 and row.iloc[2] <= 12
        assert row.iloc[3] >= 1 and row.iloc[3] <= 31
        assert row.iloc[4] >= 1 and row.iloc[4] <= 7
        assert row.iloc[5] in week_days
        assert row.iloc[6] in months
        assert row.iloc[7] >= 1 and row.iloc[7] <= 4


def test_returns_all_dates_passed_as_rows(test_dataframe):
    result = transform_date(test_dataframe)
    assert len(result.index) == 14


def test_returns_unique_date_rows(test_dataframe):
    result = transform_date(test_dataframe)
    assert len(result["date_id"]) == len(set(result["date_id"]))  # same length

    string_list = [
        date_object.strftime("%Y-%m-%d") for date_object in result["date_id"]
    ]

    assert sorted(list(string_list)) == [
        "2015-10-03",
        "2015-10-07",
        "2015-10-08",
        "2018-03-03",
        "2018-03-06",
        "2018-03-07",
        "2021-06-03",
        "2021-06-05",
        "2021-06-07",
        "2022-11-03",
        "2022-11-05",
        "2022-11-08",
        "2023-08-04",
        "2023-08-07",
    ]  # expected values


def test_raises_error_for_invalid_input():
    rando_df = pd.DataFrame([1, 2, 3])
    with pytest.raises(ValueError):
        assert transform_date(rando_df)
