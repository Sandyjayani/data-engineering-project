import pandas as pd
from datetime import datetime, date


def dim_date(dataframe):
    """
    takes a dataframe formatted according to the sales_order table of the ToteSys OLTP database

    returns a dataframe with the dates contained in the input's timestamp,
    delivery_date and payment_date columns, formatted & completed to fit the OLAP
    database's dim_date dimension table

    there will be one row output for each input date
    """
    if list(dataframe.columns) != [
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
    ]:
        raise ValueError

    output_columns = [
        "date_id",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "quarter",
    ]

    all_dates = []
    for index in range(len(dataframe)):
        row = dataframe.loc[index]
        timestamp = row.iloc[1]
        delivery_date = row.iloc[9]
        payment_date = row.iloc[10]

        new_timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
        new_delivery = datetime.strptime(delivery_date, "%Y-%m-%d")
        new_payment = datetime.strptime(payment_date, "%Y-%m-%d")

        dates = [new_timestamp, new_delivery, new_payment]

        all_dates.append(dates)

    all_rows = []
    for dates in all_dates:
        for new_date in dates:
            date_id = new_date.date()
            year_int = int(new_date.strftime("%Y"))
            month_int = int(new_date.strftime("%m"))
            day_int = int(new_date.strftime("%d"))
            day_of_the_week_int = int(new_date.strftime("%w"))
            if day_of_the_week_int == 0:  # fixing sunday
                day_of_the_week_int == 7
            day_name = new_date.strftime("%A")
            month_name = new_date.strftime("%B")
            if month_int in [1, 2, 3]:
                quarter = 1
            elif month_int in [4, 5, 6]:
                quarter = 2
            elif month_int in [7, 8, 9]:
                quarter = 3
            elif month_int in [10, 11, 12]:
                quarter = 4

            date_row = [
                date_id,
                year_int,
                month_int,
                day_int,
                day_of_the_week_int,
                day_name,
                month_name,
                quarter,
            ]
            date_series = pd.Series(data=date_row, index=output_columns)
            all_rows.append(date_series)

    all_dates_df = pd.DataFrame(data=all_rows, columns=output_columns)

    output_df = all_dates_df.drop_duplicates()

    return output_df
