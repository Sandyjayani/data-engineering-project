import pandas as pd
from datetime import datetime, date
from re import compile, match
import os

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger # type: ignore
else:
    from src.extraction.setup_logger import setup_logger


def dim_date(dataframe):
    """
    takes a dataframe formatted according to the sales_order table of the ToteSys OLTP database

    returns a dataframe with the dates contained in the input's timestamp, 
    delivery_date and payment_date columns, formatted & completed to fit the OLAP 
    database's dim_date dimension table

    there will be one row output for each input date
    """

    logger = setup_logger("transform_dim_date")

    logger.info("Checking input value.")
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
    logger.info("Input value is valid: from sales_order table.")


    logger.info("Gathering timestamps.")
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
        secondary_timestamp = row.iloc[2]
        delivery_date = row.iloc[9]
        payment_date = row.iloc[10]

        # if there are none/null values
        if not timestamp or not secondary_timestamp or not delivery_date or not payment_date: 
            logger.error("Missing timestamp value(s).")
            return None
        
        # if there is a value but it's not a string
        for stamp in [timestamp, secondary_timestamp, delivery_date, payment_date]:
            if type(stamp) != str:
                logger.error("One or more timestamp value(s) is not a string.")
                return None
        
        date_time_pattern = compile(r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+")
        date_pattern = compile(r"\d{4}-\d{2}-\d{2}")

        # if it is a string but not in the right format
        if not date_time_pattern.match(timestamp) \
        or not date_time_pattern.match(secondary_timestamp) \
        or not date_pattern.match(delivery_date) \
        or not date_pattern.match(payment_date): 
            logger.error("One or more timestamp value(s) is not in the correct format.")
            return None

        new_timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
        new_second_timestamp = datetime.strptime(secondary_timestamp, "%Y-%m-%d %H:%M:%S.%f")
        new_delivery = datetime.strptime(delivery_date, "%Y-%m-%d")
        new_payment = datetime.strptime(payment_date, "%Y-%m-%d")

        dates = [
            new_timestamp,
            new_second_timestamp,
            new_delivery,
            new_payment
        ]

        all_dates.append(dates)

    logger.info("Converting timestamps.")
    all_rows = []
    for dates in all_dates:
        for new_date in dates:
            date_id = new_date.date()
            year_int = int(new_date.strftime("%Y"))
            month_int = int(new_date.strftime("%m"))
            day_int = int(new_date.strftime("%d"))
            day_of_the_week_int = int(new_date.strftime("%w"))
            if day_of_the_week_int == 0: # fixing sunday
                day_of_the_week_int == 7
            day_name = new_date.strftime("%A")
            month_name = new_date.strftime("%B")
            if month_int in [1,2,3]:
                quarter = 1
            elif month_int in [4,5,6]:
                quarter = 2
            elif month_int in [7,8,9]:
                quarter = 3
            elif month_int in [10,11,12]:
                quarter = 4
            
            date_row = [
                date_id,
                year_int,
                month_int,
                day_int,
                day_of_the_week_int,
                day_name,
                month_name,
                quarter
            ]
            date_series = pd.Series(data=date_row, index=output_columns)
            all_rows.append(date_series)

    logger.info("Creating new dataframe.")
    all_dates_df = pd.DataFrame(
        data=all_rows, 
        columns=output_columns
    )

    output_df = all_dates_df.drop_duplicates()

    return output_df