import pandas as pd
from copy import deepcopy
from datetime import date, time, datetime
from re import compile, match
import os
import numpy

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger # type: ignore
else:
    from src.extraction.setup_logger import setup_logger


def facts_table(dataframe):
    """
    takes a sales_order OLTP dataframe

    returns a dataframe formatted for the facts_sales_orders OLAP table (minus its primary key)

    the "sales_record_id" column will need to be populated when appending to the database, 
    as it is a serial primary key and it cannot be assumed beforehand
    """
    logger = setup_logger("transform_fact_sales_order")

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

    # checking for any null values
    if dataframe.isnull().values.any():
        logger.error("Missing value(s).")
        return None
    logger.info("Input dataframe is from sales_order and does not contain null values.")

    logger.info("Starting iteration.")
    facts_columns = [
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

    all_rows = []
    for index in range(len(dataframe)):
        row = dataframe.loc[index]

        logger.info("Validating row values.")
        # validating timestamps:
        timestamp = row.iloc[1]
        secondary_timestamp = row.iloc[2]
        delivery_date = row.iloc[9]
        payment_date = row.iloc[10]

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
        
        # validating numbers
        for index_value in [
            0,4,5,8,3,11,6
        ]:
            value = row.iloc[index_value]
            if type(value) != numpy.int64:
                logger.error("One or more integer value(s) is not an integer.")
                return None

        unit_price_check = row.iloc[7]
        if type(unit_price_check) != numpy.float64:
            logger.error("The one or more unit_price value(s) is not a float.")
            return None

        logger.info("Row values have passed validation.")

        logger.info("Starting transformation.")
        # transforming validated data
        order_id = row.iloc[0]
        created_date = datetime.strptime(row.iloc[1], "%Y-%m-%d %H:%M:%S.%f").date()
        created_time = datetime.strptime(row.iloc[1], "%Y-%m-%d %H:%M:%S.%f").time()
        last_updated_date = datetime.strptime(row.iloc[2], "%Y-%m-%d %H:%M:%S.%f").date()
        last_updated_time = datetime.strptime(row.iloc[2], "%Y-%m-%d %H:%M:%S.%f").time()
        sales_staff_id = row.iloc[4]
        counterparty_id = row.iloc[5]
        units_sold = row.iloc[6]
        unit_price = row.iloc[7]
        currency_id = row.iloc[8]
        design_id = row.iloc[3]
        agreed_payment_date = datetime.strptime(row.iloc[10], "%Y-%m-%d").date()
        agreed_delivery_date = datetime.strptime(row.iloc[9], "%Y-%m-%d").date()
        agreed_delivery_location_id = row.iloc[11]

        new_row = [
            order_id,
            created_date,
            created_time,
            last_updated_date,
            last_updated_time,
            sales_staff_id,
            counterparty_id,
            units_sold,
            unit_price,
            currency_id,
            design_id,
            agreed_payment_date,
            agreed_delivery_date,
            agreed_delivery_location_id,
        ]

        new_series = pd.Series(data=new_row, index=facts_columns)
        logger.info("Transformed row has been created.")

        all_rows.append(new_series)
    logger.info("Iteration complete.")

    logger.info("Creating new dataframe.")
    new_df = pd.DataFrame(data=all_rows, columns=facts_columns)

    return new_df