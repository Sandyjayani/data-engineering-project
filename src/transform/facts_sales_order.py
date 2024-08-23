import pandas as pd
from copy import deepcopy
from datetime import datetime
import os

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
else:
    from src.transform.setup_logger import setup_logger  

def transform_sales_order(raw_dataframe):
    """
    takes a sales_order OLTP dataframe

    returns a dataframe formatted for the facts_sales_orders OLAP table (minus its primary key)

    the "sales_record_id" column will need to be populated when appending to the database, 
    as it is a serial primary key and it cannot be assumed beforehand
    """
    logger = setup_logger('transform_staff')
    logger.info('Starting transform_sales_order')

    expected_columns = [
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
        
    if list(raw_dataframe.columns) != expected_columns:
        raise ValueError('Dataframe columns do not match the expected schema')

    dataframe = deepcopy(raw_dataframe)

    dataframe['created_at'] = pd.to_datetime(dataframe['created_at'])
    dataframe['last_updated'] = pd.to_datetime(dataframe['last_updated'])
    dataframe['agreed_payment_date'] = pd.to_datetime(dataframe['agreed_payment_date']).dt.date
    dataframe['agreed_delivery_date'] = pd.to_datetime(dataframe['agreed_delivery_date']).dt.date

    new_df = pd.DataFrame({
        "sales_order_id": dataframe["sales_order_id"],
        "created_date": dataframe['created_at'].dt.date,
        "created_time": dataframe['created_at'].dt.time,
        "last_updated_date": dataframe['last_updated'].dt.date,
        "last_updated_time": dataframe['last_updated'].dt.time,
        "sales_staff_id": dataframe["staff_id"],
        "counterparty_id": dataframe["counterparty_id"],
        "units_sold": dataframe["units_sold"],
        "unit_price": dataframe["unit_price"],
        "currency_id": dataframe["currency_id"],
        "design_id": dataframe["design_id"],
        "agreed_payment_date": dataframe["agreed_payment_date"],
        "agreed_delivery_date": dataframe["agreed_delivery_date"],
        "agreed_delivery_location_id": dataframe["agreed_delivery_location_id"]
    })

    logger.info('Transform_sales_order completed')
    return new_df