import pandas as pd
import os
from pg8000.native import literal, identifier

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
    from get_db_connection import create_connection
else:
    from src.transform.setup_logger import setup_logger
    from src.load.get_db_connection import create_connection

def insert_data_into_fact_tables(df: pd.DataFrame, table_name: str):
    logger = setup_logger("load_logger")
    try:
        conn = create_connection('load')
        
        columns = ', '.join(identifier(col) for col in df.columns)
        row_count = 0


        # Iterate over DataFrame rows
        # The underscore (_) is used as a placeholder for the index we don't need
        if table_name == "fact_sales_order":
            for _, row in df.iterrows():
                # Construct the INSERT query
                insert_query = f"INSERT INTO {identifier(table_name)} ({columns}) VALUES ( :sales_order_id, :created_date, :created_time, :last_updated_date, :last_updated_time, :sales_staff_id, :counterparty_id, :units_sold, :unit_price, :currency_id, :design_id, :agreed_payment_date, :agreed_delivery_date, :agreed_delivery_location_id)"  # noqa: E501
                # print(insert_query)
                conn.run(insert_query, sales_order_id=row['sales_order_id'], created_date=row['created_date'], created_time=row['created_time'], last_updated_date=row['last_updated_date'], last_updated_time=row['last_updated_time'], sales_staff_id=row['sales_staff_id'], counterparty_id=row['counterparty_id'], units_sold=row['units_sold'], unit_price=row['unit_price'], currency_id=row['currency_id'], design_id=row['design_id'], agreed_payment_date=row['agreed_payment_date'],  agreed_delivery_date=row['agreed_delivery_date'], agreed_delivery_location_id=row['agreed_delivery_location_id'])  # noqa: E501

                row_count += 1
        
                logger.info(f"{row_count} rows inserted into table {table_name} successfully")

    except Exception as e:
        logger.critical(f"Error inserting data into table {table_name}: {e}")
        raise e

    finally:
        if conn:
            conn.close