import pandas as pd
import os
from pg8000.native import literal, identifier

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
    from get_db_connection import create_connection
else:
    from src.transform.setup_logger import setup_logger
    from src.load.get_db_connection import create_connection


def insert_fact(df: pd.DataFrame, table_name: str):
    """
    Insert data from a DataFrame into a specified database table.

    This function connects to a database, constructs an INSERT query based on the
    DataFrame columns, and inserts each row of the DataFrame into the specified table.
    It's specifically tailored for the 'fact_sales_order' table.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be inserted.
        table_name (str): The name of the table to insert the data into.

    Raises:
        Exception: If there's an error during the database insertion process.

    Logs:
        Info: Number of rows inserted successfully.
        Critical: Any errors that occur during insertion.
    """

    logger = setup_logger("load_logger")
    try:
        conn = create_connection("load")

        columns = ", ".join(identifier(col) for col in df.columns)
        row_count = 0

        # Iterate over DataFrame rows
        # The underscore (_) is used as a placeholder for the index we don't need
        if table_name == "fact_sales_order":
            for _, row in df.iterrows():
                # Construct the INSERT query
                insert_query = f"INSERT INTO {identifier(table_name)} ({columns}) VALUES ( :sales_order_id, :created_date, :created_time, :last_updated_date, :last_updated_time, :sales_staff_id, :counterparty_id, :units_sold, :unit_price, :currency_id, :design_id, :agreed_payment_date, :agreed_delivery_date, :agreed_delivery_location_id)"  # noqa: E501
                conn.run(
                    insert_query,
                    sales_order_id=row["sales_order_id"],
                    created_date=row["created_date"],
                    created_time=row["created_time"],
                    last_updated_date=row["last_updated_date"],
                    last_updated_time=row["last_updated_time"],
                    sales_staff_id=row["sales_staff_id"],
                    counterparty_id=row["counterparty_id"],
                    units_sold=row["units_sold"],
                    unit_price=row["unit_price"],
                    currency_id=row["currency_id"],
                    design_id=row["design_id"],
                    agreed_payment_date=row["agreed_payment_date"],
                    agreed_delivery_date=row["agreed_delivery_date"],
                    agreed_delivery_location_id=row["agreed_delivery_location_id"],
                )  # noqa: E501
                row_count += 1

                logger.info(
                    f"{row_count} rows inserted into table {table_name} successfully"
                )

    except Exception as e:
        logger.critical(f"Error inserting data into table {table_name}: {e}")
        raise e

    finally:
        if "conn" in locals():
            conn.close

