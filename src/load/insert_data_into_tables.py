import pandas as pd
import os
from pg8000.native import literal, identifier

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
    from get_db_connection import create_connection
else:
    from src.transform.setup_logger import setup_logger
    from src.load.get_db_connection import create_connection

def insert_data_into_tables(df: pd.DataFrame, table_name: str):

    """
    Aim:
    Insert data from a pandas DataFrame into a specified database table.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data to be inserted.
    table_name (str): The name of the target table in the database.
    conn (psycopg2.extensions.connection): An active database connection object.

    Returns:
    None

    Description:
    This function takes a pandas DataFrame and inserts its contents into a specified
    database table. It uses the provided database connection to execute the insertion.
    The function performs the following steps:
    1. Sets up a logger for tracking the insertion process.
    2. Creates a cursor from the database connection.
    3. Converts the DataFrame to a list of tuples for efficient insertion.
    4. Generates SQL placeholders based on the number of columns in the DataFrame.
    5. Constructs an INSERT query using the table name and column names from the DataFrame.
    6. Executes the INSERT query for all rows using executemany for better performance.
    7. Commits the transaction and closes the cursor.

    If any error occurs during the process, it logs a critical error and raises an exception.

    Note: This function assumes that the structure of the DataFrame matches the structure
    of the target table in the database.
    """


    try:
        logger = setup_logger("load_logger")
        conn = create_connection('load')



        # Convert timestamp to date string
        # if 'date_id' in df.columns:
        if table_name == "dim_date":
            df['date_id'] = df['date_id'].dt.strftime('%Y-%m-%d')

        # if table_name == "fact_sales_order":
        #     df['created_date'] = df['created_date'].dt.strftime('%Y-%m-%d')
        #     df['last_updated_date'] = df['last_updated_date'].dt.strftime('%Y-%m-%d')
        #     df['agreed_delivery_date'] = df['agreed_delivery_date'].dt.strftime('%Y-%m-%d')
        #     df['agreed_payment_date'] = df['agreed_payment_date'].dt.strftime('%Y-%m-%d')
        #     df['created_time'] = df['created_date'].dt.strftime('%H:%M:%S.%f')
        #     df['last_updated_time'] = df['last_updated_date'].dt.strftime('%H:%M:%S.%f')

        # Convert DataFrame to list of tuples for efficient insertion
        # data_to_insert = [tuple(row) for row in df.to_numpy()]
        columns = ', '.join(identifier(col) for col in df.columns)
        row_count = 0


        # Iterate over DataFrame rows
        # The underscore (_) is used as a placeholder for the index we don't need
        if table_name == "fact_sales_order":
            for _, row in df.iterrows():
                # Construct the INSERT query
                insert_query = f"INSERT INTO {identifier(table_name)} ({columns}) VALUES ( :sales_order_id, :created_date, :created_time, :last_updated_date, :last_updated_time, :sales_staff_id, :counterparty_id, :units_sold, :unit_price, :currency_id, :design_id, :agreed_payment_date, :agreed_delivery_date, :agreed_delivery_location_id"  # noqa: E501
                # print(insert_query)
                conn.run(insert_query, sales_order_id=row['sales_order_id'], created_date=row['created_date'], created_time=row['created_time'], last_updated_date=row['last_updated_date'], last_updated_time=row['last_updated_time'], sales_staff_id=row['sales_staff_id'], counterparty_id=row['counterparty_id'], units_sold=row['units_sold'], unit_price=row['unit_price'], currency_id=row['currency_id'], design_id=row['design_id'], agreed_payment_date=row['agreed_payment_date'],  agreed_delivery_date=row['agreed_delivery_date'], agreed_delivery_location_id=row['agreed_delivery_location_id'])  # noqa: E501

                row_count += 1
        else:
            for _, row in df.iterrows():
                insert_query=f"INSERT INTO {identifier(table_name)} ({columns}) VALUES(tuple(row.values))"
                conn.run(insert_query)
                row_count += 1


        # print(insert_query)
        print(tuple(row.values))

        logger.info(f"{row_count} rows inserted into table {table_name} successfully")

    except Exception as e:
        logger.critical(f"Error inserting data into table {table_name}: {e}")
        raise e

    finally:
        if conn:
            conn.close
