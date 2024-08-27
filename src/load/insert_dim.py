import pandas as pd
import os
from pg8000.native import literal, identifier

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
    from get_db_connection import create_connection
else:
    from src.transform.setup_logger import setup_logger
    from src.load.get_db_connection import create_connection

def insert_dim(df: pd.DataFrame, table_name: str):


    logger = setup_logger("load_logger")
    try:
        conn = create_connection('load')



        # Convert timestamp to date string
        # if 'date_id' in df.columns:
        if table_name == "dim_date":
            df['date_id'] = df['date_id'].dt.strftime('%Y-%m-%d')

        columns = ', '.join(identifier(col) for col in df.columns)
        row_count = 0
        column_lst = df.columns.tolist()
        primary_key = column_lst[0]
        


        for _, row in df.iterrows():
            update_query = [f"{col} = {literal(val)}" for col, val in zip(column_lst[1:], row[1:])]
            update_query = ", ".join(update_query)
            print(update_query)
            insert_query=f"INSERT INTO {identifier(table_name)} ({columns}) VALUES{tuple(row.values)} ON CONFLICT ({identifier(primary_key)}) DO UPDATE SET {update_query}"
            print(insert_query)
            
            conn.run(insert_query)
            row_count += 1


        print(tuple(row.values))

        logger.info(f"{row_count} rows inserted into table {table_name} successfully")

    except Exception as e:
        logger.critical(f"Error inserting data into table {table_name}: {e}")
        raise e

    finally:
        if conn:
            conn.close

insert_dim(pd.read_parquet('test/test_load/test_data/dim_date-2024-08-23_11.05.48.parquet'), "dim_date")