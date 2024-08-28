import pandas as pd
import os
from pg8000.native import identifier


if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
    from get_db_connection import create_connection
else:
    from src.transform.setup_logger import setup_logger
    from src.load.get_db_connection import create_connection



def insert_dim(df: pd.DataFrame, table_name: str):
    """
    Upsert dimensional data into a specified database table.

    Handles insertion or update of data from a DataFrame into various dimensional
    tables. Supports custom handling for dim_date, dim_location, dim_staff, and
    dim_counterparty tables, with a generic approach for others.

    Args:
        df (pd.DataFrame): Data to be upserted.
        table_name (str): Target database table name.

    Raises:
        Exception: On database insertion errors.

    Logs:
        Info: Successful insertion count.
        Critical: Any errors encountered.
    """
    logger = setup_logger("load_logger")
    try:
        conn = create_connection("load")

        # Convert timestamp to date string
        if table_name == "dim_date":
            df["date_id"] = df["date_id"].dt.strftime("%Y-%m-%d")

        columns = ", ".join(identifier(col) for col in df.columns)
        row_count = 0
        column_lst = df.columns.tolist()
        primary_key = column_lst[0]

        if table_name == "dim_location":
            for _, row in df.iterrows():
                update_query = ", ".join(
                    [
                        f"{identifier(col)} = EXCLUDED.{identifier(col)}"
                        for col in column_lst
                    ]
                )
                insert_query = f"INSERT INTO {identifier(table_name)} ({columns}) VALUES (:location_id, :address_line_1, :address_line_2, :district, :city, :postal_code, :country, :phone) ON CONFLICT ({identifier(primary_key)}) DO UPDATE SET {update_query}"

                conn.run(
                    insert_query,
                    location_id=row.location_id,
                    address_line_1=row.address_line_1,
                    address_line_2=row.address_line_2,
                    district=row.district,
                    city=row.city,
                    postal_code=row.postal_code,
                    country=row.country,
                    phone=row.phone,
                )
                row_count += 1

        elif table_name == "dim_staff":
            for _, row in df.iterrows():
                update_query = ", ".join(
                    [
                        f"{identifier(col)} = EXCLUDED.{identifier(col)}"
                        for col in column_lst
                    ]
                )            
                insert_query = f"INSERT INTO {identifier(table_name)} ({columns}) VALUES (:staff_id, :first_name, :last_name, :department_name, :location, :email_address) ON CONFLICT ({identifier(primary_key)}) DO UPDATE SET {update_query}"
                conn.run(
                    insert_query,
                    staff_id=row.staff_id,
                    first_name=row.first_name,
                    last_name=row.last_name,
                    department_name=row.department_name,
                    location=row.location,
                    email_address=row.email_address
                )
                row_count += 1

        elif table_name == "dim_counterparty":
            for _, row in df.iterrows():
                update_query = ", ".join(
                    [
                        f"{identifier(col)} = EXCLUDED.{identifier(col)}"
                        for col in column_lst
                    ]
                )
                insert_query = f"INSERT INTO {identifier(table_name)} ({columns}) VALUES (:counterparty_id, :counterparty_legal_name, :counterparty_legal_address_line_1, :counterparty_legal_address_line_2, :counterparty_legal_district, :counterparty_legal_city, :counterparty_legal_postal_code, :counterparty_legal_country, :counterparty_legal_phone_number) ON CONFLICT ({identifier(primary_key)}) DO UPDATE SET {update_query}"
                conn.run(
                    insert_query,
                    counterparty_id=row.counterparty_id,
                    counterparty_legal_name=row.counterparty_legal_name,
                    counterparty_legal_address_line_1=row.counterparty_legal_address_line_1,
                    counterparty_legal_address_line_2=row.counterparty_legal_address_line_2,
                    counterparty_legal_district=row.counterparty_legal_district,
                    counterparty_legal_city=row.counterparty_legal_city,
                    counterparty_legal_postal_code=row.counterparty_legal_postal_code,
                    counterparty_legal_country=row.counterparty_legal_country,
                    counterparty_legal_phone_number=row.counterparty_legal_phone_number,
                )
                row_count += 1
        else:
            for _, row in df.iterrows():
                update_query = ", ".join(
                    [
                        f"{identifier(col)} = EXCLUDED.{identifier(col)}"
                        for col in column_lst
                    ]
                )
                insert_query = f"INSERT INTO {identifier(table_name)} ({columns}) VALUES {tuple(row.values)} ON CONFLICT ({identifier(primary_key)}) DO UPDATE SET {update_query}"
                conn.run(insert_query)
                row_count += 1

        logger.info(f"{row_count} rows inserted into table {table_name} successfully")

    except Exception as e:
        logger.critical(f"Error inserting data into table {table_name}: {e}")
        raise e

    finally:
        if conn:
            conn.close
