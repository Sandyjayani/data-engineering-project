import boto3
import pandas as pd
import os
from io import StringIO

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
    from get_ingestion_timestamp import get_ingestion_timestamp
    from get_transformation_timestamp import get_transformation_timestamp
else:
    from src.transform.setup_logger import setup_logger
    from src.transform.get_ingestion_timestamp import get_ingestion_timestamp
    from src.transform.get_transformation_timestamp import get_transformation_timestamp


def load_ingested_tables():
    """
    This function reads latest file from s3 ingestion bucket and returns dict of DataFrames.
    This function connects to the "smith-morra-ingestion-bucket" s3 bucket and retrieves the latest csv files from each table and reads the CSV files into pandas DataFrames. the DataFrames are stored in a dictionary with the table names as keys.

    Returns:
        Dict: key-table_name, value-df

    Raises:
        Exception: if there is an error loading data from s3, an exception is raised.

    Logging:
        Logs at the start and successful completion for each table. logs error if data loading fails.
    """

    logger = setup_logger("read_from_ingestion_s3_bucket_logger")
    BUCKET_NAME = "smith-morra-ingestion-bucket"

    TRANSFORMED_TABLES = {  # look up dict
        "dim_counterparty": ["counterparty", "address"],
        "dim_currency": ["currency"],
        "dim_design": ["design"],
        "dim_location": ["address"],
        "dim_staff": ["staff", "department"],
        "fact_sales_order": ["sales_order"],
    }

    s3 = boto3.client("s3")
    data_dicts = {}

    try:
        for transformed_table in TRANSFORMED_TABLES:
            for table in TRANSFORMED_TABLES[
                transformed_table
            ]:  # getting relevant ingestion tables for each dim_table
                logger.info(f"Loading data from {table} from {BUCKET_NAME}")
                ingestion_timestamp_datetime = get_ingestion_timestamp(table)
                transformation_timestamp_dt = get_transformation_timestamp(
                    transformed_table
                )
                timestamp_str = ingestion_timestamp_datetime.strftime(
                    "%Y-%m-%d_%H.%M.%S"
                )
                if (
                    ingestion_timestamp_datetime > transformation_timestamp_dt
                    and table not in data_dicts.keys()
                ):  # avoiding unnecessary double-runs for address
                    s3_key = (
                        f"{table}/"
                        f"{ingestion_timestamp_datetime.year}/"
                        f"{ingestion_timestamp_datetime.month}/"
                        f"{ingestion_timestamp_datetime.day}/"
                        f"{ingestion_timestamp_datetime.hour}-{ingestion_timestamp_datetime.minute}/"
                        f"{table}-{timestamp_str}.csv"
                    )

                    obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)["Body"]

                    df = pd.read_csv(StringIO(obj.read().decode("utf-8")))
                    data_dicts[table] = df

                    logger.info(f"Data from {table} loaded successfully")
                else:
                    logger.info(f"No new data to transform for {table} table.")
        return data_dicts
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise e
