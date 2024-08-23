import boto3
import pandas as pd
import os
from io import BytesIO


if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
    from get_load_timestamp import get_load_timestamp
    from get_transformation_timestamp import get_transformation_timestamp
else:
    from src.load.setup_logger import setup_logger
    from src.load.get_load_timestamp import get_load_timestamp
    from src.load.get_transformation_timestamp import get_transformation_timestamp


def read_parquet_from_s3() -> dict:

    """
    Aim: 
    Read the most recent parquet file for a given table from an S3 bucket.

    Parameters:
    table_name (str): The name of the table corresponding to the parquet file.

    Returns:
    pd.DataFrame: A pandas DataFrame containing the data from the parquet file.

    Description:
    This function connects to an S3 bucket, identifies the most recent parquet file
    for the specified table, downloads its content, and returns it as a DataFrame.
    It uses boto3 for S3 operations and pandas for parquet file handling.

    The function lists objects in the S3 bucket, filters for parquet files,
    selects the most recent one, downloads its content, and reads it into a DataFrame.
    If any error occurs, it logs a critical error and raises an exception.

    Note: The S3 bucket name is hardcoded as "smith-morra-transformation-bucket".
    """

    logger = setup_logger("load_logger")
    s3_client = boto3.client("s3")
    bucket_name = "smith-morra-transformation-bucket"

    try:
        data_dicts = {}

        table_list = ["dim_date", "dim_location", "dim_design", "fact_sales_order", "dim_currency", "dim_counterparty", "dim_staff"]  # noqa: E501
        for table_name in table_list:

            logger.info(f"Reading parquet file from S3 for table: {table_name}")


            load_timestamp = get_load_timestamp(table_name)

            transformation_dt = get_transformation_timestamp(table_name)
            timestamp_str = transformation_dt.strftime("%Y-%m-%d_%H.%M.%S")
            if transformation_dt > load_timestamp:
                s3_key = (
                    f"{table_name}/"
                    f"{transformation_dt.year}/"
                    f"{transformation_dt.month}/"
                    f"{transformation_dt.day}/"
                    f"{transformation_dt.hour}-{transformation_dt.minute}/"
                    f"{table_name}-{timestamp_str}.parquet"
                )

                obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)['Body']

                df = pd.read_parquet(BytesIO(obj.read()))
                data_dicts[table_name] = df

                logger.info(f"Data from {table_name} loaded successfully")
            else:
                logger.info(f"No new data to transform for {table_name} table.")
        return data_dicts

    except Exception as e:
        logger.critical(f"Error reading parquet file from S3: {e}")
        raise e

result = read_parquet_from_s3()
print(result.keys())
print(type(result))