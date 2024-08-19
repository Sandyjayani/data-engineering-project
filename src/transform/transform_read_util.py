import boto3
import pandas as pd
from src.extraction.setup_logger import setup_logger
from src.extraction.get_timestamp import get_timestamp
from datetime import datetime
from io import StringIO


def ingestion_data_from_s3():

    logger = setup_logger("read_from_ingestion_s3_bucket_logger")
    BUCKET_NAME = "smith-morra-ingestion-bucket"
    TABLE_NAMES = [
        "payment",
        "payment_type",
        "currency",
        "staff",
        "department",
        "purchase_order",
        "transaction",
        "sales_order",
        "design",
        "address",
        "counterparty",
    ]

    s3 = boto3.client('s3')
    data_dicts = {}

    try:
        for table in TABLE_NAMES:
            logger.info(f"Load data from {table} from {BUCKET_NAME}")
            timestamp_datetime = get_timestamp(table)
            timestamp_str = timestamp_datetime.strftime("%Y-%m-%d_%H.%M.%S")

            s3_key = (
                f"{table}/"
                f"{timestamp_datetime.year}/"
                f"{timestamp_datetime.month}/"
                f"{timestamp_datetime.day}/"
                f"{timestamp_datetime.hour}-{timestamp_datetime.minute}/"
                f"{table}-{timestamp_str}.csv"
            )

            obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)['Body']

            df = pd.read_csv(StringIO(obj.read().decode('utf-8')))
            data_dicts[table] = df

            logger.info(f"Data from {table} loaded successfully")
        return data_dicts
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise e