import boto3
from io import StringIO
import pandas as pd
import os
from datetime import datetime

if os.environ.get("AWS_EXECUTION_ENV") is not None:
    from setup_logger import setup_logger
else:
    from src.load.setup_logger import setup_logger


def save_timestamps(table_name: str, timestamp: str):
    """
    - create a var for the file key
    - download the existing timestamps csv if it exits
    - if timestamps csv does not exists, create an empty one
    - create a new dataframe to store the currect timestamp
    - concate the two dataframes
    - upload the combined dataframe, allow overwriting
    """
    logger = setup_logger("Save timestamps logger")
    s3_client = boto3.client("s3")

    timestamp_key = f"{table_name}/load_timestamp.csv"
    try:
        bucket_name = "smith-morra-transformation-bucket"
        # download the existing timestamps csv if it exits
        logger.debug(
            "Attempting to download timestamps CSV from"
            + " S3 with key: {timestamp_key}",
            extra={"table_name": table_name, "bucket_name": bucket_name},
        )
        response = s3_client.get_object(Bucket=bucket_name, Key=timestamp_key)
        timestamp_df = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")))

    except s3_client.exceptions.NoSuchKey:
        # if timestamps csv does not exists, create an empty one
        logger.warning("Timestamps CSV not found in S3. Creating a new one.")
        timestamp_df = pd.DataFrame(columns=["Date"])

    except Exception as e:
        logger.error(
            f"An unexpected error occurred: {e}",
            extra={"table_name": table_name, "bucket_name": bucket_name},
        )
        return "Failed to download timestamps file"

    new_timestamp_df = pd.DataFrame({"Date": [timestamp]})
    updated_timestamp_df = pd.concat(
        [timestamp_df, new_timestamp_df], ignore_index=True
    )

    timestamp_csv_buffer = StringIO()
    updated_timestamp_df.to_csv(timestamp_csv_buffer, index=False)
    timestamp_csv_buffer.seek(0)

    try:
        logger.debug(
            "Attempting to upload timestamps CSV from "
            + f"S3 with key: {timestamp_key}",
            extra={"table_name": table_name, "bucket_name": bucket_name},
        )
        s3_client.put_object(
            Bucket=bucket_name, Key=timestamp_key, Body=timestamp_csv_buffer.getvalue()
        )

    except Exception as e:
        logger.debug(
            f"An unexpected error occurred while uploading timestamps CSV:{e}",
            extra={"table_name": table_name, "bucket_name": bucket_name},
        )
        raise e
