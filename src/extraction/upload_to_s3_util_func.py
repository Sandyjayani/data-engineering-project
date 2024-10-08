from datetime import datetime
import boto3
from io import StringIO, BytesIO
import pandas as pd
from botocore.exceptions import ClientError
import os

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
else:
    from src.extraction.setup_logger import setup_logger


def upload_tables_to_s3(
    table_data: pd.DataFrame | None, table_name: str, bucket_name: str
) -> str:
    """
    IMPORTANT: This function would only save and upload the dfas parquet
    if thebucket name contains the word "transform", case insensitive
    csv otherwise

    - get the current timestamp
    - call save_timestamps to save the current timestamp in a csv
    file (for the get_timestamp func
    [which generate the timestamp as an input for get_table to input)
    - create a var for the file key in
    "[Table Name]/Year/Month/Day/hh-mm/[tablename]-[timestamp].csv"
    - convert the given dataframe to csv (should be written to an in-memory
    buffer, not via local
    file and delete)
    - upload the csv from the buffer to the s3
    - return a confirmation message with the upload details
    Note: once to consider between csv and json, given our data structure
    (multiple tabular and
    relational tables), csv seems to be a better choice.
    """

    # get the current timestamp

    logger = setup_logger("Upload table to s3 logger")

    timestamp_datetime = datetime.now()
    timestamp_str = timestamp_datetime.strftime("%Y-%m-%d_%H.%M.%S")

    logger.debug(
        f"Timestamp generated for the upload: {timestamp_str}",
        extra={"table_name": table_name, "bucket_name": bucket_name},
    )

    if "transform" in bucket_name.lower():
        file_type = "parquet"
    else:
        file_type = "csv"

    # create a var for the file key in
    # "[Table Name]/Year/Month/Day/hh-mm/[tablename]-[timestamp].csv"

    s3_key = (
        f"{table_name}/"
        f"{timestamp_datetime.year}/"
        f"{timestamp_datetime.month}/"
        f"{timestamp_datetime.day}/"
        f"{timestamp_datetime.hour}-{timestamp_datetime.minute}/"
        f"{table_name}-{timestamp_str}.{file_type}"
    )

    logger.debug(
        f"S3 key for the file: {s3_key}",
        extra={"table_name": table_name, "s3_key": s3_key},
    )

    try:

        if isinstance(table_data, pd.DataFrame):
            # written to an in-memory buffer
            # convert the given dataframe to csv
            # reposition stream to the beginning

            if file_type == "parquet":
                buffer = BytesIO()
                table_data.to_parquet(buffer, index=False)
            else:
                buffer = StringIO()
                table_data.to_csv(buffer, index=False)

            buffer.seek(0)

            # upload the csv from the buffer to the s3
            s3_client = boto3.client("s3")

            s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
            logger.info(
                f"Table {table_name} has been uploaded to {bucket_name} "
                f"with key {s3_key}.",
                extra={
                    "table_name": f"{table_name}",
                    "bucket_name": f"{bucket_name}",
                    "s3_key": f"{s3_key}",
                },
            )
            save_timestamps(table_name, timestamp_str, bucket_name)
            return (
                f"Table {table_name} has been uploaded to "
                + f"{bucket_name} with key {s3_key}."
            )
        return "No new data to upload"

    except ClientError as e:
        logger.error(
            f"Failed to upload CSV file for table {table_name} "
            + "to S3 bucket {bucket_name}: {e}",
            extra={
                "table_name": f"{table_name}",
                "bucket_name": f"{bucket_name}",
                "s3_key": f"{s3_key}",
            },
        )
        raise e


def save_timestamps(table_name: str, timestamp: str, bucket_name: str):
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

    timestamp_key = f"{table_name}/timestamps.csv"
    try:
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
