from datetime import datetime
import boto3
from io import StringIO
import pandas as pd
from botocore.exceptions import ClientError
import logging
from setup_logger import setup_logger

logger = setup_logger('extraction_looger')
# logger = logging.getLogger('save_timestamps')
# logger.setLevel(logging.DEBUG)


def upload_tables_to_s3(
    dataframe: pd.DataFrame, table_name: str, bucket_name: str
) -> pd.DataFrame:
    """
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

    # get the current timestampe

    logger.info(
        "Starting upload_tables_to_s3 function.",
        extra={"table_name": table_name, "bucket_name": bucket_name},
    )

    timestamp_datetime = datetime.now()
    timestamp_str = timestamp_datetime.strftime("%Y-%m-%d_%H-%M")

    logger.debug(
        f"Timestamp generated for the upload: {timestamp_str}",
        extra={"table_name": table_name, "bucket_name": bucket_name},
    )
    save_timestamps(table_name, timestamp_str, bucket_name)
    # create a var for the file key in
    # "[Table Name]/Year/Month/Day/hh-mm/[tablename]-[timestamp].csv"

    s3_key = (
        f"{table_name}/"
        f"{timestamp_datetime.year}/"
        f"{timestamp_datetime.month}/"
        f"{timestamp_datetime.day}/"
        f"{timestamp_datetime.hour}-{timestamp_datetime.minute}/"
        f"{table_name}-{timestamp_str}.csv"
    )

    logger.debug(
        f"S3 key for the file: {s3_key}",
        extra={"table_name": table_name, "s3_key": s3_key},
    )

    # written to an in-memory buffer
    csv_buffer = StringIO()
    # convert the given dataframe to csv
    dataframe.to_csv(csv_buffer, index=False)
    # reposition stream to the beginning
    csv_buffer.seek(0)

    # upload the csv from the buffer to the s3
    s3_client = boto3.client("s3")

    try:
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
        logger.info(
            f"Table {table_name} has been uploaded to {bucket_name} "
            f"with key {s3_key}.",
            extra={
                "table_name": f"{table_name}",
                "bucket_name": f"{bucket_name}",
                "s3_key": f"{s3_key}",
            },
        )
        return (
            f"Table {table_name} has been uploaded to "
            + f"{bucket_name} with key {s3_key}."
        )

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


# A test to see the demo
# data = {
#     'Column1': [1, 2, 3],
#     'Column2': ['A', 'B', 'C'],
#     'Column3': [10.5, 20.75, 30.25]
# }
# test_data = pd.DataFrame(data)
# upload_tables_to_s3(test_data,'test_table', 'test-ingestion-s3-test-nc-9')

# data = {
#     'Column1': [4, 5, 6],
#     'Column2': ['D', 'E', 'F'],
#     'Column3': [11.5, 21.75, 31.25]
# }
# test_data = pd.DataFrame(data)
# upload_tables_to_s3(test_data,'test_table', 'test-ingestion-s3-test-nc-9')
