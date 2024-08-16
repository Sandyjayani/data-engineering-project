import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
from botocore.exceptions import ClientError
from util_functions.setup_logger import setup_logger  # Change path when lambda is ready


logger = setup_logger("extraction")


def get_timestamp(table_name: str) -> str:
    """Function takes name of table and searches for latest timestamp
    in csv file for that table. If no timestamp CSV exist, it will return
    timestamp of 0001-01-01_01-01

    parameters:
        - table_name: str

    return value:
        - timestamp: str (e.g., '2024-08-14_14-09')"""
    bucket_name = "smith-morra-ingestion-bucket"
    key = f"{table_name}/timestamps.csv"
    s3_client = boto3.client("s3")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)["Body"]
        timestamp_df = pd.read_csv(StringIO(response.read().decode("utf-8")))
        timestamp = timestamp_df["Date"].max()
        if not timestamp:
            timestamp = "0001-01-01_01-01"
        timestamp = datetime.strptime(timestamp, '%Y-%m-%d_%H-%M')
        print(timestamp)
        print(type(timestamp))
        logger.info(
            "Retrieved get_timestamp.",
            extra={"table_name": table_name, "bucket_name": bucket_name},
        )
        return timestamp

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchKey":
            logger.warning(
                f"No timestamps file found for table '{table_name}'. This might be the first run.",
                extra={"table_name": table_name, "bucket_name": bucket_name},
            )
            return datetime.strptime("0001-01-01_01-01", '%Y-%m-%d_%H-%M')
        else:
            logger.error(
                f"An AWS related error occurred: {e}",
                extra={"table_name": table_name, "bucket_name": bucket_name},
            )
            raise e

    except Exception as e:
        logger.exception(
            f"An unexpected error occurred: {e}",
            extra={"table_name": table_name, "bucket_name": bucket_name},
        )
        raise e


# TypeError: catching classes that do not inherit from BaseException is not allowed
# except s3_client.exceptions.NoSuchKey:
#     logger.error(f"No timestamps file found for table '{table_name}'. This might be the first run.",
#                 extra={'table_name': table_name, 'bucket_name': bucket_name})
#     return '0001-01-01_01-01'

# except ClientError as e:
#     logger.error(f"An AWS related error occurred: {e}",
#                 extra={'table_name': table_name, 'bucket_name': bucket_name})
#     raise e
