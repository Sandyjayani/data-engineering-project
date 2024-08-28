import pandas as pd
import os
import boto3


if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger  # type: ignore
else:
    from src.extraction.setup_logger import setup_logger


def generate_date_table() -> pd.DataFrame:
    logger = setup_logger("transform_dim_date")

    try:
        logger.info("Starting creation of dim_date")

        start_date = pd.to_datetime("2022-01-01")
        end_date = pd.to_datetime("2024-12-31")
        date_range = pd.date_range(start=start_date, end=end_date, freq="D")

        df = pd.DataFrame(
            {
                "date_id": date_range,
                "year": date_range.year,
                "month": date_range.month,
                "day": date_range.day,
                "day_of_week": date_range.dayofweek,
                "day_name": date_range.strftime("%A"),
                "month_name": date_range.strftime("%B"),
                "quarter": date_range.quarter,
            }
        )

        logger.info("dim_date creation completed successfully")
        return df

    except Exception as e:
        logger.error(f"Error in transform_dim_date : {str(e)}")
        return pd.DataFrame()


def transform_date() -> pd.DataFrame:
    """
    Creates a date dimension DataFrame and returns it.

    Returns:
        pd.DataFrame: DataFrame containing the date dimension.
    """

    logger = setup_logger("transform_dim_date")

    s3_client = boto3.client("s3")
    BUCKET_NAME = "smith-morra-transformation-bucket"
    FOLDER_PREFIX = "dim_date"

    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FOLDER_PREFIX)
        if "Contents" in response:
            return None
        return generate_date_table()

    except Exception as e:
        raise e
