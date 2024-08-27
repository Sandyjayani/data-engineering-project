import os
from datetime import datetime

if os.environ.get("AWS_EXECUTION_ENV"):
    from read_parquet_from_s3 import read_parquet_from_s3
    from insert_dim import insert_dim
    from insert_fact import insert_fact
    from setup_logger import setup_logger
    from save_load_timestamp import save_timestamps
else:
    from src.load.read_parquet_from_s3 import read_parquet_from_s3
    from src.load.insert_dim import insert_dim
    from src.load.insert_fact import insert_fact
    from src.load.setup_logger import setup_logger
    from src.load.save_load_timestamp import save_timestamps


def lambda_handler(event, context):
    """
    Aim:
    Lambda function to load data from S3 into a database.

    Parameters:
    event (dict): AWS Lambda event data (unused).
    context (object): AWS Lambda runtime information (unused).

    Returns:
    dict: Status code and message indicating operation result.

    Description:
    - Sets up logger
    - Processes predefined list of tables
    - For each table: reads parquet from S3, inserts into database
    - Logs process completion
    - Returns success response or raises exception on error

    Note:
    Assumes database connection is available. Handles errors by logging and re-raising.
    """
    logger = setup_logger("load_logger")
    try:
        logger.info("Starting load process")

        dict_df = read_parquet_from_s3()
        for table, df in dict_df.items():
            if "dim" in table:
                insert_dim(df, table)
                timestamp = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
                save_timestamps(table, timestamp)
            else:
                insert_fact(df, table)
                timestamp = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
                save_timestamps(table, timestamp)

        logger.info("Load process complete")

        return {"statusCode": 200, "body": f"Load process completed  successfully"}
    except Exception as e:
        logger.critical(f"Critical error: {str(e)}")
        raise e
