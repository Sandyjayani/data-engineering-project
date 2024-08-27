import os

if os.environ.get("AWS_EXECUTION_ENV"):
    from read_parquet_from_s3 import read_parquet_from_s3
    from insert_data_into_tables import insert_data_into_tables
    from setup_logger import setup_logger
else:
    from src.load.read_parquet_from_s3 import read_parquet_from_s3
    from src.load.insert_data_into_tables import insert_data_into_tables
    from src.load.setup_logger import setup_logger


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
        logger.info('Starting load process')


        dict_df = read_parquet_from_s3()
        for table,df in dict_df.items():

            insert_dim(df, table)

        logger.info('Load process complete')
        return {
            'statusCode': 200,
            'body': f"Load process completed  successfully"
        }
    except Exception as e:
        logger.critical(f"Critical error: {str(e)}")
        raise e

