from util_functions.get_db_connection import create_connection
from util_functions.get_table import get_table
from util_functions.upload_to_s3_util_func import upload_tables_to_s3
from util_functions.setup_logger import setup_logger
from util_functions.get_timestamp import get_timestamp
from requests import Response
from datetime import datetime


def lambda_handler(event, context):
    """
    queries totesys database

    collects any updates to it since last checked

    uploads new data to s3 bucket

    returns response object with 201 status
    """

    # notes during dev:

    # event is caused by scheduler/step function telling it to run
    # what format is that? will we even use that to begin with?

    # if an error is raised at any point during this
    # we allow it to be raised in order to handle it in the step function
    # -> do we need an except block then?


    try:
        logger = setup_logger('extraction_logger')

        logger.info('Creating connection.')
        conn = create_connection()
        logger.info('Connection has been created.')

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
            "counterparty"
        ]

        for table_name in sorted(TABLE_NAMES):

            logger.info(f'Getting last timestamp for {table_name} table.')
            last_timestamp = get_timestamp(table_name)
            last_timestamp = datetime.strptime(last_timestamp, '%Y-%m-%d_%H-%M')
            logger.info(f'Last timestamp for {table_name} table ({last_timestamp}) has been retrieved')

            logger.info(f'Getting newest table data from {table_name} table.')
            table_data = get_table(table_name, conn, last_timestamp)
            logger.info(f'Successfully retrieved new table data from {table_name} table.')

            logger.info(f'Uploading newest {table_name} data to s3 bucket.')
            upload_response = upload_tables_to_s3(table_data, table_name, BUCKET_NAME)
            logger.info(f'Successfully uploaded newest {table_name} data to s3 bucket.')
        
        logger.info('Table iteration has finished.')

        logger.info('Creating output response object.')
        response_message = Response()
        response_message.status_code = 201
        # response_message.text = 'new data was successfully uploaded'
        return response_message
    
    finally:
        if "conn" in locals():
            logger.info('Closing connection.')
            conn.close()
# lambda_handler({},{})


# def lambda_handler(event,context):
#     logger = setup_logger('extraction_logger')
#     logger.info('Testing 123')

#     print('Test')
#     logger.critical('test for alert')