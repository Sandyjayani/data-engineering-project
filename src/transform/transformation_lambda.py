import os
import json

if os.environ.get("AWS_EXECUTION_ENV"):
    from upload_to_transformation_s3 import upload_to_transformation_s3
    from setup_logger import setup_logger
    from dim_currency import transform_currency
    from dim_date import transform_date
    from transform.facts_sales_order import facts_table
    from transform.dim_design import transform_design
    from transform.dim_location import transform_location
    from transform.load_ingested_tables import load_ingested_tables
else:
    from src.transform.upload_to_transformation_s3 import upload_to_transformation_s3
    from src.transform.setup_logger import setup_logger
    from src.transform.dim_currency import transform_currency
    from src.transform.dim_date import transform_date
    from src.transform.facts_sales_order import facts_table
    from src.transform.dim_design import transform_design
    from src.transform.dim_location import transform_location
    from src.transform.load_ingested_tables import load_ingested_tables


def lambda_handler(event, context):
    """
    The function will be triggered within a step function
    after the successful execution of the extraction lambda.

    It:

    - instantiates a logger using the setup_logger util func
    - logs:
        - the completion of steps within its execution
        - any execptions that are raised
    - calls the load_ingestion_data to create a dictionary where:
        key: table_name
        value: dataframe containing any newly ingested data for that table
    - calls the transformation functions for each OLAP table,
        passing in the dictionary or a single dataframe
        and capturing the output transformed dataframes.
    - calls the upload_tables_to_s3 function, passing in
        the transformed dataframes to be saved as parquet tables.


    """
    try:
        logger = setup_logger("transformation_logger")
        logger.info('Starting transformation process')
        
        # loads any newly ingested data as dictionary of dataframes per table
        new_data_dict = load_ingested_tables()

        # passes dict to transform_currency, if there is new currency data
        # it will be passed to the upload_to_transformation_s3 
        transformed_currency_data = transform_currency(new_data_dict)
        if transformed_currency_data is not None:
            upload_to_transformation_s3(transformed_currency_data, 'currency')

        # transformed_counterparty_data = transform_counterparty(new_data_dict)
        # if transformed_currency_data:
        #     upload_to_transformation_s3(transformed_currency_data, 'counterparty')
        
        transformed_design_data = transform_design(new_data_dict)
        if transformed_design_data is not None:
            upload_to_transformation_s3(transformed_design_data, 'design')

        transformed_location_data = transform_location(new_data_dict)
        if transformed_location_data is not None:
            upload_to_transformation_s3(transformed_location_data, 'location')

        if new_data_dict.get('sales_order'):
            transformed_sales_order_data = facts_table(new_data_dict['sales_order'])
            if transformed_sales_order_data is not None:
                upload_to_transformation_s3(transformed_sales_order_data, 'sales')


        # transformed_date_data = transform_date() # might rely on the transformed staff data.
        
        return {
            "statusCode": 200,
            "body": json.dumps("Transformation Lambda executed successfully"),
        }

    except Exception as e:
        logger.critical(f"Critical error: {str(e)}")
        raise e


# import json
# import sys
# import os

# if os.environ.get("AWS_EXECUTION_ENV") is not None:
#     from upload_to_s3_util_func import upload_tables_to_s3
#     from setup_logger import setup_logger
#     from transform_staff import transform_staff
#     from transform_read_util import ingestion_data_from_s3
# else:
#     from src.transform.setup_logger import setup_logger
#     from src.transform.upload_to_s3_util_func import upload_tables_to_s3
#     from src.transform.transform_staff import transform_staff
#     from src.transform.transform_read_util import ingestion_data_from_s3


# def lambda_handler(event, context):
#     BUCKET_NAME = "smith-morra-transformation-bucket"
#     try:
#         logger = setup_logger("transformation_logger")
#         logger.info("Starting to transform")
#         data_dict = ingestion_data_from_s3()
#         df = transform_staff(data_dict)
#         upload_tables_to_s3(df, table_name="dim_staff", bucket_name=BUCKET_NAME)
#     except Exception as e:
#         raise e
