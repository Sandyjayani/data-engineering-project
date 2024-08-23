import os
import json
import pandas as pd

if os.environ.get("AWS_EXECUTION_ENV"):
    from upload_to_transformation_s3 import upload_to_transformation_s3
    from setup_logger import setup_logger
    from dim_currency import transform_currency
    from dim_date import transform_date
    from facts_sales_order import transform_sales_order
    from dim_design import transform_design
    from dim_staff import transform_staff
    from dim_location import transform_location
    from load_ingested_tables import load_ingested_tables
else:
    from src.transform.upload_to_transformation_s3 import upload_to_transformation_s3
    from src.transform.setup_logger import setup_logger
    from src.transform.dim_currency import transform_currency
    from src.transform.dim_date import transform_date
    from src.transform.facts_sales_order import transform_sales_order
    from src.transform.dim_design import transform_design
    from src.transform.dim_location import transform_location
    from src.transform.dim_staff import transform_staff
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
    - catches any exceptions raised during execution, logs them and raises
        the exception again to be handled by the step function.


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

        if new_data_dict.get('sales_order') is not None:
            transformed_sales_order_data = transform_sales_order(new_data_dict['sales_order'])
            if transformed_sales_order_data is not None:
                upload_to_transformation_s3(transformed_sales_order_data, 'sales_order')

        transformed_staff_data = transform_staff(new_data_dict)
        if transformed_staff_data is not None:
            upload_to_transformation_s3(transformed_staff_data, 'staff')
        
        # transformed_date_data = transform_date() # might rely on the transformed staff data.
        
        logger.info('Transformation process complete')        
        
        return {
            "statusCode": 200,
            "body": json.dumps("Transformation Lambda executed successfully"),
        }

    except Exception as e:
        logger.critical(f"Critical error: {str(e)}")
        raise e
