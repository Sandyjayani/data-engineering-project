import os
import json

if os.environ.get("AWS_EXECUTION_ENV"):
    from upload_to_transformation_s3 import upload_to_transformation_s3
    from setup_logger import setup_logger
    from dim_currency import transform_dim_currency
    from dim_date import dim_date
    from transform.facts_sales_order import facts_table
    from transform.dim_design import transform_dim_design
    from transform.dim_location import transform_dim_location
    from transform.load_ingested_tables import ingestion_data_from_s3 as load_ingestion_data
else:
    from src.transform.upload_to_transformation_s3 import upload_to_transformation_s3
    from src.transform.setup_logger import setup_logger
    from src.transform.dim_currency import transform_dim_currency
    from src.transform.dim_date import dim_date
    from transform.facts_sales_order import facts_table
    from transform.dim_design import transform_dim_design
    from transform.dim_location import transform_dim_location
    from transform.load_ingested_tables import ingestion_data_from_s3 as load_ingestion_data


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
        new_data_dict = load_ingestion_data()

        # passes dict to transform_currency, if there is new currency data
        # it will be passed to the upload_to_transformation_s3 
        transformed_currency_data = transform_dim_currency(new_data_dict)
        if transformed_currency_data:
            upload_to_transformation_s3(transformed_currency_data, 'currency')

        # transformed_counterparty_data = transform_counterparty(new_data_dict)
        # if transformed_currency_data:
        #     upload_to_transformation_s3(transformed_currency_data, 'counterparty')
        
        transformed_design_data = transform_dim_design(new_data_dict)
        if transformed_design_data:
            upload_to_transformation_s3(transformed_design_data, 'design')

        transformed_location_data = transform_dim_location(new_data_dict)
        if transformed_location_data:
            upload_to_transformation_s3(transformed_location_data, 'location')

        transformed_sales_order_data = facts_table(new_data_dict)
        if transformed_sales_order_data:
            upload_to_transformation_s3(transformed_sales_order_data, 'sales')


        # transformed_date_data = dim_date() # might rely on the transformed staff data.
        





        return {
            "statusCode": 200,
            "body": json.dumps("Transformation Lambda executed successfully"),
        }

    except Exception as e:
        logger.critical(f"Critical error: {str(e)}")
        raise e
