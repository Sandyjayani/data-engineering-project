import os
import json

if os.environ.get("AWS_EXECUTION_ENV"):
    from upload_to_s3_util_func import upload_tables_to_s3
    from setup_logger import setup_logger
    from dim_currency import transform_dim_currency
    from dim_date import dim_date
    from facts_table import facts_table
    from transform_dim_design import transform_dim_design
    from transform_dim_location import transform_dim_location
    from transform_read_util import ingestion_data_from_s3 as load_ingestion_data
else:
    from src.transform.upload_to_s3_util_func import upload_tables_to_s3
    from src.transform.setup_logger import setup_logger
    from src.transform.dim_currency import transform_dim_currency
    from src.transform.dim_date import dim_date
    from src.transform.facts_table import facts_table
    from src.transform.transform_dim_design import transform_dim_design
    from src.transform.transform_dim_location import transform_dim_location





def lambda_handler(event, context):
    """
    The function is triggered within a step function
    after the successful execution of the extraction lambda.


    """
    try:
        logger = setup_logger("transformation_logger")
        logger.info('Starting transformation process')
        new_data_dict = load_ingestion_data()
        # transformed_counterparty_data = transform_counterparty(new_data_dict)
        transformed_currency_data = transform_dim_currency(new_data_dict)
        transformed_design_data = transform_dim_design(new_data_dict)
        transformed_location_data = transform_dim_location(new_data_dict)
        transformed_sales_order_data = facts_table(new_data_dict['sales_order'])
        transformed_date_data = dim_date() # might rely on the transformed staff data.
        
        return {
            "statusCode": 200,
            "body": json.dumps("Transformation Lambda executed successfully"),
        }
        
    except Exception as e:
        logger.critical(f"Critical error: {e}")
        raise e