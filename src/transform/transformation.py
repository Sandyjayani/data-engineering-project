import json
import sys
import os

if os.environ.get("AWS_EXECUTION_ENV") is not None:
    from upload_to_s3_util_func import upload_tables_to_s3
    from setup_logger import setup_logger
    from transform_staff import transform_staff
    from transform_read_util import ingestion_data_from_s3
else:
    from src.transform.setup_logger import setup_logger
    from src.transform.upload_to_s3_util_func import upload_tables_to_s3
    from src.transform.transform_staff import transform_staff
    from src.transform.transform_read_util import ingestion_data_from_s3

def lambda_handler(event, context):
    BUCKET_NAME = "smith-morra-transformation-bucket"
    try:
        logger = setup_logger("transformation_logger")
        logger.info("Starting to transform")
        data_dict = ingestion_data_from_s3()
        df = transform_staff(data_dict)
        upload_tables_to_s3(df, table_name="dim_staff", bucket_name=BUCKET_NAME)
    except Exception as e:
        raise e
