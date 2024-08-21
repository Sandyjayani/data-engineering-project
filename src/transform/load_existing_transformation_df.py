import boto3 
from datetime import datetime 
import pandas as pd
from io import BytesIO, StringIO
import os
from re import search

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
else:    
    from src.transform.setup_logger import setup_logger

logger = setup_logger("Load and Combine files logger")

'''
    - It would be a function that load all the Parquet files in the folder (e.g. dim_staff) 
    and combined them in a df and then return the df.
    - It could be used in the functions for dim_staff and dim_counterparty and also the loading stage
'''
def extract_timestamp(filename):
    '''
    extract the timestamp from the fileanme and return the it in datetime 
    '''
    match = search(r'(\d{4}-\d{2}-\d{2})_(\d{2})\.(\d{2})\.(\d{2})\.parquet$', filename)
    if match:
        date_str = match.group(1)
        hour = match.group(2)
        minute = match.group(3)
        second = match.group(4)
        timestamp_str = f'{date_str} {hour}.{minute}.{second}'

        return datetime.strptime(timestamp_str, '%Y-%m-%d %H.%M.%S')
    return None

def load_and_combine_transformed_tables(table_name: str, bucket_type='transform') -> pd.DataFrame:
    '''
    - if bucket_type = 'ingest' , it would read from the ingestion bucket and file type to be csv
    - otherwise, would read from the transformation bucket and file type to be parquet
    - create a s3 client 
    - create an empty dict
    - create a variable storing a list of keys in the table_name folder
    - loop over the list
    -   check if the file endswith the file type
    -   if so get the object from s3 and load as df
    -   append the df to the dict
    - sort the df by the timestamp
    - concate all df in the list into a single df 
    - return the df 
    - if there is nothing in the list, log a warning 
    '''
    if bucket_type == 'ingest':
        BUCKET_NAME =  "smith-morra-ingestion-bucket"
        file_type = 'csv'
    else:
        BUCKET_NAME =  "smith-morra-transformation-bucket"
        file_type = 'parquet'

    PREFIX = f"{table_name}/"

    s3_client = boto3.client('s3')
    df_dict ={}

    s3_response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)

    for content in s3_response.get('Contents', []):
        key = content.get('Key','')
        if key.endswith(file_type):
            logger.info(f"Processing file: {key}")
            try:
                response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
                if file_type == 'csv':
                    df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
                else:
                    df = pd.read_parquet(BytesIO(response['Body'].read()))
                df_dict[key] = df
                logger.info(f"Loaded DataFrame from: {key}")
            except Exception as e:
                logger.error(f"Failed to load DataFrame from {key}. Error: {e}")
                raise e
            
    if df_dict:
        df_list = sorted(df_dict, key=extract_timestamp)
        sorted_df_list = [df_dict[key] for key in df_list]
        combined_df = pd.concat(sorted_df_list, ignore_index=True)
        logger.info(f"Combined tables in {table_name} into a single DataFrame.")
        return combined_df
    else:
        logger.warning(f"No objects found in the bucket with folder name: {table_name}")
        return pd.DataFrame()
        
        
