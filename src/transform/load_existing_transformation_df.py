import boto3 
from datetime import datetime 
import pandas as pd
from io import BytesIO
from re import search

'''
    - It would be a function that load all the Parquet files in the folder (e.g. dim_staff) 
    and combined them in a df and then return the df.
    - It could be used in the functions for dim_staff and dim_counterparty and also the loading stage
'''
def extract_timestamp(filename):
    match = search(r'(\d{4}-\d{2}-\d{2})_(\d{2})\.(\d{2})\.(\d{2})\.parquet$', filename)
    if match:
        date_str = match.group(1)
        hour = match.group(2)
        minute = match.group(3)
        second = match.group(4)
        timestamp_str = f'{date_str} {hour}.{minute}.{second}'

        return datetime.strptime(timestamp_str, '%Y-%m-%d %H.%M.%S')
    return None

def load_and_combine_transformed_tables(table_name: str) -> pd.DataFrame:
    '''
    - create a s3 client 
    - create an empty list
    - create a variable storing a list of keys in the table_name folder
    - loop over the list
    -   check if the file endswith 'parquet'
    -   if so get the object from s3 and load as df
    -   append the df to the list
    - sort the df by the timestamp
    - concate all df in the list into a single df 
    - return the df 
    - if there is nothing in the list, log a warning 
    '''
    BUCKET_NAME =  "smith-morra-transformation-bucket"

    s3_client = boto3.client('s3')
    df_dict ={}

    s3_response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)

    for content in s3_response.get('Contents', []):
        key = content.get('Key','')
        if key.endswith('.parquet'):
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
            df = pd.read_parquet(BytesIO(response['Body'].read()))
            df_dict[key] = df
            
    if df_dict:
        df_list = sorted(df_dict, key=extract_timestamp)
        sorted_df_list = [df_dict[key] for key in df_list]
        combined_df = pd.concat(sorted_df_list, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame
