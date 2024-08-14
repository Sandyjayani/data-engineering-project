import boto3
import pandas as pd
from io import StringIO


def get_timestamp(table_name: str) -> str:
    '''Function takes name of table and searches for latest timestamp
    in csv file for that table. If no timestamp CSV exist, it will return
    timestamp of 0001-01-01_01-01
    
    parameters:
        - table_name: str
        
    return value:
        - timestamp: str (e.g., '2024-08-14_14-09') '''
    bucket_name = 'test_bucket'
    key = f'{table_name}/timestamps.csv'
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)['Body']
        timestamp_df = pd.read_csv(StringIO(response.read().decode('utf-8')))
        timestamp = timestamp_df['Date'].max()
        if not timestamp:
            return '0001-01-01_01-01'
        return timestamp
    except:
        return '0001-01-01_01-01'
