from src.util_functions.get_db_connection import create_connection
from src.util_functions.get_table import get_table
from src.util_functions.upload_to_s3_util_func import upload_tables_to_s3
from requests import Response

def lambda_handler(event, context):
    

    # event is caused by scheduler/step function telling it to run
        # what format is that? will we even use that to begin with?
    
    # try:

        # create a connection using create_connection util function
            # doesn't take arguments
            # returns connection

        # have a list of all table names
            # is there a way to get these easily or are we just typing them out?
            # either way I wanna hard code them

        # get timestamp using util (to be added)
            # outputs last timestamp

        # iterate through table names, for each table:

            # get table data using get_table:
                # takes table_name, connection, timestamp as arguments
                    # table name from loop variable
                    # connection we just made with connection util
                    # timestamp we just made with timestamp util
                # returns data frame

            # upload to s3 bucket
                # takes dataframe, table_name, bucket_name
                    # dataframe from get_table util
                    # table name from loop variable
                    # bucket name is smith-morra-ingestion-bucket (available in terraform)
                # returns string to confirm that upload was successful or raises error

                # 🔴 do we need this func to get time stamp 
                # when we already need timestamp before, 
                # and could pass the same one in?
                # -> ask for refactor
    
    # except:

        # if an error is raised at any point during this
        # we allow it to be raised in order to handle it in the step function
        # -> do we need an except block then?
    
    # finally:

        # we close our connection

    # 🔴 where do we do our logging?
    # I would like for it to be consistent, ie:
        # if we log in each util -> log in each util
        # if we log only in lambda handler -> only log in lambda handler
        # if we log in both -> make sure all aspects are logging
        
    try:    
        conn = create_connection()
        table_name = 'table name'
        bucket_name = 'smith-morra-ingestion-bucket'
        table_data = get_table(table_name,conn,'timestamp')
        upload_tables_to_s3(table_data,table_name,bucket_name)
        response_message = Response() 
        response_message.status_code = 201
        # response_message.text = 'new data was successfully uploaded'
        return response_message
    finally:
        if 'conn' in locals():
            conn.close()
    