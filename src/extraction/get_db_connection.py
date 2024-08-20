import boto3
from botocore.exceptions import ClientError
from pg8000.native import Connection, Error
import pandas as pd
import json
import os

if os.environ.get("AWS_EXECUTION_ENV") is not None:
    from get_secret import get_secret
else:
    from src.extraction.get_secret import get_secret


def create_connection(stage_name):
    # Retrieve the secret
    if stage_name == "extraction":
        secret = get_secret("DataSource_PostgresDB_Credentials")
    elif stage_name == "transformation":
        secret = get_secret("DataTarget_PostgresDB_Credentials")
    secret_dict = json.loads(secret)

    # Extract credentials from the secret
    user = secret_dict["user"]
    password = secret_dict["password"]
    host = secret_dict["host"]
    database = secret_dict["database"]
    port = secret_dict["port"]

    # Establish the database connection
    conn = Connection(
        user=user, database=database, host=host, password=password, port=port
    )
    return conn
