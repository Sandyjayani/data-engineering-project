import boto3
from botocore.exceptions import ClientError
from pg8000.native import Connection, Error
import pandas as pd
import json


def get_secret():
    secret_name = "DataSource_PostgresDB_Credentials"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]
    return secret


def create_connection():
    # Retrieve the secret
    secret = get_secret()
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
