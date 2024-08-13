import pytest
import pandas as pd
import datetime
import boto3
import json
from pg8000.native import Connection
from unittest.mock import Mock
from src.get_table import get_table as gt


def get_secret():
    secret_name = "DataSource_PostgresDB_Credentials"
    region_name = "eu-west-2"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    secret = get_secret_value_response['SecretString']
    return secret


def create_connection():
    # Retrieve the secret
    secret = get_secret()
    secret_dict = json.loads(secret)
    # Extract credentials from the secret
    user = secret_dict['user']
    password = secret_dict['password']
    host = secret_dict['host']
    database = secret_dict['database']
    port = secret_dict['port']
    # Establish the database connection
    conn = Connection(
        user=user,
        database=database,
        host=host,
        password=password,
        port=port
    )
    return conn


@pytest.fixture(scope='function')
def connection():
    connection = Mock()
    currency_columns = [{'name': 'currency_id'}, 
                    {'name': 'currency_code'}, 
                    {'name': 'created_at'},
                    {'name': 'last_updated'}]
    connection.columns = currency_columns
    return connection


@pytest.mark.it('Returns data frame')
def test_returns_data_frame(connection):
    connection.run.return_value = [[1, 'GBP', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)], 
                                   [2, 'USD', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)], 
                                   [3, 'EUR', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]]
    timestamp = datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)
    output = gt('currency', connection, timestamp)
    assert isinstance(output, pd.DataFrame)


@pytest.mark.it('Returns none if query finds no rows with last_updated after timestamp')
def test_returns_none(connection):
    timestamp = datetime.datetime(2024, 11, 3, 14, 20, 49, 962000)
    data = [[1, 'GBP', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)], 
            [2, 'USD', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)], 
            [3, 'EUR', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]]
    def return_result(query, table_name):
        return [row for row in data if row[3] > timestamp]
    connection.run.side_effect = return_result
    output = gt('currency', connection, timestamp)
    assert output == None


@pytest.mark.it('Returns dataframe that only includes rows where last_updated after timestamp')
def test_returns_only_new_rows(connection):
    timestamp = datetime.datetime(2023, 11, 3, 14, 20, 49, 962000)
    data = [[1, 'GBP', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)], 
            [2, 'USD', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)], 
            [3, 'EUR', datetime.datetime(2024, 11, 3, 14, 20, 49, 962000), datetime.datetime(2024, 11, 3, 14, 20, 49, 962000)]]
    def return_result(query, table_name):
        return [row for row in data if row[3] > timestamp]
    connection.run.side_effect = return_result
    output = gt('currency', connection, timestamp)
    last_updated = list(output['last_updated'])
    assert all([time > timestamp for time in last_updated])


@pytest.mark.it('Returns none when connecting to real db if no valid rows')
def test_queries_real_db_with_no_result():
    timestamp = datetime.datetime(2023, 11, 3, 14, 20, 49, 962000)
    conn = create_connection()
    output = gt('currency', conn, timestamp)
    assert output == None


@pytest.mark.it('Returns rows when connecting to real db if valid rows')
def test_queries_real_db_with_result():
    timestamp = datetime.datetime(2021, 11, 3, 14, 20, 49, 962000)
    conn = create_connection()
    output = gt('currency', conn, timestamp)
    last_updated = list(output['last_updated'])
    assert all([time > timestamp for time in last_updated])


