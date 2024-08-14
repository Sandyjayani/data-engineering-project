from src.get_timestamp import get_timestamp as gt
import pytest
from moto import mock_aws
import os
import boto3


@pytest.fixture(scope='function')
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEAFULT_REGION"] = "eu-west-2"


@pytest.fixture(scope='function')
def s3_client(aws_creds):
    with mock_aws():
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket='test_bucket', 
                                CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
    })
        yield s3_client


@pytest.mark.it('Test returns timestamp string')
def test_returns_string(s3_client):
    table_name = 'test_table'
    with open('test/test_timestamps.csv', 'r', encoding='utf-8') as f:
        s3_client.put_object(
                        Bucket='test_bucket', Key=f'{table_name}/timestamps.csv',
                        Body=f.read())
    assert gt(table_name) == '2024-08-14_14-09'


@pytest.mark.it('Returns 0 AD if no timestamps')
def test_no_timestamps(s3_client):
    table_name = 'test_table'
    assert gt(table_name) == '0001-01-01_01-01'


