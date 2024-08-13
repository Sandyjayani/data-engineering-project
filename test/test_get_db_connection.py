from src.get_db_connection import get_secret, create_connection
import os
import boto3
import pytest
from moto import mock_aws
from unittest.mock import patch, MagicMock 
import json
from botocore.exceptions import ClientError


    
@pytest.fixture(scope='function')
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEAFULT_REGION"] = "eu-west-2"


@pytest.fixture(scope='function')
def secrets_client(aws_creds):
    with mock_aws():
        sectrets_client = boto3.client('secretsmanager')
        yield sectrets_client


@patch('boto3.session.Session')
def test_get_secret_configuration(mock_session):
    # Configure the mock session
    mock_session_instance = MagicMock()
    mock_session.return_value = mock_session_instance
    
    # Configure the mock client
    mock_client = MagicMock()
    mock_session_instance.client.return_value = mock_client
    
    # Set up specific return values or behaviors
    mock_client.get_secret_value.return_value = {
        'SecretString': '{"key": "mocked_value"}'
    }

    result = get_secret()

    mock_session.assert_called_once()
    
    assert json.loads(result) == {"key": "mocked_value"}
    
    

@patch('boto3.session.Session')
def test_get_secret_exception(mock_session):
        # Configure the mock session
    mock_session_instance = MagicMock()
    mock_session.return_value = mock_session_instance
    
    # Configure the mock client
    mock_client = MagicMock()
    mock_session_instance.client.return_value = mock_client

    mock_client.get_secret_value.side_effect = ClientError(
            {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Secret not found'}},
            'GetSecretValue'
        )
   
    with pytest.raises(ClientError) as excinfo:
        mock_client.get_secret_value(SecretId='nonexistent-secret')
        
    assert 'ResourceNotFoundException' in str(excinfo.value)