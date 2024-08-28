import pytest
import boto3
from botocore.exceptions import ClientError
from moto import mock_aws
from src.load.get_secret import get_secret
from unittest.mock import patch

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture(scope="function")
def secretsmanager_client(aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")

@pytest.mark.it("should retrieve a valid secret")
def test_get_valid_secret(secretsmanager_client):
    secret_name = "DataSource_PostgresDB_Credentials"
    secret_value = '{"username": "test", "password": "test123"}'
    secretsmanager_client.create_secret(Name=secret_name, SecretString=secret_value)

    result = get_secret(secret_name)
    assert result == secret_value

@pytest.mark.it("should retrieve another valid secret")
def test_get_another_valid_secret(secretsmanager_client):
    secret_name = "DataTarget_PostgresDB_Credentials"
    secret_value = '{"username": "target", "password": "target123"}'
    secretsmanager_client.create_secret(Name=secret_name, SecretString=secret_value)

    result = get_secret(secret_name)
    assert result == secret_value

@pytest.mark.it("should raise ValueError for invalid secret name")
def test_get_secret_invalid_name():
    with pytest.raises(ValueError):
        get_secret("InvalidSecretName")

@pytest.mark.it("should raise TypeError for non-string secret name")
def test_get_secret_non_string_name():
    with pytest.raises(TypeError):
        get_secret(123)

@pytest.mark.it("should raise ClientError when secret doesn't exist")
def test_get_nonexistent_secret(secretsmanager_client):
    with pytest.raises(ClientError):
        get_secret("DataSource_PostgresDB_Credentials")

@pytest.mark.it("should handle ClientError from AWS")
def test_handle_aws_client_error():
    with patch('boto3.session.Session.client') as mock_client:
        mock_client.return_value.get_secret_value.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Secret not found"}},
            "GetSecretValue"
        )

        with pytest.raises(ClientError):
            get_secret("DataSource_PostgresDB_Credentials")
