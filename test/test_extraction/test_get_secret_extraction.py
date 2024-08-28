from src.extraction.get_secret import get_secret
from moto import mock_aws
import boto3
from botocore.exceptions import ClientError
import pytest
import os
from json import dumps, loads


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEAFULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def secrets_client(aws_creds):
    with mock_aws():
        secrets_client = boto3.client("secretsmanager")

        secret_dict = {
            "user": "test_smith_morra",
            "password": "test_password",
            "host": "test_warehouse.eu-west-2.rds.amazonaws.com",
            "database": "test_postgres",
            "port": 9999,
        }
        secret_json = dumps(secret_dict)

        secrets_client.create_secret(
            Name="DataSource_PostgresDB_Credentials", SecretString=secret_json
        )

        yield secrets_client


def test_returns_secret_as_json_string(secrets_client):
    result = get_secret("DataSource_PostgresDB_Credentials")
    assert type(result) == str


def test_decoded_return_value_is_a_dict(secrets_client):
    result = get_secret("DataSource_PostgresDB_Credentials")
    decoded_result = loads(result)
    assert type(decoded_result) == dict


def test_secret_contains_stored_secret_values(secrets_client):
    secret_dict = {
        "user": "test_smith_morra",
        "password": "test_password",
        "host": "test_warehouse.eu-west-2.rds.amazonaws.com",
        "database": "test_postgres",
        "port": 9999,
    }
    result = get_secret("DataSource_PostgresDB_Credentials")
    decoded_result = loads(result)
    assert decoded_result == secret_dict


def test_secret_raises_value_error_if_passed_non_existent_secret_name_string(
    secrets_client,
):
    with pytest.raises(ValueError):
        assert get_secret("No_Such_credentials")


def test_secret_raises_type_error_if_passed_anything_other_than_string(secrets_client):
    with pytest.raises(TypeError):
        assert get_secret(123)


def test_secret_raises_client_error_if_client_error_occurs(secrets_client):
    with pytest.raises(ClientError):
        assert get_secret("DataTarget_PostgresDB_Credentials")
        # valid input but I didn't put that one in the mock
        # so it won't find it = raise clienterror


# @patch("boto3.session.Session")
# def test_get_secret_configuration(mock_session):
#     # Configure the mock session
#     mock_session_instance = MagicMock()
#     mock_session.return_value = mock_session_instance

#     # Configure the mock client
#     mock_client = MagicMock()
#     mock_session_instance.client.return_value = mock_client

#     # Set up specific return values or behaviors
#     mock_client.get_secret_value.return_value = {
#         "SecretString": '{"key": "mocked_value"}'
#     }

#     result = get_secret("DataSource_PostgresDB_Credentials")

#     mock_session.assert_called_once()

#     assert json.loads(result) == {"key": "mocked_value"}

# # Note from Ren: this test doesn't use the function we're testing, what are we asserting with it?
# @patch("boto3.session.Session")
# def test_get_secret_exception(mock_session):
#     # Configure the mock session
#     mock_session_instance = MagicMock()
#     mock_session.return_value = mock_session_instance

#     # Configure the mock client
#     mock_client = MagicMock()
#     mock_session_instance.client.return_value = mock_client

#     mock_client.get_secret_value.side_effect = ClientError(
#         {"Error": {"Code": "ResourceNotFoundException", "Message": "Secret not found"}},
#         "GetSecretValue",
#     )

#     with pytest.raises(ClientError) as excinfo:
#         mock_client.get_secret_value(SecretId="nonexistent-secret")

#     assert "ResourceNotFoundException" in str(excinfo.value)


# Test requires completion
#
# @patch("test_get_db_connection.create_connection")
# @patch("src.get_db_connection.get_secret")
# def test_create_connection(mock_get_secrets, mock_create_connection):
#     mock_get_secrets.return_value = json.dumps(
#         '{"user": "test_user","password":"test_password","host":"test_host","database":"test_database","port":5432}'
#     )
#     result = create_connection()
#     print(result)
#     # assert mock_create_connection.call_count == 1
#     mock_create_connection.assert_called_with(
#         {
#             "user": "test_user",
#             "password": "test_password",
#             "host": "test_host",
#             "database": "test_database",
#             "port": 5432,
#         }
#     )
