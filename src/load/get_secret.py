import boto3
from botocore.exceptions import ClientError


def get_secret(secret_name):
    if secret_name not in ["DataSource_PostgresDB_Credentials","DataTarget_PostgresDB_Credentials"]:
        if type(secret_name) == str:
            raise ValueError
        else:
            raise TypeError

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
