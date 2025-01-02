#
# Company : Pure Storage 
# Date : Nov 2024
# Author : L. Boschet
#
# Purpose : Create a new bucket in a given account
# 
# Need to setup the 2 following environment variables :
#    - $ARRAY_NAME should contain the FQDN of the Flashblade or the IP address
#    - $USER_API_TOKEN should contain the user API Token that needs to create the new S3 bucket
#
# Flashblade REST API Documentation
# https://purity-fb.readthedocs.io/en/latest/
#

import os
import urllib3
from dotenv import load_dotenv
import boto3

# Default variables
ARRAY_NAME = "flashblade.mydomain.com"
USER_API_TOKEN = "T-eece21f3-66cb-4927-xxxx-33560585ced4"
USER_API_NAME="storage-admin-user"
S3_ACCOUNT_NAME = "test-account"
S3_USER_NAME = "test-user"
S3_BUCKET_NAME = "test-bucket"

def init_s3_client():
    try:
        s3_endpoint = f"http://{os.getenv('AWS_ENDPOINT_URL')}"
        access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        region = os.getenv('AWS_DEFAULT_REGION')

        if not isinstance(s3_endpoint, str):
            raise ValueError("AWS_ENDPOINT_URL is not a string")
        if not isinstance(access_key_id, str):
            raise ValueError("AWS_ACCESS_KEY_ID is not a string")
        if not isinstance(secret_access_key, str):
            raise ValueError("AWS_SECRET_ACCESS_KEY is not a string")

        s3_client = boto3.client(
            's3',
            region_name=region, # Region parameter is not used on FlashBlade
            endpoint_url=s3_endpoint,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

        # Test the connection by listing buckets
        s3_client.list_buckets()
        #print("\t- S3 connection successful.")
        
        return s3_client

    except (NoCredentialsError, PartialCredentialsError):
        print("Error: AWS credentials are not set or incomplete.")
    except EndpointConnectionError:
        print("Error: Unable to connect to the specified endpoint URL.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def list_accounts(s3_client):
    # List all accounts (buckets in this case)
    response = s3_client.list_buckets()
    accounts = [bucket['Name'] for bucket in response['Buckets']]
    return accounts

def select_account(s3_client):
    accounts = list_accounts(s3_client)
    print("*** Choose the Account: ")
    for i, account in enumerate(accounts):
        print(f'\t{i}) {account}')
    
    selected_account = -1
    while selected_account < 0 or selected_account >= len(accounts):
        try:
            selected_account = int(input("Select: "))
        except ValueError:
            print("Invalid Number")
    
    return accounts[selected_account]

def list_users(s3_client, account):
    # List all users (for simplicity, we assume users are represented by IAM users)
    iam_client = boto3.client('iam')
    response = iam_client.list_users()
    users = [user['UserName'] for user in response['Users']]
    return users

def select_user(s3_client, account):
    users = list_users(s3_client, account)
    print("*** Choose the Object User to create a new bucket: ")
    for i, user in enumerate(users):
        print(f'\t{i}) {user}')
    
    selected_user = -1
    while selected_user < 0 or selected_user >= len(users):
        try:
            selected_user = int(input("Select: "))
        except ValueError:
            print("Invalid Number")
    
    return users[selected_user]

def create_new_bucket(s3_client, account, user):
    pattern = "^[a-zA-Z0-9.-_]{1,255}$"
    bucket_name = input("Please enter new bucket name: ")

    while not re.fullmatch(pattern, bucket_name):
        print("Invalid character in S3 bucket name, Try again")
        bucket_name = input("Please enter new bucket name: ")

    response = s3_client.create_bucket(Bucket=bucket_name)
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print(f'New Bucket {bucket_name} created')
        return bucket_name
    else:
        print(f'Error when trying to create new bucket {bucket_name} for account {account}')

#
# MAIN 
#    - Connect to the Flashblade with API Token (user must be in AD and Token created and not expired)
#    - List accounts and chosse account
#    - List users and choose user
#    - Input bucket name and IAM access 
#
if __name__=='__main__':

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Load environment variables form file .env
    load_dotenv(verbose=True)

    print("Connexion to S3 endpoint...")

    #print(f"\t- AWS_ENDPOINT_URL: {os.getenv('AWS_ENDPOINT_URL')}")
    #print(f"\t- AWS_ACCESS_KEY_ID: {os.getenv('AWS_ACCESS_KEY_ID')}")

    # Initialize the S3 client with credentials from .env
    s3_client = init_s3_client()

    # List buckets
    account = select_account(s3_client)
    print(f'Account selected: {account}')

    # List and select user
    user = select_user(s3_client, account)
    print(f'User selected: {user}')

    # Input correct bucket name and create new S3 Bucket
    bucket = create_new_bucket(s3_client, account, user)
