import boto3
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, EndpointConnectionError

def init_s3_client():
    try:
        s3_client = boto3.client(
            's3',
            region_name=os.getenv('AWS_DEFAULT_REGION'),  # Region parameter is not used on FlashBlade
            endpoint_url=f"http://{os.getenv('AWS_ENDPOINT_URL')}",
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        )
        
        # Test the connection by listing buckets
        s3_client.list_buckets()
        print("S3 connection successful.")
        
        return s3_client
    except (NoCredentialsError, PartialCredentialsError):
        print("Error: AWS credentials are not set or incomplete.")
    except EndpointConnectionError:
        print("Error: Unable to connect to the specified endpoint URL.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Function to calculate the remaining retention from a given date
def get_retention_remaining(retention_date):
    now = datetime.now(timezone.utc)
    remaining = retention_date - now
    return remaining.days

# Function to process each page of objects
def process_page(page, bucket_name):
    locked_objects = []
    total_objects = 0
    total_size_bytes = 0
    locked_objects_size_bytes = 0

    if 'Contents' in page:
        for obj in page['Contents']:
            total_objects += 1
            total_size_bytes += obj['Size']
            
            # Get the object's lock information
            try:
                object_lock_info = s3.get_object_retention(Bucket=bucket_name, Key=obj['Key'])
                if 'Retention' in object_lock_info:
                    retention_date = object_lock_info['Retention']['RetainUntilDate']
                    remaining_days = get_retention_remaining(retention_date)
                    locked_objects.append({
                        'Key': obj['Key'],
                        'RetentionDate': retention_date,
                        'RemainingDays': remaining_days,
                        'Size': obj['Size']
                    })
                    locked_objects_size_bytes += obj['Size']
            except s3.exceptions.ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchObjectLockConfiguration':
                    raise

    return locked_objects, total_objects, total_size_bytes, locked_objects_size_bytes

# Load environment variables form file .env
load_dotenv(verbose=True)

# Initialize the S3 client with credentials from .env
s3 = init_s3_client()

# List all buckets for this user
buckets_response = s3.list_buckets()
buckets = buckets_response['Buckets']

# Check which buckets have Object Lock enabled
buckets_with_object_lock = []
for bucket in buckets:
    try:
        object_lock_config = s3.get_object_lock_configuration(Bucket=bucket['Name'])
        if 'ObjectLockConfiguration' in object_lock_config:
            buckets_with_object_lock.append(bucket['Name'])
    except s3.exceptions.ClientError as e:
        # If the bucket does not have Object Lock enabled, an error is raised
        if e.response['Error']['Code'] != 'ObjectLockConfigurationNotFoundError':
            raise

# Display the list of buckets and those with Object Lock enabled
print("Existing buckets list:")
for bucket in buckets:
    print(f"\t- {bucket['Name']}")

print("\nBuckets with Object Lock enabled:")
for bucket in buckets_with_object_lock:
    print(f"\t- {bucket}")

# Display the list of buckets with Object Lock for selection
print("\nList of buckets with locked objects:")
if not buckets_with_object_lock:
    print("\tNone")
else:
    for i, bucket_name in enumerate(buckets_with_object_lock):
        print(f"\t {i + 1}. {bucket_name}")

# Prompt the user to select a bucket
bucket_index = int(input("\nEnter the number of the bucket with ObjectLock you want to use: ")) - 1
bucket_name = buckets_with_object_lock[bucket_index]
print(f"Selected bucket: {bucket_name}")

# List objects in the selected bucket with pagination
paginator = s3.get_paginator('list_objects_v2')
page_iterator = paginator.paginate(Bucket=bucket_name)

# Use ThreadPoolExecutor to process pages concurrently
locked_objects = []
total_objects = 0
total_size_bytes = 0
locked_objects_size_bytes = 0

with ThreadPoolExecutor(max_workers=10) as executor:
    future_to_page = {executor.submit(process_page, page, bucket_name): page for page in page_iterator}
    for future in as_completed(future_to_page):
        page_locked_objects, page_total_objects, page_total_size_bytes, page_locked_objects_size_bytes = future.result()
        locked_objects.extend(page_locked_objects)
        total_objects += page_total_objects
        total_size_bytes += page_total_size_bytes
        locked_objects_size_bytes += page_locked_objects_size_bytes

# Create a DataFrame
df = pd.DataFrame(locked_objects)

# Sort the DataFrame by RemainingDays
df = df.sort_values(by='RemainingDays')

# Calculate the total size of objects in MB
total_size_mb = total_size_bytes / (1024 * 1024)
locked_objects_size_mb = locked_objects_size_bytes / (1024 * 1024)

# Group by remaining retention days
grouped_df = df.groupby('RemainingDays').agg({
    'Key': 'count',
    'Size': 'sum'
}).reset_index().rename(columns={'Key': 'Count'})

# Display the results with right-aligned numeric values
print(f"Total number of objects:                       {total_objects:>20}")
print(f"Total size of objects:                         {total_size_mb:>20.2f} MB")
print(f"Total number of objects with non-expired lock: {len(locked_objects):>20}")
print(f"Total size of objects with lock:               {locked_objects_size_mb:>20.2f} MB")

print(f"\nObject Statistics sorted by remaining retention days:")
print(grouped_df)