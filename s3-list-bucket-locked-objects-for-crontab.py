#
# Company : Pure Storage 
# Date : Nov 2024
# Author : L. Boschet
#
# Purpose : List locked objects (compliance mode) in a given bucket to get some statistics
#    Time, RemainingDays, LockMode, Count, Size
#
# Options:
#   --bucket <bucketname>: specify the bucket to inspect (must have credentials)
#
#   --csv : output result in .csv format for further analysis
#
import warnings
import boto3
import os
import re
import argparse
from datetime import datetime, timezone
from dotenv import load_dotenv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, EndpointConnectionError, ClientError

# Suppress specific warning about Python deprecation
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module="boto3.compat",
)

# Initialize the S3 client with credentials from .env
def init_s3_client():
    try:
        s3_client = boto3.client(
            's3',
            region_name=os.getenv('AWS_DEFAULT_REGION'),
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

# Validate bucket name based on S3 bucket naming rules
def is_valid_bucket_name(bucket_name):
    pattern = r"^(?![0-9]+\.)[a-z0-9.-]{3,63}$"
    return bool(re.match(pattern, bucket_name)) and not ".." in bucket_name

# Check if the bucket exists and if Object Lock is enabled
def check_bucket_exists_and_lock_enabled(s3_client, bucket_name):
    try:
        # Check if the bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        # Check if Object Lock is enabled for the bucket
        lock_config = s3_client.get_object_lock_configuration(Bucket=bucket_name)
        if 'ObjectLockConfiguration' not in lock_config:
            print(f"Error: Bucket '{bucket_name}' does not have Object Lock enabled.")
            return False
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Error: Bucket '{bucket_name}' does not exist.")
        else:
            print(f"Error: Unable to access the bucket '{bucket_name}': {e.response['Error']['Message']}")
        return False

# Function to calculate the remaining retention from a given date
def get_retention_remaining(retention_date):
    now = datetime.now(timezone.utc)
    remaining = retention_date - now
    return max(remaining.days, 0)  # Ensure no negative values

# Function to process each page of objects
def process_page(page, bucket_name, s3):
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

                    # Fetch the mode of lock
                    lock_mode = object_lock_info['Retention'].get('Mode', 'Unknown')

                    locked_objects.append({
                        'Key': obj['Key'],
                        'RetentionDate': retention_date,
                        'RemainingDays': remaining_days,
                        'LockMode': lock_mode,
                        'Size': obj['Size']
                    })
                    locked_objects_size_bytes += obj['Size']
            except s3.exceptions.ClientError as e:
                # Handle non-locked objects gracefully
                if e.response['Error']['Code'] == 'ObjectLockConfigurationNotFoundError':
                    continue  # Skip objects without Object Lock configuration
                else:
                    # Re-raise other unexpected errors
                    raise

    return locked_objects, total_objects, total_size_bytes, locked_objects_size_bytes

# Main function
def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Analyze S3 bucket objects with retention policies.")
    parser.add_argument('--bucket', required=True, help="Name of the S3 bucket to analyze.")
    parser.add_argument('--csv', action='store_true', help="Output results as CSV to standard output.")

    args = parser.parse_args()

    bucket_name = args.bucket
    output_as_csv = args.csv

    # Validate bucket name
    if not is_valid_bucket_name(bucket_name):
        print(f"Error: '{bucket_name}' is not a valid S3 bucket name.")
        exit(1)

    # Load environment variables from .env
    load_dotenv(verbose=True)

    # Initialize S3 client
    s3_client = init_s3_client()

    # Check if the bucket exists and Object Lock is enabled
    if not check_bucket_exists_and_lock_enabled(s3_client, bucket_name):
        exit(1)

    # List objects in the specified bucket with pagination
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name)

    # Use ThreadPoolExecutor to process pages concurrently
    locked_objects = []
    total_objects = 0
    total_size_bytes = 0
    locked_objects_size_bytes = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_page = {executor.submit(process_page, page, bucket_name, s3_client): page for page in page_iterator}
        for future in as_completed(future_to_page):
            page_locked_objects, page_total_objects, page_total_size_bytes, page_locked_objects_size_bytes = future.result()
            locked_objects.extend(page_locked_objects)
            total_objects += page_total_objects
            total_size_bytes += page_total_size_bytes
            locked_objects_size_bytes += page_locked_objects_size_bytes

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Create a DataFrame for locked objects
    if locked_objects:
        df = pd.DataFrame(locked_objects)

        # Sort the DataFrame by RemainingDays
        df = df.sort_values(by='RemainingDays')

        # Group by remaining retention days and LockMode
        grouped_df = df.groupby(['RemainingDays', 'LockMode']).agg({
            'Key': 'count',
            'Size': 'sum'
        }).reset_index().rename(columns={'Key': 'Count'})

        # Add the timestamp to the grouped DataFrame
        grouped_df['Time'] = current_time

        # Reorder columns to match the desired format
        grouped_df = grouped_df[['Time', 'RemainingDays', 'LockMode', 'Count', 'Size']]

        # Exclude the TOTAL row from CSV output
        grouped_df_csv = grouped_df[grouped_df['RemainingDays'] != 'Total']

        if output_as_csv:
            # Output as CSV to standard output
            csv_output = grouped_df_csv.to_csv(index=False)
            print(csv_output)
        else:
            # Standard output format
            total_size_mb = total_size_bytes / (1024 * 1024)
            locked_objects_size_mb = locked_objects_size_bytes / (1024 * 1024)

            print(f"\nCurrent Date and Time: {current_time}")
            print(f"\nTotal number of objects:                       {total_objects:>20}")
            print(f"Total size of objects:                         {total_size_mb:>20.2f} MB")
            print(f"Total number of objects with non-expired lock: {len(locked_objects):>20}")
            print(f"Total size of objects with lock:               {locked_objects_size_mb:>20.2f} MB")

            print(f"\nObject Statistics grouped by remaining retention days and lock mode:")
            print(grouped_df.to_string(index=False))
    else:
        if not output_as_csv:
            print(f"\nCurrent Date and Time: {current_time}")
            print("\nNo locked objects found in the bucket.")

if __name__ == "__main__":
    main()