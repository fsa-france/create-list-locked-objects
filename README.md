# S3 Bucket Object Lock Analysis

This project allows you to create random locked objects (with an object_lock retention) and analyze S3 buckets for objects with retention policies using an S3-compatible storage solution (e.g., Pure Storage FlashBlade). It identifies buckets with Object Lock enabled and lists objects with active retention locks, along with their metadata.

## Prerequisites

- Python 3.6 or newer.
- Access to an S3-compatible storage endpoint.
- Valid S3 credentials.

## Setup

### 1. Create and Configure the `.env` File

Copy the .env_template file and rename it as '.env' on the app root directory 
    % cp .env_template .env

Update information with your credentials and endpoint

    # Credentials and information for S3 compatible client connection (Pure Storage Flashblade)
    # Notes 
    #    - Region is not used
    #    - For Flashblade, use on of the Data VIP endpoint for S3
    AWS_ACCESS_KEY_ID=<YOUR FLASHBLADE ACCESS KEY>
    AWS_SECRET_ACCESS_KEY=<YOUR FLASHBLADE SECRET KEY>
    AWS_ENDPOINT_URL=<YOUR ENDPOINT VIP> or <YOUR END¨POINT FQDN> 
    AWS_DEFAULT_REGION=eu-west-1 

### 2. Install required package in your python env

We recommend to use a python venv or a conda env

Activate your venv:

    % source my-venv/bin/activate

Install packages

    % pip install -r requirements.txt

## Usage

### Create object locked files

Start the object creation python3 script that will ask which bucket you want to use:

    % python3 s3-create-locked-objects-parallel.py
    Connexion to S3 endpoint...
        - AWS_ENDPOINT_URL: 192.168.xx.yy
        - AWS_ACCESS_KEY_ID: XXXXXXXXXXXX
        - S3 connection successful.
    All Existing buckets list:
        - bucket1-lock
        - bucket2-lock
        - bucket3
    All buckets with ObjectLock Enabled list:
        - bucket1-lock
        - bucket2-lock

    List of buckets to create locked objects:
        1. bucket1-lock
        2. bucket2-lock

    Enter the number of the bucket with ObjectLock you want to use: 1
    Specify rétention date using ISO 8601 format (Example: 2024-12-31T00:00:00Z): 2024-12-28T00:00:00Z
    Enter the number of objects to create: 100
    Enter object prefix to be used for this serie of locked objects: random_objs_locked_
    Enter the initial index to use for the objects: 1

Output :
    
    100 locked objects successfully created with prefix 'random_objs_locked_' in bucket 'lbo-bucket-lock'.

### Get statistics for locked objects in a iven bucket

Start the object_lock statistic python3 script that will ask which bucket you want to analyze:

    % python3 s3-list-bucket-locked-objects.py
    S3 connection successful.
    Existing buckets list:
        - bucket1-lock
        - bucket2-lock
        - bucket3

    Buckets with Object Lock enabled:
        - bucket1-lock
        - bucket2-lock

    List of buckets with locked objects:
        1. bucket1-lock
        2. bucket2-lock

    Enter the number of the bucket you want to analyze: 2
    Selected bucket: bucket2-lock

    Total number of objects:                                  111101
    Total size of objects:                                     10789.71 MB
    Total number of objects with non-expired lock:            111101
    Total size of objects with lock:                           10789.71 MB

    Object Statistics sorted by remaining retention days:
        RemainingDays  Count        Size
    0              -5    100    11351040
    1              -4  20000  2057929728
    2              -2  10000  1014203392
    3               6  10000  1025987584
    4              11  30901  3188153344
    5              18   1100     1126400
    6              23  20000  2067164160
    7              24   7000   714650624
    8              27  12000  1233259520


