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
        - S3 connection successful.
    All Existing buckets list:
        - bucket1-lock
        - bucket2-lock
        - bucket3
        - bucket4

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
    All Existing buckets list:
        - bucket1-lock
        - bucket2-lock
        - bucket3
        - bucket4

    All buckets with ObjectLock Enabled list:
        1. bucket1-lock
        2. bucket2-lock

    Enter the number of the bucket you want to analyze: 2
    Selected bucket: bucket2-lock

    Object Statistics sorted by remaining retention days:
    Total number of objects:                                       1380
    Total size of objects:                                       115.70 MB
    Total number of objects with non-expired lock:                 1380
    Total size of objects with lock:                             115.70 MB

    Object Statistics sorted by remaining retention days:
    RemainingDays  Count      Size
                0    180  92788736
                2    500    512000
                6    150    153600
               16    200    204800
               22     50  27356160
               26    300    307200
            Total   1380 121322496

### Performances

Scripts are written in standard python3 using AWS boto3 standard module.


To list objects, pagination must be used with s3.get_paginator('list_objects_v2') because the maximum number 
of objects that can be returned in a single page is 1,000. 
This limit is set by AWS and cannot be increased. 
If there are more than 1,000 objects in the bucket, the paginator will handle fetching additional pages of results.
This can induce some time to process buckest with a large number of objects.

Creation of 500.000 random objects of size between 1KB and 1024KB :

    % time python3 s3-create-locked-objects-parallel.py  
    11303,08s user 1047,34s system 24% cpu 14:16:46,16 total ===> about 3 hours

Here is the results to list 500.000 random size locked objects in a same bucket:

    % time python3 s3-list-bucket-locked-objects.py
    625,49s user 48,01s system 47% cpu 23:45,67 total ===> about 11 minutes


