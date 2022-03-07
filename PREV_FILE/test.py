import psycopg2
import os
import redshift_connector
import base64
from logger_sf import *
from db_connection import *
import json
from botocore.exceptions import ClientError
from logging import *

# os.environ['aws_access_key_id'] = 'AKIASC3QUFY6SICETLNP'
# os.environ['aws_secret_access_key'] = 'SsC4FkH6zcMfjqpyu1ZgJjGJAoSVu1cRlyGJi0Ps'
# s3 = boto3.resource('s3')

# secrets = get_secret()
# secrets = secrets.replace("\n", "")
# secrets = secrets.replace(" ", "")
# secrets = json.loads(secrets)

# s3_bucket = []
# table_list = []
# index_parquet_list = []
# split_table_list = []
# # appending bucket names
# try:
#     logger.info('checking for all the buckets')
#     for bucket in s3.buckets.all():
#         s3_bucket.append(bucket.name)
#         logger.info('Appended all the buckets to s3_bucket list')
#     print(s3_bucket)
# except Exception as e:
#     logger.error("Unable to fetch S3 bucket")
#     logger.error(e)


# Read the index.txt file from testbucketsuraj bucket

