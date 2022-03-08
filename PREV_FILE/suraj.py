import boto3
import psycopg2
import os
import redshift_connector
import base64
from logger_sf import *
from db_connection import *
import json
from botocore.exceptions import ClientError
from logging import *
import os

# GLOBAL INITIALIZATION
s3_bucket = []
table_list = []
table_list_1 = []
index_parquet_list = []
split_table_list = []
split_table = []
# LOG FILE SET UP
LOG_FORMAT = '{lineno}  : {name}: {asctime}: {message}'
basicConfig(filename='logfile.log',level=DEBUG, filemode = 'a',style='{',format=LOG_FORMAT)
logger = getLogger('SFHTC')

# AWS SECRETE MANAGER

os.environ['aws_access_key_id'] = 'AKIASC3QUFY6SICETLNP'
os.environ['aws_secret_access_key'] = 'SsC4FkH6zcMfjqpyu1ZgJjGJAoSVu1cRlyGJi0Ps'
secrets = get_secret()
secrets = secrets.replace("\n", "")
secrets = secrets.replace(" ", "")
secrets = json.loads(secrets)
# print(secrets["region_name"])

# UPLOAD LOG-FILES

def upload_log(bucket_name):
    logger.info("Inside upload_log function")
    folder = 'logfile/' + 'logfile.log'
    try:
        s3_client.upload_file('logfile.log', bucket_name, folder)
        logger.info("Log File Uploaded Successfully!!.")
    except Exception as e:
        logger.error("LogFile Uploaded Failed!!.")
        logger.error(e)




if __name__ == '__main__':
    try:
        logger.info("-----AWS S3 Connectivity Intiated-----")
        logger.info("Setting Up S3 client")
        s3_client = boto3.client("s3", region_name='ap-south-1', aws_access_key_id='AKIASC3QUFY6SICETLNP', aws_secret_access_key='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')
        logger.info("Setting Up Os.environ")
        os.environ['aws_access_key_id'] = 'AKIASC3QUFY6SICETLNP'
        os.environ['aws_secret_access_key'] = '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'
        s3 = boto3.resource('s3')

        def upload_log(bucket_name):
            logger.info("Inside upload_log function")
            folder = 'logfile/' + 'logfile.log'
            try:
                s3_client.upload_file('logfile.log', bucket_name, folder)
                logger.info("Log File Uploaded Successfully!!.")
            except Exception as e:
                logger.error("LogFile Uploaded Failed!!.")
                logger.error(e)
        
        s3_bucket = []
        # appending bucket names
        try:
            logger.info('checking for all the buckets')
            for bucket in s3.buckets.all():
                s3_bucket.append(bucket.name)
                logger.info('Appended all the buckets to s3_bucket list')
            #print(s3_bucket)
        except Exception as e:
            logger.error("Unable to fetch S3 bucket")
            logger.error(e)

        try:
            if 'index-bucket-sfs' and 'parquet-bucket-sfs' in s3_bucket:
                logger.info("index-bucket and parquet-bucket found in the s3_bucket list")
                # index_bucket = s3.Bucket('index-bucket-sfs')
                # parquet_bucket1 = s3.Bucket('parquet-bucket-sfs')
                #code for accessing tablename in bucket goes here.
                #copy_command to copy to db, can be modified according to use.
                copy_command = ("COPY table_1 FROM 's3://parquet-bucket-sfs/table_1/userdata8.parquet' IAM_ROLE 'arn:aws:iam::143580737085:role/migrationrole' FORMAT AS PARQUET;")
                print("Db connection started")
                # logger.log("Creating Database Connection")
                secrets = get_secret()
                secrets = secrets.replace("\n", "")
                secrets = secrets.replace(" ", "")
                secrets = json.loads(secrets)
                print(secrets["host"])
                con = get_db_conn(secrets)
                cur = con.cursor()
                print("Db connection established")
                # logger.log("Copying started for parquet file to redshift")
                print("copy started")
                cur.execute(copy_command)
                con.commit()
                # logger.log("Copying completed to redshift for file")
            else:
                logger.error("Index File Not Present-----!!")

        except Exception as e:
            logger.error("Something went wrong while processing index and parquet bucket----->")

    except Exception as e:
        logger.critical("Main Execution Stopped----->")

    finally:
        # upload_log('testbucketsuraj')
        
        logger.info("Job Executed------------------------------------------------------------------------------------------------------------------")