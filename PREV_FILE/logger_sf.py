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
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = os.path.join(BASE_DIR, "PREV_FILE\logfile.log")


LOG_FORMAT = '{lineno}  : {name}: {asctime}: {message}'
basicConfig(filename='logfile.log',level=DEBUG, filemode = 'a',style='{',format=LOG_FORMAT)
logger = getLogger('SFHTC')


def upload_log(bucket_name):
    logger.info("Inside upload_log function")
    folder = 'logfile/' + 'logfile.log'
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(LOG_DIR, bucket_name, folder)
        logger.info("Log File Uploaded Successfully!!.")
    except Exception as e:
        logger.error("LogFile Uploaded Failed!!.")
        logger.error(e)