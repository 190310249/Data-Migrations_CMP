import boto3
import psycopg2
import os
from logging import *


LOG_FORMAT = '{lineno}  : {name}: {asctime}: {message}'
basicConfig(filename='logfile.log',level=DEBUG, filemode = 'a',style='{',format=LOG_FORMAT)
logger = getLogger('SFHTC')


def upload_log(bucket_name):
    logger.info("Inside upload_log function")
    folder = 'logfile/' + 'logfile.log'
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(r'C:\\Users\\M.PRATIK KUMAR\\OneDrive\\Desktop\\Data-Migrations_CMP\\logfile.log', bucket_name, folder)
        logger.info("Log File Uploaded Successfully!!.")
    except Exception as e:
        logger.error("LogFile Uploaded Failed!!.")
        logger.error(e)