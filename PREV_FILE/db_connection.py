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

LOG_FORMAT = '{lineno}  : {name}: {asctime}: {message}'
basicConfig(filename='logfile.log',level=DEBUG, filemode = 'a',style='{',format=LOG_FORMAT)
logger = getLogger('SFHTC')

logger.info("-------------------------------------------------------Job Started---------------------------------------------------------------------")

#-----------------------------------------------------------------------------------------------------------------------
# DB connection
class DBConnection:
    def __init__(self): ## These values should be read from AWS Secret Manager - In Secret Manager Password and userid should be encrypted form.
        self.host = "redshift-cluster-1.c04kzwicvscs.ap-south-1.redshift.amazonaws.com:5439/dev"
        self.port = "5439"
        self.dbname = "dev"
        self.user = "migration"
        self.password = "Pratik@2000"

    def get_db_connection(self):
        try:
            db_conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.dbname, user=self.user,password=self.password)
            logger.info("DB Connection Successful")
            db_conn.autocommit = True
            return db_conn
        except Exception as e:
            #print("Exception in DB connection", e)
            logger.critical("Exception in DB Connection")
            logger.error(e)

def get_db_conn():
    logger.info("Inside get_db_conn")
    postgres_db = DBConnection()
    db_conn = postgres_db.get_db_connection()

    return db_conn