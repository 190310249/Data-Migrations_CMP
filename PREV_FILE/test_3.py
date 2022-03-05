import boto3
import psycopg2
import os
from logging import *
import datetime

LOG_FORMAT = '{lineno}  : {name}: {asctime}: {message}'
basicConfig(filename='logfile.log',level=DEBUG, filemode = 'a',style='{',format=LOG_FORMAT)
logger = getLogger('SFHTC')

logger.info("-------------------------------------------------------Job Started---------------------------------------------------------------------")

#-----------------------------------------------------------------------------------------------------------------------
# DB connection
class DBConnection:
    def __init__(self): ## These values should be read from AWS Secret Manager - In Secret Manager Password and userid should be encrypted form.
        self.host = "xxxx-ap-south-1-db.cckebez2onwv.ap-south-1.rds.amazonaws.com"
        self.port = "5432"
        self.dbname = "webappdb"
        self.user = "postgres"
        self.password = "master123"

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





#-----------------------------------------------------------------------------------------------------------------------
# S3 connectivity

if __name__ == '__main__':

    try:
        logger.info("-----AWS S3 Connectivity Intiated-----")

        logger.info("Setting Up S3 client")
        s3_client = boto3.client("s3", region_name='', aws_access_key_id='', aws_secret_access_key='')

        logger.info("Setting Up Os.environ")
        os.environ['aws_access_key_id'] = 'AKIAWJDXUJQ65MVS2OHT'
        os.environ['aws_secret_access_key'] = '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'

        s3 = boto3.resource('s3')


        def upload_log(bucket_name):
            logger.info("Inside upload_log function")
            folder = 'logfile/' + 'logfile.log'
            try:
                s3_client.upload_file('C:\\Users\\sumit\\PycharmProjects\\mysample\\logfile.log', bucket_name, folder)
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
        #s3 = boto3.resource('s3')

        try:
            if 'index-bucket-sf' and 'parquet-bucket-sf' in s3_bucket:
                logger.info("index-bucket and parquet-bucket found in the s3_bucket list")
                index_bucket = s3.Bucket('index-bucket-sf')
                parquet_bucket1 = s3.Bucket('parquet-bucket-sf')
                table_list = []
                index_parquet_list = []
                split_table_list = []

                logger.info("Checking for parquet file and table")
                for obj1 in parquet_bucket1.objects.all():
                    key1 = obj1.key
                    table_list.append(key1)
                    if key1.split('/')[1] not in table_list:
                        split_table_list.append(key1.split('/')[1])
                print("Table List : ",table_list)
                print("split Table List : ", split_table_list)

                logger.info("checking for index file in index bucket")
                for obj in index_bucket.objects.all():
                    key = obj.key
                    print(key)
                    if key=='index.txt':

                        logger.info("index.txt file found in the bucket")
                        print("checking for the validity of the index file if needed")
                        body = obj.get()['Body'].readline()
                        str1 = body.decode('UTF-8')
                        str2 = str1.split(',')

                        # print(str2)
                        for i in str2:
                            l = i.split('|')
                            # print(l)
                            # print(l[0])
                            if l[0].endswith('.parquet'):
                                index_parquet_list.append(l[0])

                        logger.info("Parquet file details of index.txt updated in index_parquet_list")

                        print("Index parquet list : ",index_parquet_list)
                        for i in index_parquet_list:
                            logger.info("checking parquet file")
                            logger.info(i)
                            if i in split_table_list:
                                print(table_list[split_table_list.index(i)])
                                print("Matched: ",i)

                                logger.info("Available in s3 parquet bucket")

                                print("Parquet file validation will happen")
                                copy_command = ("COPY global.ecy_job1 FROM " + "'s3://parquet-bucket-1/" + table_list[split_table_list.index(i)][0] + "{}'".format(l[0]) + "IAM_ROLE 'arn:aws:iam::0123456789:role/sf_poc_redshift_role'" + 'FORMAT AS PARQUET;')

                                print("processing file :" + 's3://parquet-bucket-1/'+ table_list[split_table_list.index(i)] + 'start time : ', datetime.datetime.now())

                                logger.log("Creating Database Connection")
                                con = get_db_conn()
                                cur = con.cursor()

                                logger.log("Truncating the table")
                                logger.log(table_list[split_table_list.index(i)][0])

                                curr_table = "truncate table global." + table_list[split_table_list.index(i)][0]
                                cur.execute(curr_table)

                                logger.log("Copying started for parquet file to redshift")
                                logger.log(table_list[split_table_list.index(i)])

                                cur.execute(copy_command)
                                con.commit()
                                logger.log("Copying completed to redshift for file")
                                logger.log(table_list[split_table_list.index(i)])

                                print("Data processing completed successfully for file :" + 's3://parquet-bucket-1/' + table_list[split_table_list.index(i)],datetime.datetime.now())
                    else:

                        logger.error("Index File Not Present-----!!")
                        print("writing in log file index.txt not present")
            else:
                logger.error('S3 Bucket Not Found-----!!')
                print("writing in log file s3 bucket not found")
        except Exception as e:
            logger.error("Something went wrong while processing index and parquet bucket----->")

    except Exception as e:
        logger.critical("Main Execution Stopped----->")

    finally:
        upload_log('log-bucket')
        logger.info("Job Executed------------------------------------------------------------------------------------------------------------------")



















