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
truncate_table_list = []

# LOG FILE SET UP
LOG_FORMAT = '{lineno}  : {name}: {asctime}: {message}'
basicConfig(filename='logfile.log',level=DEBUG, filemode = 'a',style='{',format=LOG_FORMAT)
logger = getLogger('SFHTC')

# AWS SECRETE MANAGER

secrets = get_secret()
secrets = secrets.replace("\n", "")
secrets = secrets.replace(" ", "")
secrets = json.loads(secrets)
# print(secrets["region_name"])


# MAIN FUNCTION

if __name__ == '__main__':
    try:        
        logger.info("-----AWS S3 Connectivity Intiated-----")
        logger.info("Setting Up S3 client")
        s3_client = boto3.client("s3", region_name=secrets["region_name"], aws_access_key_id=secrets["aws_access_key_id"], aws_secret_access_key=secrets["aws_secret_access_key"])
        logger.info("Setting Up Os.environ")        
        s3 = boto3.resource('s3')

        # APPENDING ALL BUCKET NAMES IN A LIST
        try:
            logger.info('checking for all the buckets')
            for bucket in s3.buckets.all():
                s3_bucket.append(bucket.name)
                logger.info('Appended all the buckets to s3_bucket list')
        except Exception as e:
            logger.error("Unable to fetch S3 bucket")
            logger.error(e)        
        
        
        # Read the index.txt file from index-bucket-cmp bucket
        try:
            if 'index-bucket-sfs' and 'parquet-bucket-sfs' in s3_bucket:
                logger.info("index-bucket and parquet-bucket found in the s3_bucket list")
                index_bucket = s3.Bucket('index-bucket-sfs')
                parquet_bucket1 = s3.Bucket('parquet-bucket-sfs')
                
                logger.info("Checking for parquet file and table")
                
                #TESTING
                for obj1 in parquet_bucket1.objects.all():
                    key1 = obj1.key
                    table_list.append(key1)
                    if key1.split('/')[0] not in table_list:
                        split_table.append(key1.split('/')[0])

                for obj2 in parquet_bucket1.objects.all():
                    key2 = obj2.key
                    table_list_1.append(key2)
                    if key1.split('/')[1] not in table_list_1:
                        split_table_list.append(key1.split('/')[1])

                
                logger.info("checking for index file in index bucket")
                for obj in index_bucket.objects.all():                    
                    key = obj.key
                    # print(key)
                    if key=='index.txt':
                        logger.info("index.txt file found in the bucket")
                        print("checking for the validity of the index file if needed")
                        body = obj.get()['Body']._raw_stream.readline()  # _raw_stream added 
                        str1 = body.decode('UTF-8')
                        str2 = str1.split(',')
                        # print(str2)
                        for i in str2:
                            l = i.split('|')
                            if l[0].endswith('.parquet'):
                                index_parquet_list.append(l[0])
                        logger.info("Parquet file details of index.txt updated in index_parquet_list")                       
                        for i in index_parquet_list:  
                            logger.info("checking parquet file")
                            logger.info(i)
                        for i in split_table_list: 
                            logger.info("Available in s3 parquet bucket") 
                        print("Table List : ",table_list)
                            # print("Table List 1 : ",table_list_1)
                            # print("split Table List : ", split_table_list)
                        print("split Table : ", split_table)
                        count_array = []
                        count=0
                        prev = ''
                        count_array.append('F')
                        for i in range(0,len(split_table)-1):
                            if split_table[i] == split_table[i+1]:
                                count_array.append('T')
                            else:
                                count_array.append('F')
                        print(count_array)
                        n= len(count_array)
                        # print(n)
                        # print(secrets)
                        con = get_db_conn(secrets)
                        cur = con.cursor()
                        print('PASSED')
                        #Truncate the Database
                        for i in split_table:
                            if i not in truncate_table_list: 
                                curr_table = "truncate table " + i
                                cur.execute(curr_table)
                                truncate_table_list.append(i)
                        # print(truncate_table_list)

                        for i in range(n):
                            if(count_array[i]=='T'):
                                print("Parquet file validation will happen")
                                table = split_table[i]
                                parquet_buck = table_list[i]
                                # print(table,':',parquet_buck)
                                # copy_command = ('"'+'COPY '+table+ ' FROM '+'s3://parquet-bucket-sfs/'+parquet_buck+' IAM_ROLE arn:aws:iam::143580737085:role/migrationrole'+' FORMAT AS PARQUET;'+'"')
                                # print(copy_command)
                                print("Db connection started ")
                                # logger.log("Creating Database Connection") 
                                con = get_db_conn(secrets)
                                cur = con.cursor()
                                # logger.log("Truncating the table")
                                # logger.log(table_list[split_table_list.index(i)][0])
                                
                                print("copy started ",parquet_buck)
                                print("Db connection established")
                                copy_command = ("COPY "+table +" FROM '"+'s3://parquet-bucket-sfs/'+parquet_buck+"' IAM_ROLE '"+"arn:aws:iam::143580737085:role/migrationrole'"+"FORMAT AS PARQUET;")
                                # print(copy_command)                              
                                cur.execute(copy_command) # CODE NOT EXECUTED
                                print('execute ended')
                                con.commit()

                            else:
                                logger.error("Erro in Database Connection")
                                
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
        upload_log('index-bucket-cmp')
        
        logger.info("Job Executed------------------------------------------------------------------------------------------------------------------")