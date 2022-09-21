from axial.sftp import sftp_list_files
from axial.sftp import sftp_to_s3
from axial.sftp import copy_s3_to_s3
from axial.sftp import sftp_connect
from axial.sftp import sftp_delete_file
#import pgdb
import boto3
from botocore.exceptions import ClientError
import json
from awsglue.utils import getResolvedOptions
#import paramiko
import re
from io import BytesIO
from io import StringIO
import io
import zipfile
import gzip
import datetime
import pgdb
import hashlib
import pandas as pd
from datetime import date
import subprocess
import sys
import os
import xlsxwriter

# def install(package):
#     subprocess.check_call(f"pip install {package}")
    
# install("XlsxWriter")

#os.system('pip install XlsxWriter')

# def install_and_import(package):
#     import importlib
#     try:
#         importlib.import_module(package)
#     except ImportError:
#         import pip
#         pip.main(['install', package])
#     finally:
#         globals()[package] = importlib.import_module(package)


# install_and_import('XlsxWriter')
















today = date.today()

# dd/mm/YY
d1 = today.strftime("%Y%m%d")


#Grabbing Job Parameters 
args = getResolvedOptions(sys.argv, ['client_extract_id'])
client_extract_id  = args['client_extract_id']



try:
    #Grabbing Redshift Credentials
    client = boto3.client("secretsmanager", region_name="us-east-1")
    get_secret_value_response_red = client.get_secret_value(SecretId='DataOps-ODS-redshift')
    secret2 = get_secret_value_response_red['SecretString']
    secret2 = json.loads(secret2)
    
    print(secret2)
    
    ##red
    dbname = secret2.get('database')
    host = secret2.get('hostname')
    username = secret2.get('user')
    password = secret2.get('password')
    port = secret2.get('port')
    
    #Getting job specifications
    con = pgdb.connect(database=dbname, host=host, port=port, user=username, password=password)
    cur = con.cursor()
    
    sql = f'select * from reference_tables_etl.master_file_client_extract_id where client_extract_id = {str(client_extract_id)} ;'
    print(sql)
    
    cur.execute(sql)
    
    if cur.rowcount != 0:
        print(cur.rowcount)
        client_extract_id,filename,extract_to,db_secret,sftp_secret,bucket,s3_file_path,sftp_file_path,query,utl_job,client_name = cur.fetchone()
        print(client_extract_id,filename,extract_to,db_secret,sftp_secret,bucket,s3_file_path,sftp_file_path,query,utl_job)
        
        #Executiing query
        sql_fetch = f"{query}"
        df = pd.read_sql_query(sql_fetch, con)
        df = pd.DataFrame(df)
        
        if str(client_name) == 'hmkde_md':
            with io.BytesIO() as output:
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df.to_excel(writer, 'sheet_name')
                data = output.getvalue()
                
                
            filename_w_date = filename + d1 + '.xlsx'
            file_bucket_path = f'{s3_file_path}{filename_w_date}'
            
            
            s3 = boto3.resource('s3')
            s3.Bucket(bucket).put_object(Key=file_bucket_path, Body=data)
            
        
        else:
            #Saving as csv
            output = StringIO()
            df.to_csv(output)
            
            filename_w_date = filename + d1 + '.csv'
    
    
    
            file_bucket_path = f'{s3_file_path}{filename_w_date}'
            s3 = boto3.resource('s3')
            s3.Object(bucket, f'{file_bucket_path}').put(Body=output.getvalue())
        
        cur.close()
        con.close()    
        
        if extract_to == 'sftp':
            client_sftp_secret = client.get_secret_value(SecretId=f'{sftp_secret}')
            client_sftp_secret = client_sftp_secret['SecretString']
            client_sftp_secret = json.loads(client_sftp_secret)
    
            print(client_sftp_secret)
    
            ##client_sftp credentials
            site1 = client_sftp_secret.get('site')
            username1 = client_sftp_secret.get('username')
            pwsecret1 = client_sftp_secret.get('password')
            port1 = int(client_sftp_secret.get('port'))  
            
            #upload to client sftp 
            s3_client = boto3.client("s3")
            sftp_conn = sftp_connect(site1, port1, username1, pwsecret1)
            with sftp_conn.open(sftp_file_path + filename_w_date, 'wb', 32768) as f:
                s3_client.download_fileobj(bucket, f'{file_bucket_path}', f)
            sftp_conn.close()
            
            #s3.Object(bucket, f'{file_bucket_path}').delete()
            
            
        
        if str(client_extract_id) == '6':
            sns1 = boto3.client("sns")
            topic_arn = 'arn:aws:sns:us-east-1:877582243023:Data-OPS_CSIN_Daily_Extract'
                
            # construct the email message
            email_dict={}
            email_dict['sub'] = 'CSIN DAILY EXTRACTS LOADING'
            #email_dict['sub'] = 'THIS IS A TEST - Wayspring'
            email_body = 'CSIN DAILY EXTRACT tables have been created and will be loaded shortly to /csin_extract'
                
            print(email_body)
            response = sns1.publish(TopicArn=topic_arn, Message=email_body,Subject=email_dict['sub'])
except:
    #### 
    sns1 = boto3.client("sns")
    topic_arn = 'arn:aws:sns:us-east-1:877582243023:DataOps-ODS-AWSGlueAlert-Redshift_Care_Management_Model_Load'

    # construct the email message
    email_dict={}
    email_dict['sub'] = f'ERROR Client Files Extracts : client_extract_id = {client_extract_id}'
    #emailBody = emailBody 
    email_body = 'Failure on Client_Files_Extracts glue job'

    print(email_body)
    response = sns1.publish(TopicArn=topic_arn, Message=email_body,Subject=email_dict['sub'])
        
    
    

