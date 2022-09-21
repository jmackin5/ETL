from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.auth.client_credential import ClientCredential
import pandas as pd
import os
import boto3
import io 
from io import StringIO
from datetime import date 
import pgdb
import json 


today = date.today()

# dd/mm/YY
d1 = today.strftime("%Y%m%d")


client_id = 'id'
client_secret =  'secret'
client_credentials = ClientCredential(f'{client_id}',f'{client_secret}')
url = 'https://axialhealthcare.sharepoint.com/sites/BITeam/'
ctx = ClientContext(f'{url}').with_credentials(client_credentials)



#grabing data
data = File.open_binary(ctx, "/sites/BITeam/Shared Documents/Master Data - Recovery/Episodes excepted from eligibility checking.xlsx").content

#Reading bytes into excel 
toread = io.BytesIO()
toread.write(data)  # pass your `decrypted` string as the argument here
toread.seek(0)  # reset the pointer

#need to convert excel to csv for upload
df = pd.read_excel(toread, engine='openpyxl')
#only need first 15 columns
df = df.iloc[:,:15]
#Universly converting date columns 
df['Exception From Date'] = pd.to_datetime(df['Exception From Date'], errors='coerce')
df['Exception Thru Date'] = pd.to_datetime(df['Exception Thru Date'], errors='coerce')
df['Last Reviewed Date'] = pd.to_datetime(df['Last Reviewed Date'], errors='coerce')
df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce')
df['Date added to File'] = pd.to_datetime(df['Date added to File'], errors='coerce')
df['Date sent to Health Plan'] = pd.to_datetime(df['Date sent to Health Plan'], errors='coerce')
#df.head()


#Transfering df to bucket
bucket = 'axialhealthcare-knox-etl-raw-dev' # already created on S3
csv_buffer = StringIO()
df.to_csv(csv_buffer, sep='|', index=False)
s3_resource = boto3.resource('s3')

file_name = f'redshift/episodes_excepted_from_eligibility_checking_{d1}.csv'
s3_resource.Object(bucket,  file_name).put(Body=csv_buffer.getvalue())








### Connecting to Redshift
client = boto3.client("secretsmanager", region_name="us-east-1")
get_secret_value_response = client.get_secret_value(
            SecretId='DataOps-ODS-redshift')

            
secret = get_secret_value_response['SecretString']
secret = json.loads(secret)
    
dbname = secret.get('database')
dbhost = secret.get('hostname')
dbusername = secret.get('user')
dbpassword = secret.get('password')
dbport = secret.get('port')


con = pgdb.connect(database=dbname, host=dbhost, port=dbport, user=dbusername, password=dbpassword)
cur = con.cursor()



sqlcoms = [ "delete from ops_model.tbl_episode_eligibility_exceptions;"

           , f"COPY ops_model.tbl_episode_eligibility_exceptions  FROM 's3://axialhealthcare-knox-etl-raw-dev/{file_name}' iam_role 'arn:aws:iam::877582243023:role/Redshift_role' csv ignoreheader 1 delimiter '|' DATEFORMAT 'YYYY-MM-DD';"
           
          ]



try: 
    for sql in sqlcoms:
        
        cur.execute(f"""
                        {sql}
                    """)
        con.commit()
        
        
    #Close connection 
    cur.close()
    con.close()

except: 
    sns1 = boto3.client("sns")
    topic_arn = 'arn:aws:sns:us-east-1:877582243023:JobErrorAlert'
    
    
    # construct the email message
    email_dict={}
    email_dict['sub'] = 'Failed Ops_model Execution: tbl_episode_eligibility_exceptions'
    email_body = 'These Tables Didnt Load Sharepoint file didnt load'
    
    print(email_body)
    response = sns1.publish(TopicArn=topic_arn, Message=email_body,Subject=email_dict['sub'])

    
    
    
