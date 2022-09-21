#Grabbing Job Parameters 
args = getResolvedOptions(sys.argv, ['client_extract_id'])
client_extract_id  = args['client_extract_id']

#Grabbing Redshift Credentials
client = boto3.client("secretsmanager", region_name="us-east-1")
get_secret_value_response_red = client.get_secret_value(SecretId='DataOps-ODS-redshift')
secret2 = get_secret_value_response_red['SecretString']
secret2 = json.loads(secret2)

##red
dbname = secret2.get('database')
host = secret2.get('hostname')
username = secret2.get('user')
password = secret2.get('password')
port = secret2.get('port')






con = pgdb.connect(database=dbname, host=host, port=port, user=username, password=password)

sql_fetch = f"{query}"
df = pd.read_sql_query(sql_fetch, con)
df = pd.DataFrame(df)

#Saving File as Xlsx
with io.BytesIO() as holder:
    with pd.ExcelWriter(holder, engine='xlsxwriter') as writer:
        df.to_excel(writer, 'sheet_name')
    output = holder.getvalue()
    
    
#Saving File as csv
output = StringIO()
df.to_csv(output)


#Saving to s3
file_name = f'redshift_migration/Exasol_Archive/{schema_name}/{table}.<txt or xlsx>'
s3.upload_fileobj(output, bucket, file_name)
#or for xlsx
s3 = boto3.resource('s3')
s3.Bucket(bucket).put_object(Key=file_bucket_path, Body=output)

con.close()

