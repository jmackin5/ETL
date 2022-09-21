from axial.sftp import sftp_list_files
from axial.sftp import sftp_to_s3
from axial.sftp import copy_s3_to_s3
from axial.sftp import sftp_connect
from axial.sftp import sftp_delete_file
#import pgdb
import sys
import boto3
from botocore.exceptions import ClientError
import json
from awsglue.utils import getResolvedOptions
#import paramiko
import re
from io import BytesIO
import zipfile
import gzip
import datetime
import pgdb
import hashlib
import pandas as pd
from botocore.client import Config

emailBody = ''
err_message = ''

##To send email alert
sns1 = boto3.client("sns")
topic_arn = 'arn:aws:sns:us-east-1:877582243023:DataOps-ODS-AWSGlueAlert'

raw_rec_count = 0
BLOCK_SIZE = 65536 # The size of each read from the file
file_hash = hashlib.sha1()
renameunzippedfile = ''
def s3_eTag(bucket_name, resource_name):
    try:
        eTag = boto3.client('s3').head_object(Bucket=bucket_name,Key=resource_name)['ETag'][1:-1]
            
    except botocore.exceptions.ClientError:
        eTag = None
        pass
    return eTag

#Run Glue job    
def run_glue_job(job_name, arguments = {}):
   session = boto3.session.Session()
   glue_client = session.client('glue')
   try:
      job_run_id = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
      return job_run_id
   except ClientError as e:
      raise Exception( "boto3 client error in run_glue_job: " + e.__str__())
   except Exception as e:
      raise Exception( "Unexpected error in run_glue_job: " + e.__str__())
    

try: 
    #Get parameter values
    #args = getResolvedOptions(sys.argv, ['S3Bucket', 'FileName', 'SKey', 'FileDomain','Sourcefolder','Destinationfolder','TestConnection','AxialSFTPKey','AxialSFTPPath','Client','RenameSourceFile','DynamicS3Folder'])
    args = getResolvedOptions(sys.argv, ['master_file_detail_sftp_id'])
    master_file_detail_sftp_id  = args['master_file_detail_sftp_id']
    
    client = boto3.client("secretsmanager", region_name="us-east-1")
    #For DB config
    get_secret_value_response = client.get_secret_value(SecretId='DataOps-ODS-PostGres-Prod')
            
    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)
    
    dbname = secret.get('dbname')
    host = secret.get('host')
    username = secret.get('username')
    password = secret.get('password')
    port = secret.get('port')
    
    ##DB get job configs
    con = pgdb.connect(database=dbname, host=host, port=port, user=username, password=password)
    cur = con.cursor()
    sql = "select sftp_secretname,file_domain,client_name,s3_bucket,sftp_file_name_format,sftp_sourcefolder,s3_folder_path,axial_sftp_key,axial_sftp_path,file_name_format,file_encoding, s.master_file_detail_id "\
          "from client_process.master_file_detail fd inner join user_iindawala.master_file_detail_sftp s on fd.master_file_detail_id = s.master_file_detail_id "\
          "inner join client_process.master_client c on fd.client_id = c.client_id where master_file_detail_sftp_id = " + str(master_file_detail_sftp_id)
    
    print(sql)
    cur.execute(sql)
    if cur.rowcount != 0 :
        P_SecretName, FileDomain, Axialclient, S3Bucket, filenameLike, Sourcefolder,Destinationfolder, AxialSFTPKey, AxialSFTPPath, Axialfile_name_format, file_encoding, master_file_detail_id = cur.fetchone()
        cur.close()
        con.close()
        TestConnection = 'False'
        RenameSourceFile = 'False'
        if str.replace(filenameLike,'*','') != str.replace(Axialfile_name_format,'%',''):
            RenameSourceFile = 'True'


        if RenameSourceFile == 'True':
            x = datetime.datetime.now()
            AppendDateFileName = x.strftime("%Y")+x.strftime("%m") + x.strftime("%d")
            AppendCurYearMonth = x.strftime("%Y")+x.strftime("%m")
            if FileDomain == 'census' and Axialclient == 'agptn':
                renamezipfile = 'AmerigroupTN_'+ AppendDateFileName +'.zip'
                s3key = 'client_files/agptn/census/'+ AppendCurYearMonth +'/'
                renameunzippedfile = 'AmerigroupTN_Census_'+ AppendDateFileName +'.txt'
        else:
            AppendFileName = ''
            s3key = Destinationfolder
            
            
        ###temporary fix as we are switching highmark sftps
        if str(master_file_detail_sftp_id) in ['68','69','66','67','37','310','311','241','65']:
            P_SecretName = 'DataOps-SFTP-Highmark-Test'
            
            
        #grab client_sftp credentials
        if P_SecretName == 'S3':
            ##Connecting to s3
            s3 = boto3.resource('s3')
            my_bucket = s3.Bucket(S3Bucket)
            
            ## Getting a list of files in bucket
            listOfPlaces = []
            for my_bucket_object in my_bucket.objects.filter(Prefix=Sourcefolder +'From_Remote_Desktop/'):
                if my_bucket_object.key.endswith('.txt') or my_bucket_object.key.endswith('.csv'):
                    listOfPlaces.append(my_bucket_object.key.split('/')[-1]) 
                    
        else: 
            client = boto3.client("secretsmanager", region_name="us-east-1")
            get_secret_value_response = client.get_secret_value(
                SecretId=P_SecretName)
            secret = get_secret_value_response['SecretString']
            secret = json.loads(secret)
            
            site = secret.get('site')
            username = secret.get('username')
            pwsecret = secret.get('password')
            port = int(secret.get('port'))
            
            listOfPlaces = sftp_list_files(site, port, username, pwsecret, Sourcefolder)            
            
            
            
        
        #For DB
        get_secret_value_response = client.get_secret_value(
                SecretId='DataOps-ODS-PostGres-Prod')
                #"DataOps-ODS-PostGres-Prod")
                #"DataOps-ODS-PostGres-QA")
                
        secret = get_secret_value_response['SecretString']
        secret = json.loads(secret)
        
        dbname = secret.get('dbname')
        dbhost = secret.get('host')
        dbusername = secret.get('username')
        dbpassword = secret.get('password')
        dbport = secret.get('port')
        #print('DBUserName ' + dbusername)
    
        
        ##Initializing file found
        found = 'No'
        date1 = datetime.datetime.now()
        curYearMonth = date1.strftime("%Y")+ date1.strftime("%m")

            
        
        filenamelike = str.replace(filenameLike,"*","[A-Z, ,a-z,0-9,.,\,-,_,-]*",)
        pattern = re.compile(filenamelike)
        print(filenamelike, pattern)
        for filename in listOfPlaces:
            #print(listOfPlaces)
            if pattern.fullmatch(filename,0) :
                found = 'Yes'
                filename1 = filename ##assign filename to filename1
                #path to file on s3 sftp
                sourcefilename = Sourcefolder + filename1
                #path to file on s3 client stage bucket 
                destfilename = Destinationfolder + 'stage/' + filename1 
                

                #
                ##For normal files, we upload from sftp->s3(stage)
                ##For s3 files, we upload from s3->s3(stage)
                #
                if P_SecretName == 'S3':
                    copy_source = {
                                 'Bucket': S3Bucket,
                                 'Key': Sourcefolder +'From_Remote_Desktop/'+filename 
                                    }
                    s3.meta.client.copy(copy_source, S3Bucket, destfilename)
                else:
                    sftp_to_s3(site, port, sourcefilename, S3Bucket, destfilename, username, pwsecret)


                
                if not (filenamelike.endswith('.gz') or filenamelike.endswith('.zip')):
                    if FileDomain == 'prior_auth' and Axialclient == 'csin':
                        copy_s3_to_s3(S3Bucket, destfilename, S3Bucket, destfilename, unidecode_flag=True,remove_tail=True, tail_regex="T|")#, credentials=None)
                    else:
                        copy_s3_to_s3(S3Bucket, destfilename, S3Bucket, destfilename, unidecode_flag=True,remove_tail=True, tail_regex="TLR|")#, credentials=None)
                print('Downloaded ' + sourcefilename + ' From SFTP and Uploaded ' + destfilename + ' To S3 Stage')
                
                
                
                #Find Compression
                if re.search('.gz', filename1):
                    fileCompress = 'gz'
                elif re.search('.zip', filename1):
                    fileCompress = 'zip'
                else:
                    fileCompress = 'None'
                    
                    
                #Deleting file from Client SFTP
                #Dont need to delete file from sftp for s3 files
                #Kinda want to move this till the end? -jm
                if Axialclient != 'agptn':
                    if P_SecretName == 'S3':
                        print('s3 file: Dont need to delete from client sftp')
                    else:
                        sftp_delete_file(site, port, username, pwsecret,sourcefilename) #Deleting file from Client SFTP
                        
                        
                #Getting credentials for axial sftp  
                get_secret_value_response = client.get_secret_value(
                    SecretId=AxialSFTPKey)
                
                secret1 = get_secret_value_response['SecretString']
                secret1 = json.loads(secret1)
            
                site1 = secret1.get('site')
                username1 = secret1.get('username')
                pwsecret1 = secret1.get('password')
                port1 = int(secret1.get('port'))   
                        
                

                
                #gz compressed
                if fileCompress == 'gz':
                    bucket = S3Bucket
                    #s3 client stage path (.gz included)
                    gzipped_key = Destinationfolder + 'stage/' + filename1
                    #s3 client stage/uncompressed path (w/o .gz included- meaning just a .txt now)
                    uncompressed_key = Destinationfolder + 'stage/'+ 'Uncompressed/' + str.replace(filename1,'.gz','')
                    # initialize s3 client, this is dependent upon your aws config being done 
                    
                    #old way 
                    #s3 = boto3.client('s3', use_ssl=False)  # optional
                    # s3.upload_fileobj(                      # upload a new obj to s3
                    #     Fileobj=gzip.GzipFile(              # read in the output of gzip -d
                    #         None,                           # just return output as BytesIO
                    #         'rb',                           # read binary
                    #         fileobj=BytesIO(s3.get_object(Bucket=bucket, Key=gzipped_key)['Body'].read())),
                    #     Bucket=bucket,                      # target bucket, writing to
                    #     Key=uncompressed_key)               # target key, writing to
                        
                        
                    s3 = boto3.client('s3', use_ssl=True, config=Config(signature_version='s3v4') )  # optional

                    gzippedfileunbit = s3.get_object(Bucket=bucket, Key=gzipped_key)['Body'].read()
                    sbit = BytesIO(gzippedfileunbit)
                    
                    s3.upload_fileobj(                      # upload a new obj to s3
                        Fileobj=gzip.GzipFile(              # read in the output of gzip -d
                            None,                           # just return output as BytesIO
                            'rb',                           # read binary
                            fileobj=sbit),
                        Bucket=bucket,                      # target bucket, writing to
                        Key=uncompressed_key)               # target key, writing to
                        
                    print('gz uncompress Done')
                        
                     ###Upload file to ODS box
                    if AxialSFTPKey != 'None':
                        #This is where port1 went 
                        #Connecting to axial sftp
                        sftpconn = sftp_connect(site1, port1, username1, pwsecret1)
                        print('ODS connected')
                        
                        s3 = boto3.client('s3')
                        with sftpconn.open(AxialSFTPPath  +'/'+ str.replace(filename1,'.gz',''), 'wb', 32768) as f:
                            s3.download_fileobj(bucket,uncompressed_key,f) ## Destinationfolder + 'Uncompressed/' + filename1, f)
                        
                        finalFileName = str.replace(filename1,'.gz','')
                        



                        
                        #Get shasum of the file
                        with sftpconn.open(AxialSFTPPath +'/'+ str.replace(filename1,'.gz',''),'rb') as f:
                            fb = f.read(BLOCK_SIZE) # Read from the file. Take in the amount declared above
                            while len(fb) > 0: # While there is still data being read from the file
                                file_hash.update(fb) # Update the hash
                                fb = f.read(BLOCK_SIZE) # Read the next block from the file
                        
                        ##Removing Trailor line
                        with sftpconn.open(AxialSFTPPath +'/'+ str.replace(filename1,'.gz','')) as fd:
                            lines = fd.readlines()
                            total_lines_in_file = (len(lines))
                            lastline = (lines[total_lines_in_file-1])
                            #print('Last Line - ' + (lines[total_lines_in_file-1]))
                            fd.close()
                        
                        if lastline.startswith('TLR') or lastline.startswith('T|') :
                            #with sftpconn.open(AxialSFTPPath + Axialclient +'/' + FileDomain  +'/'+ renameunzippedfile,'w') as fw:
                            with sftpconn.open(AxialSFTPPath +'/'+ str.replace(filename1,'.gz',''),'w') as fw:
                                fw.writelines([item for item in lines[:-1]])
                                fw.close()
                                print('Last Trailor Line Removed')
                        
                        shasum= file_hash.hexdigest()
                        print('Shasum - ' + shasum)
                        
                        ##Move file to Main S3 bucket and Deleting Uncompressed file from S3
                        s3 = boto3.resource('s3')
                        copy_source = {'Bucket': S3Bucket,'Key': gzipped_key}
                        bucket = s3.Bucket(S3Bucket)
                        bucket.copy(copy_source, str.replace(gzipped_key,'stage',curYearMonth))
                        print('Replaced file from stage folder - ' + str.replace(gzipped_key,'stage',curYearMonth))
                        print('Uncompressed key -' + uncompressed_key)
                        ##Delete Uncompressed file
                        ##s3.Object(bucket, uncompressed_key).delete() #need to fix this
                        
                        
                
                ##Zip compressed
                if fileCompress == 'zip':
                    print('Zip Starting')
                    bucket = S3Bucket
                    zipped_key = Destinationfolder + 'stage/' + filename1
                    s3_resource = boto3.resource('s3')
                    zip_obj = s3_resource.Object(bucket_name=bucket, key=zipped_key)
                    buffer = BytesIO(zip_obj.get()["Body"].read())
                    
                    z = zipfile.ZipFile(buffer)
                    for filename in z.namelist():
                        filename1 = filename
                        key = Destinationfolder + 'stage/' + 'Uncompressed/' + filename1
                        #file_info = z.getinfo(filename)
                        s3_resource.meta.client.upload_fileobj(
                            z.open(filename),
                            Bucket=bucket,
                            #Key=f'test/glue/temp/Test/Uncompressed/{filename}'
                            Key= Destinationfolder + 'stage/' + 'Uncompressed/' + filename1
                        )
                    print('Zip uncompress Done - ' + key)
                    
                    ##Uploading to ODS box
                    if AxialSFTPKey != 'None':
                        
                        if renameunzippedfile == '':
                            renameunzippedfile = filename1
                        
                        
                        if FileDomain == 'prior_auth' and Axialclient == 'csin':
                            copy_s3_to_s3(S3Bucket, key, S3Bucket, key, unidecode_flag=True,remove_tail=True, tail_regex="T|")
                        else:
                            copy_s3_to_s3(S3Bucket, key, S3Bucket, key, unidecode_flag=True,remove_tail=True, tail_regex="TLR|")
                        
                        
                        #Connecting to axialsftp
                        sftpconn = sftp_connect(site1, port1, username1, pwsecret1)
                        s3 = boto3.client('s3')
                        with sftpconn.open(AxialSFTPPath + '/' + renameunzippedfile, 'wb', 32768) as f:
                            s3.download_fileobj(bucket, Destinationfolder + 'stage/' + 'Uncompressed/' + filename1, f)
                        
                        if FileDomain != 'census' and Axialclient != 'agptn':
                            s3key = str.replace(destfilename,'stage',curYearMonth)
                            renamezipfile = ''
                            print('S3Key for non Census Zip - ' + s3key)
                        
                        if FileDomain == 'census' and Axialclient != 'agptn':
                            s3key = str.replace(destfilename,'stage',curYearMonth)
                            renamezipfile = ''
                            print('S3Key for Zip - ' + s3key)
                        
                        
                        ##Removing last line if blank
                        if Axialclient == 'agptn' and FileDomain == 'census':
                            with sftpconn.open(AxialSFTPPath + '/' + renameunzippedfile) as fd:
                                lines = fd.readlines()
                                total_lines_in_file = (len(lines))
                                #print('Length of last Line - ' + str(len(lines[total_lines_in_file-1])))
                                fd.close()
                            
                            if len(lines[total_lines_in_file-1]) < 5 :
                                #with sftpconn.open(AxialSFTPPath + Axialclient +'/' + FileDomain  +'/'+ renameunzippedfile,'w') as fw:
                                with sftpconn.open(AxialSFTPPath + '/' + renameunzippedfile,'w') as fw:
                                    fw.writelines([item for item in lines[:-1]])
                                    fw.close()
                        
                        
                        finalFileName = renameunzippedfile
                        
                        #Get shasum of the file
                        with sftpconn.open(AxialSFTPPath  + '/' + renameunzippedfile,'rb') as f:
                            fb = f.read(BLOCK_SIZE) # Read from the file. Take in the amount declared above
                            while len(fb) > 0: # While there is still data being read from the file
                                file_hash.update(fb) # Update the hash
                                fb = f.read(BLOCK_SIZE) # Read the next block from the file
                        
                        shasum= file_hash.hexdigest()
                        
                        print('Upload Unzipped to ODS -'+ AxialSFTPPath +'/'+ renameunzippedfile)
                        
                        ##Move file to Main S3 bucket and Deleting Uncompressed file from S3
                        s3 = boto3.resource('s3')
                        copy_source = {'Bucket': S3Bucket,'Key': zipped_key}
                        s3.meta.client.copy(copy_source, S3Bucket, s3key+renamezipfile)
                        
                        ##Delete Uncompressed and original file
                        s3.Object(bucket, Destinationfolder + 'stage/' + 'Uncompressed/' + filename1).delete()
                        s3.Object(bucket, destfilename).delete()
                        
                        print('Uncompressed file deleted from S3')
                        
                        renameunzippedfile = ''
                
                ##Non compressed file
                if fileCompress == 'None' and AxialSFTPKey != 'None':
                    #connecting
                    sftpconn = sftp_connect(site1, port1, username1, pwsecret1)
                    print('ODS connected')
                    
                    finalFileName = filename1
                    
                    if FileDomain == 'census' and Axialclient == 'shca':
                        s = filename1[0:6] # counting from the end will ensure that you extract the date no matter what the "name" is and how long it is
                        s = '20' + s
                        date1 = datetime.datetime(year=int(s[0:4]), month=int(s[4:6]), day=int(s[6:8])) + datetime.timedelta(days=1)
                        Loaddate = date1.strftime("%Y")+ date1.strftime("%m") + date1.strftime("%d")
                        curYearMonth = date1.strftime("%Y")+ date1.strftime("%m")
                        finalFileName =  'hcaz_census_'+ Loaddate +'.csv'
                        s3key = 'client_files/shca/census/'+ curYearMonth +'/'+ finalFileName

                    else:
                        s3key = str.replace(destfilename,'stage',curYearMonth)
                        
                    s3 = boto3.client('s3')
                    with sftpconn.open(AxialSFTPPath + '/' + finalFileName, 'wb', 32768) as f:
                            s3.download_fileobj(S3Bucket, destfilename, f)
                    
                    #Get shasum of the file
                    with sftpconn.open(AxialSFTPPath + '/' + finalFileName,'rb') as f:
                        fb = f.read(BLOCK_SIZE) # Read from the file. Take in the amount declared above
                        while len(fb) > 0: # While there is still data being read from the file
                            file_hash.update(fb) # Update the hash
                            fb = f.read(BLOCK_SIZE) # Read the next block from the file
                            
                    # #keep sending over files with bad characters, and we dont have a contact person to have them fix it  
                    # if Axialclient == 'agptn' and FileDomain == 'eligibility': 
                            
                    #     # Read in the file
                    #     with sftpconn.open(AxialSFTPPath + '/' + finalFileName, 'r') as file :
                    #       filedata = file.read()

                    #     # Replace the target string
                    #     print('we made it')
                    #     filedata = str(filedata).replace('"', '')
                    #     print('we made it')
                    #     filedata = str(filedata).replace("^", '')
                    #     print('we made it')

                    #     # Write the file out again
                    #     with sftpconn.open(AxialSFTPPath + '/' + finalFileName, 'w') as file:
                    #       file.write(filedata)                            
                            
                            
                            
                        
                    shasum= file_hash.hexdigest()
                    print('Shasum - ' + shasum)
                    
                    
                    ##Move file to Main S3 bucket and Deleting file from S3 stage
                    s3 = boto3.resource('s3')
                    copy_source = {'Bucket': S3Bucket,'Key': destfilename}
                    s3.meta.client.copy(copy_source, S3Bucket, s3key)
                    
                    ##Delete file from stage
                    s3.Object(S3Bucket, destfilename).delete()
                

                
                ##DB executions
                con = pgdb.connect(database=dbname, host=dbhost, port=dbport, user=dbusername, password=dbpassword)
                cur = con.cursor()
                #insert file log id
                sql = "select client_process.insert_file_log(" + "'" + Axialclient + "'"','"'" + FileDomain + "'"','"'" +  finalFileName + "'"','"'" + shasum + "'"')'""
                print(sql)
                cur.execute(sql)
                con.commit()
                cur.execute("select file_log_id,file_name from client_process.file_log order by file_log_id desc limit 1")
                file_log_id,file_name = cur.fetchone()
                
                if file_name == finalFileName:
                    print('File name matches')
                    sql1 = ''
                    sql2 = ''
                    sql3 = ''
                    sql4 = ''
                    sql5 = ''
                    sql6 = ''
                    sql7 = ''
                    sql8 = ''
                    sql9 = ''
                    sql10 = ''
                    sql11 = ''
                    sql12 = ''
                    
                    rec1 = ''
                    rec2 = ''
                    rec3 = ''
                    rec4 = ''
                    rec5 = ''
                    rec6 = ''
                    rec7 = ''
                    rec8 = ''
                    rec9 = ''
                    rec10 = ''
                    rec11 = ''
                    rec12 = ''
                    
                    #sql = "select  user_iindawala.import_files_to_raw(" + str(file_log_id) +")"
                    ##For oa_csin need to only run file log id
                    if Axialclient == 'oa_hmkny' or Axialclient == 'oa_csin' or Axialclient == 'oa_shpwi'  or (Axialclient == 'uhctn' and FileDomain != 'census')  or (Axialclient == 'uhctn_di') or (Axialclient in ('hmkpa','hmkwv') and FileDomain == 'eligibility'):
                        print(Axialclient)
                        sql1 = "select  client_process.import_files_to_raw(" + str(file_log_id) +")"
                        ##sql2 = "select  client_process.import_keys_to_stage(" + str(file_log_id) +")"
                    else:
                        print('Non oa_csin')
                        sql1 = "select  client_process.import_files_to_raw(" + str(file_log_id) +")"
                        sql2 = "select  client_process.import_keys_to_stage(" + str(file_log_id) +")"
                    
                    

                    
                    #Census
                    if Axialclient == 'shca' and FileDomain == 'census':
                        sql3 = "select  client_stage.hcaz_census_stage(" + str(file_log_id) +")"
                        sql4 = "select  client_production.import_stage_to_prod(9, 'census') "
                    if Axialclient == 'agptn' and FileDomain == 'census':
                        sql3 = "select  client_stage.agptn_census_stage(" + str(file_log_id) +")"
                        sql4 = "select  client_production.import_stage_to_prod(5, 'census') "
                    if Axialclient == 'hmkpa' and FileDomain == 'census':
                        sql3 = "select  client_stage.hmkpa_census_stage(" + str(file_log_id) +")"
                        #sql4 = ''
                        sql4 = "select  client_production.import_stage_to_prod(3, 'census') "
                    if Axialclient == 'csin' and FileDomain == 'census':
                        sql3 = "select  client_stage.csin_census_stage(" + str(file_log_id) +")"
                        sql4 = "select  client_production.import_stage_to_prod(19, 'census') "
                    if Axialclient == 'csin' and FileDomain == 'prior_auth':
                        sql3 = "select client_stage.csin_prior_auth_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.import_stage_to_prod(19, 'prior_authorization') "
                    if Axialclient == 'uhctn' and FileDomain == 'census':
                        sql3 = "select  client_stage.uhctn_census_stage(" + str(file_log_id) +")"
                        sql4 = "select  client_production.import_stage_to_prod(6, 'census') " 
                    #Added on 02-24-2022
                    if Axialclient == 'azch' and FileDomain == 'census':
                        sql3 = "select  client_stage.azch_census_stage(" + str(file_log_id) +")"
                        sql4 = "select  client_production.import_stage_to_prod(18, 'census') " 
                        
                    if Axialclient == 'azch' and FileDomain == 'adt':
                        sql3 = "select  client_stage.azch_adt_stage(" + str(file_log_id) +")"
                        sql4 = "select  client_production.import_stage_to_prod(18, 'adt') " 
                        
                    if Axialclient == 'hmkde_md' and FileDomain == 'adt':
                        sql3 = "select client_stage.hmkde_md_adt_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.import_stage_to_prod(20, 'adt')"
                        
                    #need to add to real client load. 
                    if Axialclient == 'hmkde_md' and FileDomain == 'prior_auth':
                        sql3 = "select client_stage.hmkde_md_prior_auth_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.import_stage_to_prod(20, 'prior_authorization')"
                        
                        
                    if Axialclient == 'azch' and FileDomain == 'prior_auth':
                        sql3 = "select client_stage.azch_prior_auth_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.import_stage_to_prod(18, 'prior_authorization')"   
                        
                    if Axialclient == 'azch' and FileDomain == 'provider_identity':
                        sql3 = "select client_stage.azch_provider_record_stage(" + str(file_log_id) +")"
                        
                        
                        
                    #   
                    if Axialclient == 'agptn' and FileDomain == 'medical_claims':
                        sql3 = "select  client_stage.agptn_medical_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.medical_claim_pre_filter(" + str(file_log_id) +")"
                        sql5 = "select client_stage.agptn_medical_claim_filter(" + str(file_log_id) +")"
                        sql6 = "select client_production.medical_claim_promotion(" + str(file_log_id) +")"
                        sql7 = "select client_production.medical_claim_diagnosis(5)"
                        sql8 = "select client_production.medical_claim_events_including_eol(5)"
                        sql9 = "refresh materialized view client_production.member_malignant_hospice"
                        sql10 = "select dataops_to_reporting.mat_adherence_days('agptn')"
                        sql11 = "select dataops.npi_member_relationship('agptn')"

                    if Axialclient == 'agptn' and FileDomain == 'eligibility':
                        sql3 = "select  client_stage.agptn_member_stage(" + str(file_log_id) +")"
                        sql4 = "select  client_production.import_stage_to_prod(5, 'member')"
                        sql5 = "select  client_production.import_stage_to_prod(5, 'member_plan')"
                        sql6 = "select  client_production.member_month_by_lob(5)"
                        
                        
                    if Axialclient == 'agptn' and FileDomain == 'rx_claims':
                        sql3 = "select  client_stage.agptn_rx_claim_stage(" + str(file_log_id) +")"
                        ##Added 5/12
                        sql4 = "select  client_production.rx_claim_pre_filter(" + str(file_log_id) +")"
                        sql5 = "select  client_stage.agptn_rx_claim_filter(" + str(file_log_id) +")"
                        sql6 = "select  client_production.rx_claim_promotion(" + str(file_log_id) +")"
                        sql7 = "select  client_production.rx_claim_event(5)"
                        sql8 = "select  dataops_to_reporting.mat_adherence_days('agptn')"
                        sql9 = "select  dataops.npi_member_relationship('agptn')"
                        
                    if Axialclient == 'agptn' and FileDomain == 'provider_identity':
                        sql3 = "select  client_stage.agptn_provider_record_stage(" + str(file_log_id) +")"
                        sql4 = ''
                        #sql4 = "select  client_production.import_stage_to_prod(5, 'provider_identity') "
                    
                    #shca   
                    if Axialclient == 'shca' and FileDomain == 'medical_claims':
                        sql3 = "select  client_stage.shca_medical_claim_stage(" + str(file_log_id) +")"
                        sql4 = ''
                        #sql4 = "select  client_production.import_stage_to_prod(5, 'medical_claims') "
                    if Axialclient == 'shca' and FileDomain == 'eligibility':
                        sql3 = "select  client_stage.shca_member_stage(" + str(file_log_id) +")"
                        sql4 = ''
                        #sql4 = "select  client_production.import_stage_to_prod(5, 'eligibility') "
                    if Axialclient == 'shca' and FileDomain == 'rx_claims':
                        sql3 = "select  client_stage.shca_rx_claim_stage(" + str(file_log_id) +")"
                        sql4 = ''
                        #sql4 = "select  client_production.import_stage_to_prod(5, 'rx_claims') "
                    if Axialclient == 'shca' and FileDomain == 'provider_identity':
                        sql3 = "select  client_stage.shca_provider_record_stage(" + str(file_log_id) +")"
                        sql4 = ''
                        #sql4 = "select  client_production.import_stage_to_prod(5, 'provider_identity') "
                    
                    #bcbstn
                    if Axialclient == 'bcbstn' and FileDomain == 'medical_claims':
                        sql3 = "select  client_stage.bcbstn_medical_claim_stage(" + str(file_log_id) +")"
                        sql4 = ''
                        #sql4 = "select  client_production.import_stage_to_prod(5, 'medical_claims') "
                    if Axialclient == 'bcbstn' and FileDomain == 'eligibility':
                        sql3 = "select  client_stage.bcbstn_member_stage(" + str(file_log_id) +")"
                        sql4 = ''
                        #sql4 = "select  client_production.import_stage_to_prod(5, 'eligibility') "
                    if Axialclient == 'bcbstn' and FileDomain == 'rx_claims':
                        sql3 = "select  client_stage.bcbstn_rx_claim_stage(" + str(file_log_id) +")"
                        sql4 = ''
                        #sql4 = "select  client_production.import_stage_to_prod(5, 'rx_claims') "
                    if Axialclient == 'bcbstn' and FileDomain == 'provider_identity':
                        sql3 = "select  client_stage.bcbstn_provider_record_stage(" + str(file_log_id) +")"
                        sql4 = ''
                        #sql4 = "select  client_production.import_stage_to_prod(5, 'provider_identity') "
                    
                    if Axialclient == 'humky' and FileDomain == 'rx_claims':
                        ##Send notification when triggered
                        sns1.publish(TopicArn=topic_arn, Message='Started File load For - ' +  Axialclient + ' - ' + FileDomain ,Subject='Started File load For - ' +  Axialclient + ' - ' + FileDomain)
                       
                        sql3 = "select client_stage.humky_rx_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.rx_claim_pre_filter(" + str(file_log_id) +") "
                        sql5 = "select client_stage.humky_rx_claim_filter(" + str(file_log_id) +") "
                        sql6 = "select client_production.rx_claim_promotion(" + str(file_log_id) +") "
                        sql7 = "select client_production.rx_claim_event(8)"
                        sql8 = "select dataops_to_reporting.mat_adherence_days('humky')"
                        sql9 = "select dataops.npi_member_relationship('humky')"
                    
                    if Axialclient == 'humky' and FileDomain == 'medical_claims':
                       
                        sql3 = "select client_stage.humky_medical_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.medical_claim_pre_filter(" + str(file_log_id) +") "
                        
                    if Axialclient == 'hmkpa' and FileDomain == 'medical_claims':
                        ##Send notification when triggered
                        sns1.publish(TopicArn=topic_arn, Message='Started File load For - ' +  Axialclient + ' - ' + FileDomain ,Subject='Started File load For - ' +  Axialclient + ' - ' + FileDomain)
                        
                        sql3 = "select client_stage.hmkpa_medical_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.medical_claim_pre_filter(" + str(file_log_id) +") "
                        sql5 = "select client_stage.hmkpa_medical_claim_filter(" + str(file_log_id) +") "
                        sql6 = "select client_production.medical_claim_promotion(" + str(file_log_id) +") "
                        sql7 = "select client_production.medical_claim_diagnosis(3)"
                        sql8 = "select client_production.medical_claim_events_including_eol(3)"
                        sql9 = "select dataops_to_reporting.mat_adherence_days('hmkpa')"
                        sql10 = "select dataops.npi_member_relationship('hmkpa')"
                    
                    if Axialclient == 'hmkpa' and FileDomain == 'rx_claims':
                        ##Send notification when triggered
                        sns1.publish(TopicArn=topic_arn, Message='Started File load For - ' +  Axialclient + ' - ' + FileDomain ,Subject='Started File load For - ' +  Axialclient + ' - ' + FileDomain)
                        
                        sql3 = "select client_stage.hmkpa_rx_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.rx_claim_pre_filter(" + str(file_log_id) +") "
                        sql5 = "select client_stage.hmkpa_rx_claim_filter(" + str(file_log_id) +") "
                        sql6 = "select client_production.rx_claim_promotion(" + str(file_log_id) +") "
                        sql7 = "select client_production.rx_claim_event(3)"
                    
                    #Added 03/07/2022 for hmkpa eligibility for covered_individual
                    if Axialclient == 'hmkpa' and str(master_file_detail_sftp_id) == '97':
                        sql2 = "select client_import.hmkpa_member_pre_stage(" + str(file_log_id) +")"
                        sql3 = "select client_process.import_keys_to_stage_plus(" + str(file_log_id) +",95,3) "
                        print(sql3)
                        sql4 = "select client_stage.hmkpa_member_stage(" + str(file_log_id) +") "
                        sql5 = "select client_production.import_stage_to_prod(3, 'member') "
                        sql6 = "select client_production.import_stage_to_prod(3, 'member_plan')"
                        sql7 = "select client_production.member_month_by_lob(3)"
                    
                    if Axialclient == 'hmkwv' and FileDomain == 'medical_claims':
                       
                        sql3 = "select client_stage.hmkwv_medical_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.medical_claim_pre_filter(" + str(file_log_id) +") "
                        sql5 = "select client_stage.hmkwv_medical_claim_filter(" + str(file_log_id) +") "
                        sql6 = "select client_production.medical_claim_promotion(" + str(file_log_id) +") "
                        sql7 = "select client_production.medical_claim_diagnosis(4)"
                        sql8 = "select client_production.medical_claim_events_including_eol(4)"
                        sql9 = "select dataops_to_reporting.mat_adherence_days('hmkwv')"
                        sql10 = "select dataops.npi_member_relationship('hmkwv')"
                        
                    
                    if Axialclient == 'hmkwv' and FileDomain == 'rx_claims':
                       
                        sql3 = "select client_stage.hmkwv_rx_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.rx_claim_pre_filter(" + str(file_log_id) +") "
                        sql5 = "select client_stage.hmkwv_rx_claim_filter(" + str(file_log_id) +") "
                        sql6 = "select client_production.rx_claim_promotion(" + str(file_log_id) +") "
                        sql7 = "select client_production.rx_claim_event(4)"
                    
                    #Added 03/07/2022 for hmkwv eligibility for covered_individual
                    if Axialclient == 'hmkwv' and str(master_file_detail_sftp_id) == '104':
                        sql2 = "select client_import.hmkwv_member_pre_stage(" + str(file_log_id) +")"
                        sql3 = "select client_process.import_keys_to_stage_plus(" + str(file_log_id) +",96,4) "
                        print(sql3)
                        sql4 = "select client_stage.hmkwv_member_stage(" + str(file_log_id) +") "
                        sql5 = "select client_production.import_stage_to_prod(4, 'member') "
                        sql6 = "select client_production.import_stage_to_prod(4, 'member_plan')"
                        sql7 = "select client_production.member_month_by_lob(4)"
                    
                    #csin   
                    if Axialclient == 'csin' and FileDomain == 'medical_claims':
                        sql3 = "select  client_stage.csin_medical_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.medical_claim_pre_filter(" + str(file_log_id) +")"
                        sql5 = "select client_stage.csin_medical_claim_filter(" + str(file_log_id) +")"
                        sql6 = "select client_production.medical_claim_promotion(" + str(file_log_id) +")"
                        sql7 = "select client_production.medical_claim_diagnosis(19)"
                        sql8 = "select client_production.medical_claim_events_including_eol(19)"
                        sql9 = "select dataops_to_reporting.mat_adherence_days('csin')"
                        sql10 = "select dataops.npi_member_relationship('csin')"
                        sql11 = "select client_production.caresource_inclusion_exclusion_fill()"
                        #Added 5/13 rebecca request
                        sql12 = "CALL client_production.csin_claim_transactions_fill()"
                        
                        
                        
                    if Axialclient == 'csin' and FileDomain == 'eligibility':
                        sql3 = "select client_stage.csin_member_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.import_stage_to_prod(19, 'member') "
                        sql5 = "select client_production.import_stage_to_prod(19, 'member_plan') "
                        sql6 = "select client_production.member_month_by_lob(19)"
                        
                        
                        
                    if Axialclient == 'csin' and FileDomain == 'rx_claims':
                        sql3 = "select client_stage.csin_rx_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.rx_claim_pre_filter(" + str(file_log_id) +") "
                        sql5 = "select client_stage.csin_rx_claim_filter(" + str(file_log_id) +") "
                        sql6 = "select client_production.rx_claim_promotion(" + str(file_log_id) +") "
                        sql7 = "select client_production.rx_claim_event(19)"
                    if Axialclient == 'csin' and FileDomain == 'provider_identity':
                        sql3 = "select  client_stage.csin_provider_record_stage(" + str(file_log_id) +")"
                        sql4 = "select  client_production.import_stage_to_prod(19, 'provider_record') "
                    


                    if Axialclient == 'hmkde' and FileDomain == 'medical_claims':
                        sql3 = "select  client_stage.hmkde_medical_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.medical_claim_pre_filter(" + str(file_log_id) +")"
                        sql5 = "select client_stage.hmkde_medical_claim_filter(" + str(file_log_id) +")"
                        sql6 = "select client_production.medical_claim_promotion(" + str(file_log_id) +")"
                        sql7 = "select client_stage.hmkde_post_production_medical(" + str(file_log_id) +")"
                        sql8 = "select client_production.medical_claim_diagnosis(11)"
                        sql9 = "select client_production.medical_claim_events_including_eol(11)"
                        sql10 = "select dataops_to_reporting.mat_adherence_days('hmkde')"
                        sql11 = "select dataops.npi_member_relationship('hmkde')"
                        
                        
                    
                    
                    if Axialclient == 'hmkde' and FileDomain == 'rx_claims':
                       
                        sql3 = "select client_stage.hmkde_rx_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.rx_claim_pre_filter(" + str(file_log_id) +") "
                        sql5 = "select client_stage.hmkde_rx_claim_filter(" + str(file_log_id) +") "
                        sql6 = "select client_production.rx_claim_promotion(" + str(file_log_id) +") "
                        sql7 = "select client_production.rx_claim_event(11)"
                        
                    if Axialclient == 'hmkde' and FileDomain == 'eligibility':
                        sql3 = "select client_stage.hmkde_member_stage(" + str(file_log_id) +")"
                        
                    
                    
                    if Axialclient == 'hmkde_md' and FileDomain == 'medical_claims':
                        sql3 = "select  client_stage.hmkde_md_medical_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.medical_claim_pre_filter(" + str(file_log_id) +")"
                        sql5 = "select client_stage.hmkde_md_medical_claim_filter(" + str(file_log_id) +")"
                        sql6 = "select client_production.medical_claim_promotion(" + str(file_log_id) +")"
                        sql7 = "select client_stage.hmkde_md_post_production_medical(" + str(file_log_id) +")"
                        sql8 = "select client_production.medical_claim_diagnosis(20)"
                        sql9 = "select client_production.medical_claim_events_including_eol(20)"
                        sql10 = "select client_production.hmkde_md_inclusion_exclusion_fill()"
                        sql11 = "select dataops_to_reporting.mat_adherence_days('hmkde_md')"
                        sql12 = "select dataops.npi_member_relationship('hmkde_md')"
                    
                    if Axialclient == 'hmkde_md' and FileDomain == 'rx_claims':
                       
                        sql3 = "select client_stage.hmkde_md_rx_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.rx_claim_pre_filter(" + str(file_log_id) +") "
                        sql5 = "select client_stage.hmkde_md_rx_claim_filter(" + str(file_log_id) +") "
                        sql6 = "select client_production.rx_claim_promotion(" + str(file_log_id) +") "
                        sql7 = "select client_production.rx_claim_event(20)"
                        
                   
                    if Axialclient == 'hmkde_md' and FileDomain == 'prior_auth':
                        sql3 = "select client_stage.hmkde_md_prior_auth_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.import_stage_to_prod(20, 'prior_authorization')"
                        
                    
                    #Added AZCH 02-25-2022                        
                    if Axialclient == 'azch' and FileDomain == 'rx_claims':
                        sql3 = "select client_stage.azch_rx_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.rx_claim_pre_filter(" + str(file_log_id) +") "
                        sql5 = "select client_stage.azch_rx_claim_filter(" + str(file_log_id) +") "
                        sql6 = "select client_production.rx_claim_promotion(" + str(file_log_id) +") "
                        sql7 = "select client_production.rx_claim_event(18)"
            
                    if Axialclient == 'azch' and FileDomain == 'eligibility':
                        #Marketplace
                        if str(master_file_detail_sftp_id) == '316': 
                            sql3 = "select client_stage.azch_member_stage_truncate(" + str(file_log_id) +")"
                        
                        #Medicaid
                        elif str(master_file_detail_sftp_id) == '315':
                            sql3 = "select client_stage.azch_member_stage_no_truncate(" + str(file_log_id) +")"
                            sql4 = "select client_production.import_stage_to_prod(18, 'member') "
                            sql5 = "select client_production.import_stage_to_prod(18, 'member_plan') "
                            sql6 = "select client_production.member_month_by_lob(18)" 
                        
                    print('master_file_detail_sftp_id - ' + str(master_file_detail_sftp_id))
                    
                    #AzCH_Marketplace_Medical_Claims
                    if Axialclient == 'azch' and FileDomain == 'medical_claims' and str(master_file_detail_sftp_id) == '284':
                        print('AzCH_Marketplace_Medical_Claims') 
                        sql3 = "select  client_stage.azch_medical_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.medical_claim_pre_filter(" + str(file_log_id) +")"
                        sql5 = "select client_stage.azch_medical_claim_filter(" + str(file_log_id) +")"
                        sql6 = "select client_production.medical_claim_promotion(" + str(file_log_id) +")"

                    #AzCH_Medicaid_Medical_Claims
                    if Axialclient == 'azch' and FileDomain == 'medical_claims' and str(master_file_detail_sftp_id) == '285':
                        print('AzCH_Medicaid_Medical_Claims')
                        sql3 = "select  client_stage.azch_medical_claim_stage(" + str(file_log_id) +")"
                        sql4 = "select client_production.medical_claim_pre_filter(" + str(file_log_id) +")"
                        sql5 = "select client_stage.azch_medical_claim_filter(" + str(file_log_id) +")"
                        sql6 = "select client_production.medical_claim_promotion(" + str(file_log_id) +")"    
                        sql7 = "select client_production.medical_claim_diagnosis(18)"
                        sql8 = "select client_production.medical_claim_events_including_eol(18)"
                        sql9 = "select dataops_to_reporting.mat_adherence_days('azch')"
                        sql10 = "select dataops.npi_member_relationship('azch')"

                    if sql1 != '':
                        #print(sql1)
                        cur.execute(sql1)
                        rec1 = cur.fetchone()
                        raw_rec_count = int(rec1[0])
                        con.commit()
                        print('Raw_rec_count ' + str(raw_rec_count))
                    
                    ## Do not execute rest of the functions if no records in file or it errors out
                    if raw_rec_count > 0:
                        print('Records found in file')
                        if sql2 != '':
                            #print(sql2)
                            cur.execute(sql2)
                            rec2 = cur.fetchone()
                            con.commit()
                        
                        if sql3 != '':
                            #con.commit()
                            #print(sql3)
                            cur.execute(sql3)
                            rec3 = cur.fetchone()
                            con.commit()
                            
                            if Axialclient == 'hmkpa' and FileDomain == 'rx_claims':
                                #Trigger hmkpa Medical claims after client_process.import_keys_to_stage for RX
                                #print(run_glue_job("Client_Files_Load",{'--master_file_detail_sftp_id': '200'}))
                                print(run_glue_job("Client_Files_Load",{'--master_file_detail_sftp_id': '66'})) 
                        
                        if sql4 != '':
                            #print(sql4)
                            cur.execute(sql4)
                            rec4 = cur.fetchone()
                            con.commit()
                        
                        if sql5 != '':
                            #print(sql5)
                            cur.execute(sql5)
                            rec5 = cur.fetchone()
                            con.commit()
                        
                        if sql6 != '':
                            #print(sql6)
                            cur.execute(sql6)
                            rec6 = cur.fetchone()
                            con.commit()
                        
                        if Axialclient == 'csin' and FileDomain == 'eligibility':
                            ## Execute SFTP Uplpoad Caresource_Eligibility_Reconciliation file
                            print(run_glue_job("SFTPUpload",{'--master_outbound_file_detail_id': '5'}))
                            
                        
                        if sql7 != '':
                            #print(sql7)
                            cur.execute(sql7)
                            rec7 = cur.fetchone()
                            con.commit()
                        
                        if sql8 != '':
                            #print(sql7)
                            cur.execute(sql8)
                            rec8 = cur.fetchone()
                            con.commit()
                        
                        if sql9 != '':
                            #print(sql7)
                            cur.execute(sql9)
                            rec9 = ''#cur.fetchone()
                            con.commit()
                        
                        if sql10 != '':
                            #print(sql7)
                            cur.execute(sql10)
                            rec10 = ''#cur.fetchone()
                            con.commit()
                        
                        if sql11 != '':
                            #print(sql7)
                            cur.execute(sql11)
                            rec11 = ''#cur.fetchone()
                            con.commit()
                        
                        if sql12 != '':
                            #print(sql12)
                            cur.execute(sql12)
                            rec12 = ''#cur.fetchone()
                            con.commit()
                        
                        #Added 03-31-2022
                        if Axialclient == 'csin' and FileDomain == 'medical_claims':
                            ## Execute CARESOURCE_MEMBER_INCLUSION_EXCLUSION_TABLE_LOAD
                            print(run_glue_job("CARESOURCE_MEMBER_INCLUSION_EXCLUSION_TABLE_LOAD"))
                        
                    else:
                        print('No records in file')
                        
                        if raw_rec_count == -1:
                            err_message =  'Blank File Received '
                        elif raw_rec_count == -2:
                             err_message =  'Bad Format or Wrong rec cnt For '
                        elif raw_rec_count == -3:
                             err_message =  'Bad Hist or Wrong rec cnt For '
                        
                    
                    #Remove file from Axial SFTP and client sftp after load
                    sftp_delete_file(site1, port1, username1, pwsecret1,AxialSFTPPath  + '/' + finalFileName)
                    if P_SecretName == 'S3':
                        s3 = boto3.resource('s3')
                        s3.Object(S3Bucket, Sourcefolder +'From_Remote_Desktop/'+finalFileName).delete()

                    
                    #for last 5 file loads for same client and file domain.
                    sqlr = "select distinct f.file_log_id ,file_name,f.received_timestamp,process_total_rows "\
                    "from client_process.file_log f left join client_process.file_process_log p on f.file_log_id = p.file_log_id "\
                    "where master_file_detail_id = "  + str(master_file_detail_id) + " and process_name = 'file_import' order by 1 desc limit 5";
                    
                    cur.execute(sqlr)
                    col_names = [x.name for x in cur.description]
                    df = pd.DataFrame(cur.fetchall(), columns=col_names)
                    x = df.to_string(index=False,header=False,decimal='\t')
                    
                    if (con):
                        cur.close()
                        con.close()
                    
                    
                    line_breaker = '-'*150

                    if raw_rec_count > 0:
                        
                        
                        ###adding in 
                        command_dic =  {'1':[sql1,rec1],
                                         '2':[sql2,rec2],
                                         '3':[sql3,rec3],
                                         '4':[sql4,rec4],
                                         '5':[sql5,rec5],
                                         '6':[sql6,rec6],
                                         '7':[sql7,rec7],
                                         '8':[sql8,rec8],
                                         '9':[sql9,rec9],
                                         '10':[sql10,rec10],
                                         '11':[sql11,rec11],
                                         '12':[sql12,rec12]}
                                         
                        emailBody = f"{str(emailBody)}  \n {line_breaker} \n {str(file_log_id)} | {str(file_name)} \n {line_breaker} \n"
                        for command in  command_dic:
                            #Start out each email the same
                            sql = command_dic[command][0]
                            rec = command_dic[command][1]
                            if sql != '':
                                appendBody = f"{sql}  \n  {rec}  \n  \n"
                                emailBody = emailBody + appendBody
                        endBody = f"{line_breaker} \n {x} \n {line_breaker}"
                        emailBody = emailBody + endBody
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
                 
                else:
                    print('File name mismatch: '+ finalFileName + '!= ' + file_name)
                    emailBody = 'File doesnt match most recent file log: '+ finalFileName + '!= ' + file_name
                    raw_rec_count = -4
                    err_message = 'File name mismatch '
           
        if found != 'Yes':
            print("No matching file found with search pattern %s" % filenameLike)
            emailBody = "No matching file found with search pattern %s" % filenameLike
            raw_rec_count = -4
            err_message = 'No matching file found '
            
        else:       
            print("done with File Load")
    
    else:
        print("No configuration found for master_file_detail_sftp_id - %s" % master_file_detail_sftp_id)
        Axialclient = ''
        FileDomain = ''
        emailBody = "No configuration found for master_file_detail_sftp_id - %s" % master_file_detail_sftp_id
        raw_rec_count = -4
        err_message = 'No configuration found '
    
    
    
    # construct the email message
    email_dict={}
    if raw_rec_count > 0:
        email_dict['sub'] = 'File load For - ' +  Axialclient + ' - ' + FileDomain + ' - ' + str(master_file_detail_sftp_id)
    else:
        email_dict['sub'] =  err_message + 'For - ' +  Axialclient + ' - ' + FileDomain + ' - ' + str(master_file_detail_sftp_id)

    email_body = emailBody
    response = sns1.publish(TopicArn=topic_arn, Message=email_body,Subject=email_dict['sub'])
        
except Exception as e:
    print("Failed on exception", repr(e))
    raise
        



