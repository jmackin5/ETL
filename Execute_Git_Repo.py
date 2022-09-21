import os
import subprocess
import pgdb
import boto3
import json
from awsglue.utils import getResolvedOptions

code_didnt_work = []



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


def executeScriptsFromFile(filename):
    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()
    #If we want to separate commands within a file use sqlCommands
    sqlCommands = sqlFile.split(';')
    #executing what is in the sql file
    cur.execute(f"""
                    {sqlFile}
                """)
    con.commit()


def read_script(filename):
    sc = open(filename, 'r')
    contents = sc.read()
    sc.close()
    return contents






##Puling GitRepo
path = '/tmp/scripts/bi-reporting/redshift/ops_model'

os.system('pwd')
os.system('mkdir scripts')
os.chdir('/tmp/scripts/')
os.system('git clone https://jmackinnon24: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@github.com/Axial-Healthcare/bi-reporting.git')
os.chdir(f'{path}')

# n = os.popen('ls').read().split('\n')
# print(n)

##For Dev Ops - Look for 'Run_Branch.sql' for commands on how to run the repo
int_com = read_script('Run_Branch_test.sql').split('\n')[1:-1]
#print(int_com)




bad_code = {}
for dirs in int_com:
    print(f'Executing {dirs} Directory:')
    path_to_new_dir = f'{path}/{dirs}'
    #changing directorys
    os.chdir(path_to_new_dir)
    os.system('pwd')
    try:
        #Each directory will have a 'Run_Dir.sql' that will list scripts in order 
        dir_commands = read_script('Run_Dir_test.sql').split('\n')[1:-1]
        #loop through scripts and execute
        for scripts in dir_commands:
            try:
                print(f'Executing {dirs}.{scripts}')
                executeScriptsFromFile(scripts)
            #If script didnt work, save error message for sns email
            except con.ProgrammingError as msg:
                bad_code[scripts] = [msg]
                con.rollback()
                print(f'WARNING: {dirs}.{scripts} Did Not Execute')
                    
                
    except:
        print('No Run_Dir.sql in Directory')

        
        
        
##Updating Process Tracker
#Set Path back to home
os.chdir(f'{path}')
os.system('pwd')
#File will be call 'PROCESS_UPDATE'
executeScriptsFromFile('PROCESS_TRACKER')
    

#Close connection 
cur.close()
con.close()



#Constructing email message if any files fail to execute
if len(bad_code) > 0:
    sns1 = boto3.client("sns")
    topic_arn = 'arn:aws:sns:us-east-1:877582243023:JobErrorAlert'
    
    list_string = " "
    for bad in bad_code:
        bad_table = bad
        reason    = bad_code[bad][0]
        list_string = list_string + f"[Table: {bad} ," + f"Error: {reason}]"
    
    # construct the email message
    email_dict={}
    email_dict['sub'] = 'Failed Git Execution: Redshift Ops Model'
    email_body = 'These Tables Didnt Load: ' + list_string
    
    print(email_body)
    response = sns1.publish(TopicArn=topic_arn, Message=email_body,Subject=email_dict['sub'])















