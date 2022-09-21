-----COPYING/ALTERING DATA FROM ONE DATABASE TO ANOTHER




---copying tables from ods to redshfit 
1 - ssh jmackinnon@10.10.5.31

2 - psql axial_ods -c "\copy <table_name> TO <local_dir> DELIMITER '|' CSV HEADER; "

3- Upload to s3
aws s3 cp <local_dir> <s3_bucket>

4- Enter into Redshift
psql -h 10.10.4.205 -U jmackinnon -d axial_research -p 5439
<enter password>

5- Copy from s3_bucket to redshift table

COPY <redshift_table> FROM 's3://axialhealthcare-knox-etl-raw-dev/redshift/<file_name>' iam_role 'arn:aws:iam::877582243023:role/Redshift_role' csv ignoreheader 1 delimiter '|';



---copying tables from ods to exasol 
--Backup
CREATE TABLE ZZ_JMM.COTIVITI_MODEL_RESULTS_03152022 AS SELECT * FROM COTIVITI.COTIVITI_MODEL_RESULTS; ---15030428

--checking ROW count of prod table
SELECT count(*) FROM COTIVITI.COTIVITI_MODEL_RESULTS; ---15030428

--checking ROW count of ods table 
select count(*) from cotiviti.exasol_csin_v6; ---500018

--- insert into 
import into COTIVITI.COTIVITI_MODEL_RESULTS from jdbc at POSTGRES_ODS statement 'select 98 client_id, TRIM(client_name)client_name, time_frame, CAST(model_number as float) model_number, cast(run_date as date) run_date, axial_member_id, eligible_months, medical_spend, rx_spend, cast(predictive_value as float) predictive_value, dcg_value, adcg_value, adcg_risk_level, riskdriver, hcc_1, cast(percent_1 as float) precent_1, hcc_2, cast(percent_2 as float) precent_2, hcc_3, cast(percent_3 as float) precent_3, hcc_4, cast(percent_4 as float) precent_4, hcc_5, cast(percent_5 as float) precent_5, model_number_confirmation
from cotiviti.exasol_csin_v6';

-- Checking ROW count after insert 
SELECT count(*) FROM COTIVITI.COTIVITI_MODEL_RESULTS --15530446 ( 15030428 + 500018 = 15530446 )
--Cool query to help 
execute script database_migration.POSTGRES_TO_EXASOL(

    'postgres_ods', -- name of your database connection

    true,          -- case sensitivity handling for identifiers -> false: handle them case sensitiv / true: handle them case insensitiv --> recommended: true

    'user_btschirpke%',           -- schema filter --> '%' to load all schemas except 'information_schema' and 'pg_catalog' / '%publ%' to load all schemas like '%pub%'

    'sow_all_clients_all_codes%'            -- table filter --> '%' to load all tables

);


---Copying tables from exasol to redshfit

1 - export table to local 
ssh jmackinnon@10.4.6.170
/usr/opt/EXASuite-7/EXASolution-7.0.4/bin/Console/exaplus -u jmackinnon -c localhost:8563

EXPORT OA_CNTIN.MAT_ADHERENCE_DAYS INTO LOCAL CSV FILE '/home/jmackinnon/imports/OA_CNTIN_MAT_ADHERENCE_DAYS_2022-04-25.csv' COLUMN SEPARATOR = '|' WITH COLUMN NAMES;

2 - sftp to ods 
sftp dataops@10.10.5.31
put *.csv 

3 - put data from ods to s3_bucket  
aws s3 cp <ods_dir> <s3_bucket>

4 - log into redshfit and copy data into tables 
ssh jmackinnon@10.10.5.31
psql -h 10.10.4.205 -U jmackinnon -d axial_research -p 5439
<enter password>
COPY <redshift_table> FROM 's3://axialhealthcare-knox-etl-raw-dev/redshift/<file_name>' iam_role 'arn:aws:iam::877582243023:role/Redshift_role' csv ignoreheader 1 delimiter '|';






-----Exasol to ods
1 - export table to local 
ssh jmackinnon@10.4.6.170
/usr/opt/EXASuite-7/EXASolution-7.0.4/bin/Console/exaplus -u jmackinnon -c localhost:8563
EXPORT SELECT * FROM ZZ_DBM.SUD_HOME_OUTREACH_COHORTS_V2 where CLIENT_NAME = 'agptn' or (CLIENT_NAME = 'csin' and PRIORITIZATION_RUN_DATE = '2022-04-26')  INTO LOCAL CSV FILE '/home/jmackinnon/imports/csin_ods_sud_home04272022.csv' COLUMN SEPARATOR = '|' WITH COLUMN NAMES;

2 - sftp to ods 
sftp dataops@10.10.5.31
put <file_name.csv>

3 - log into ods and import
ssh jmackinnon@10.10.5.31 
psql axial_ods -c "\copy <target_table_name> FROM <location_on_ods> DELIMITER '|' CSV HEADER; "
