---- Milliman file extract
---- Create tables on exasol personal schema (zz_jmm)

Create table zz_jmm."clientname"+"Tablename"

----- run these codes to replace tables with updated data

create OR REPLACE table zz_jmm.hmkde_mst_medical_claims as
SELECT * FROM
master.MST_MEDICAL_CLAIMS
WHERE client_id = 20

CREATE OR REPLACE TABLE ZZ_JMM.HMKDE_MST_RX_CLAIMS AS 
SELECT * FROM
master.MST_RX_CLAIMS
WHERE client_id = 20

CREATE OR REPLACE TABLE ZZ_JMM.HMKDE_MST_MEMBER_MONTH_BY_LOB AS 
SELECT * FROM
master.MST_MEMBER_MONTH_BY_LOB
WHERE client_id = 20

CREATE OR REPLACE TABLE ZZ_JMM.HMKDE_MST_PROVIDER_DIM AS 
SELECT * FROM
master.MST_PROVIDER_DIM
WHERE client_id = 20


CREATE OR REPLACE TABLE ZZ_JMM.HMKDE_MST_MEMBER_DIM  AS 
SELECT
DISTINCT
a.AXIAL_MEMBER_ID ,
a.MEMBER_DATE_OF_BIRTH ,
b.MEMBER_ZIP ,
a.MEMBER_RACE ,
a.MEMBER_GENDER
FROM
MASTER.MEMBER_DIM a
LEFT JOIN
MASTER.MEMBER_ADDRESS_DIM b
ON
a.axial_member_id = b.AXIAL_MEMBER_ID
WHERE
a.client_id = 20

CREATE OR REPLACE TABLE ZZ_JMM.HMKDE_MEDICAL_CLAIM_DX_LONG AS 
SELECT * FROM
master.MEDICAL_CLAIM_DX_LONG
WHERE CLIENT_ID = 20


---- Once tables are created on exasol box run commands 

1.) log onto exasol box
2.) ssh jmackinnon@10.4.6.170
3.) enter password
4.) screen or old screen (-r -d <screen number>) or (screen -ls)
5.) to get out of screen cntrl 'a' 'd' brings back to command prompt
6.) to enter database enter 
		'/usr/opt/EXASuite-7/EXASolution-7.0.4/bin/Console/exaplus -u jmackinnon -c localhost:8563'
		enter password 
	
		sql commands
7.) Make sure you have a filepath created to store files
	ls -l (list of folders)
	mkdir (make directory)
	cd (change directory)
	pwd(current directory)
	lls -l (local directory)

7.) outside database head -5 <pathname> to see the file
8.) once all files create wc -l *.txt to check line numebrs 
	ls -alh 

EXPORT ZZ_JMM.HMKDE_MST_MEMBER_DIM INTO LOCAL CSV FILE '/home/jmackinnon/milliman/Updated_HMKDE_MST_MEMBER_DIM_20220310.txt' COLUMN SEPARATOR = '|' WITH COLUMN NAMES;

---- Connecting to sftp 
1.)  sftp milliman@transfer.axialhealthcare.com
2.) enter password
3.) get to correct directory ls -l and cd 
4.) 'put' + <filepath> will put files from local to sftp
	'put *.txt' will upload all the .txt files in directory
5.) 'get' + <filepath> will download files to local 
6.) 'exit' will get you out of sftp 
7.) need to be on ssh 



EXPORT ZZ_JMM.HMKDE_MST_MEDICAL_CLAIMS INTO LOCAL CSV FILE 'home/jmackinnon/milliman/HMKDE_MST_MEDICAL_CLAIMS.txt' COLUMN SEPARATOR = '|' WITH COLUMN NAMES;



----- ON ODS BOX 

 psql axial_ods -c "\copy user_jmackinnon.hmkde_md_promise_members_02172022 TO '/home/jmackinnon/milliman/hmkde_md_promise_members_02172022.txt' DELIMITER '|' CSV HEADER; "



 --- RUNS 

 ---Checking count of rebeccas table
select count(*) from  user_rnateqi.hmkde_medicaid_members_with_aid_category ---7262343

---Creating backup in jacobs schema
create table user_jmackinnon.hmkde_medicaid_members_with_aid_category_02232022 as 
select * from user_rnateqi.hmkde_medicaid_members_with_aid_category ---7262343

---Copying table to ods for transfer
 psql axial_ods -c "\copy user_jmackinnon.hmkde_medicaid_members_with_aid_category_02232022 TO '/home/jmackinnon/milliman/hmkde_medicaid_members_with_aid_category_02232022.txt' DELIMITER '|' CSV HEADER; "
---Output: 7262343

 psql axial_ods -c "\copy user_jmackinnon.hho_dds_members_3822 TO '/home/jmackinnon/milliman/hho_dds_members_3822.txt' DELIMITER '|' CSV HEADER; "
























----old


create table zz_jmm.hmkde_mst_medical_claims as
SELECT * FROM
master.MST_MEDICAL_CLAIMS
WHERE client_id = 20

SELECT * FROM
OA_CSIN.MST_RX_CLAIMS
WHERE client_id = 20

SELECT * FROM
OA_CSIN.MST_MEMBER_MONTH_BY_LOB
WHERE client_id = 20

SELECT * FROM
OA_CSIN.MST_PROVIDER_DIM
WHERE client_id = 20

SELECT
AXIAL_MEMBER_ID ,
MEMBER_DATE_OF_BIRTH ,
MEMBER_ZIP ,
MEMBER_RACE ,
MEMBER_GENDER
FROM
OA_CSIN.MST_MEMBER_DIM
WHERE client_id = 20

SELECT * FROM
OA_CSIN.MEDICAL_CLAIM_DX_LONG
WHERE CLIENT_ID = 20
