---Data Uploading Client_file_load

See if you have credentials to SFTP (only one we dont for is optum)

If we Have Credentials:
1.) Log into ods ssh 
    ssh jmackinnon@10.10.5.31
    enter password
    cd into the correct folder you want files going into <find the sftp path from master query>
    cd /pg_part1/<sftp path from master query>

2.) log into sftp 
    sftp <stfp url from lastpass>
        sftp username@sftp
    go to folder with files
    'get' + <filepath> will download files to local

3.) Once files are in correct folder on ods box
    run glue job with correct parameters
--Query to find master file lists
select master_file_detail_sftp_id,sftp_secretname,file_domain,client_name,s3_bucket,sftp_file_name_format,sftp_sourcefolder,s3_folder_path,axial_sftp_key,axial_sftp_path,file_name_format,file_encoding, s.master_file_detail_id
from client_process.master_file_detail fd inner join user_iindawala.master_file_detail_sftp s on fd.master_file_detail_id = s.master_file_detail_id
inner join client_process.master_client c on fd.client_id = c.client_id


chmod 664
chmod 775

If we DONT Have Credentials:
1.) Remote desktop into the corret sftp
        Cerebrus box 10.10.1.208
2.) Extract the files needed onto local remote desktop 
3.) Run command iqbal saved on remote desktop to put files on s3 
aws s3 cp D:/sftpusers/optum/from-optum/20220124/ s3://axialhealthcare-knox-etl-raw/client_files/uhctn/20220124/ --recursive --include "*/*" --exclude "*/*"
4.) Log into ods ssh 
    ssh jmackinnon@10.10.5.31
    enter password
    cd into the correct folder you want files going into 
    cd /pg_part1/client_
5.) Run the command to bring files from s3 onto ods box
aws s3 cp s3://axialhealthcare-knox-etl-raw/client_files/uhctn/20220124/. --recursive
    "aws s3 cp <copy path from s3> <filepath on ods box>"


6.) Once files are in correct folder on ods box
    run glue job with correct parameters
--Query to find master file lists
select master_file_detail_sftp_id,sftp_secretname,file_domain,client_name,s3_bucket,sftp_file_name_format,sftp_sourcefolder,s3_folder_path,axial_sftp_key,axial_sftp_path,file_name_format,file_encoding, s.master_file_detail_id
from client_process.master_file_detail fd inner join user_iindawala.master_file_detail_sftp s on fd.master_file_detail_id = s.master_file_detail_id
inner join client_process.master_client c on fd.client_id = c.client_id



Helpful Commands:

cd -directory
cd .. - last directory
head -1 <filepath> to get the header of a file. 
wc -l *.txt to check line numebrs



---- FOR HMKPA/HMKDE files that need to be parsed 

gzip to unzup .gz 

hmk_provider_to_csv \
--context_param client_state_abbrv=WV \
--context_param file_directory=/data/clients/hmkwv/ \
--context_param date_NowYYYYMMDD=20220210 \
--context_param provider_identity_file=AXL_provider_demographics_affiliation_full_20220210.txt



hmk_provider_to_csv \
--context_param client_state_abbrv=PA \
--context_param file_directory=/data/clients/hmkpa/ \
--context_param date_NowYYYYMMDD=20220210 \
--context_param provider_identity_file=XPA_provider_demographics_affiliation_full_20220210.txt


hmk_membership_to_csv \
--context_param client_state_abbrv=PA \
--context_param file_directory=/data/clients/hmkpa/ \
--context_param date_NowYYYYMMDD=20220122

hmk_membership_to_csv \
--context_param client_state_abbrv=PA \
--context_param file_directory=/data/clients/hmkpa/ \
--context_param date_NowYYYYMMDD=20220226

hmk_membership_to_csv \
--context_param client_state_abbrv=WV \
--context_param file_directory=/data/clients/hmkwv/ \
--context_param date_NowYYYYMMDD=20220122

hmk_membership_to_csv \
--context_param client_state_abbrv=WV \
--context_param file_directory=/data/clients/hmkwv/ \
--context_param date_NowYYYYMMDD=20220226


---------------------- Changing config in main table 

insert into user_iindawala.master_file_detail_sftp(master_file_detail_id,sftp_file_name_format,sftp_secretname,sftp_sourcefolder,
s3_bucket,s3_folder_path,axial_sftp_key,axial_sftp_path)
select master_file_detail_id,replace(replace(file_name_format,'%','*'),'.csv','.zip') sftp_file_name_format, 'DataOps-SFTP-Highmark' sftp_secretname,'outbound/prod/membership/' sftp_sourcefolder,
'axialhealthcare-knox-etl-raw' s3_bucket,'client_files/'|| c.client_name||'/' s3_folder_path,'DataOps-SFTP-Axial-Ods' axial_sftp_key,'/client_files/'|| c.client_name||'/'|| file_domain||'/' axial_sftp_path
--has_header,client_id,has_footer,footer_field_delimiter,footer_record_count_position,file_delimiter,file_quote_char,null_string,client_id,client_name,shakey,legacy_client_db,legacy_client_slug,legacy_reporting_db,legacy_client_id,is_active_production,effective_date,termination_date
from client_process.master_file_detail fd
--inner join user_iindawala.master_file_detail_sftp s on fd.master_file_detail_id = s.master_file_detail_id
inner join client_process.master_client c on fd.client_id = c.client_id
where fd.client_id in (20) and fd.master_file_detail_id in (207);



------ REmoving last line, For Weekend hmkpa medical file 
sed '$d' <file>



--------- to check where monthly loads are 

select * from pg_stat_activity
where username in ()

---enter pid
SELECT pg_terminate_backend(18439);


------ To start ods processes for files go on ods screen and run commands like this. 

psql axial_ods -c "select risk_score_processing.risk_score()"

psql axial_ods -c "select client_production.caresource_inclusion_exclusion_fill()"


---------optum medical file too large
----need to get shasum of file too
psql axial_ods -c "select client_process.insert_file_log('uhctn_di','medical_claims','axial_medical_claims_deidentified_20220302.txt','96ec4242d374099ba32320ee0fb0acaffdadb540')"
                                                                                                                                        96ec4242d374099ba32320ee0fb0acaffdadb540
                                                                                                                                     6ff1d47e5d60a4c9316c70dc476c22a2bc8e5f4e
psql axial_ods -c "select client_process.insert_file_log('uhctn_di','medical_claims','axial_medical_claims_deidentified_20220302.txt','6ff1d47e5d60a4c9316c70dc476c22a2bc8e5f4e')"
psql axial_ods -c 'select client_process.import_files_to_raw(8926)'
psql axial_ods -c 'select client_process.import_files_to_raw(9143)'

psql axial_ods -c 'select client_process.import_files_to_raw(9441)'


9441
9143




------------ file headers arent the same 

select * from client_process.master_file_import
where master_file_detail_id = 120
order by 



---psql axial_ods -c "select client_process.insert_file_log('oa_cent','census','Centene_SUD_Data_extract_census_20211201_20211221.txt','bbec9199c258644064368b5c077ce9f95e026370')"

---psql axial_ods -c "select client_process.insert_file_log('oa_cent','medical_claims','Centene_SUD_Data_extract_medical_20211201_20211231.txt','8329e75a8396f1c5c31f78914bc6d4d7c545af72')"

---psql axial_ods -c "select client_process.insert_file_log('oa_cent','eligibility','Centene_SUD_Data_extract_member_20211201_20211231.txt','25a58a0f52a12517133dad23c2b473ace297747e')"

--psql axial_ods -c "select client_process.insert_file_log('oa_cent','provider_identity','Centene_SUD_Data_extract_provider_20211201_20211231.txt','eb2499ad495eecf6c72c98222df7c7626383bc1a')"

---psql axial_ods -c "select client_process.insert_file_log('oa_cent','rx_claims','Centene_SUD_Data_extract_rx_20211201_20211231.txt','69c02a4370e81a9a1403e3ab87ee613c446e2253')"


psql axial_ods -c "select client_process.insert_file_log('hmkde_md','rx_claims','DRUG_HIGHMARK_MEDICAID_weekly_updated_03-19-2022.txt','61c66b2af15ada4d86901b3f2590a09779181e4b')"
psql axial_ods -c 'select client_process.import_files_to_raw(9143)'




------ centene oa load 

---census
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','census','Centene_SUD_Data_extract_census_20190301_20200229.txt','f27c72b7bb0320012299e1ae2719ed1d3738b7b7')"
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','census','Centene_SUD_Data_extract_census_20200301_20210228.txt','f1d090930801727eafdb3d00e3a5606908dcafa6')"
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','census','Centene_SUD_Data_extract_census_20210301_20220228.txt','439979060df477c24e75ab77057daef1a2a03ba2')"
Centene_SUD_Data_extract_census_20190301_20200229.txt f27c72b7bb0320012299e1ae2719ed1d3738b7b7
Centene_SUD_Data_extract_census_20200301_20210228.txt f1d090930801727eafdb3d00e3a5606908dcafa6
Centene_SUD_Data_extract_census_20210301_20220228.txt 439979060df477c24e75ab77057daef1a2a03ba2
9307
9306
9305
psql axial_ods -c 'select client_process.import_files_to_raw(9307)' 42794
psql axial_ods -c 'select client_process.import_files_to_raw(9306)' 39530
psql axial_ods -c 'select client_process.import_files_to_raw(9305)' 39197


---medical
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','medical_claims','Centene_SUD_Data_extract_medical_20190301_20200229.txt','91b3724f3def2c8333294ba87e7c29dd41f8bb15')"
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','medical_claims','Centene_SUD_Data_extract_medical_20200301_20210228.txt','bf3c7f50fcfa3ee55c3c42e298175c7208b8dd58')"
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','medical_claims','Centene_SUD_Data_extract_medical_20210301_20220228.txt','7c66a51e3671852c8c7e84c9b0f63cd628a58e22')"
91b3724f3def2c8333294ba87e7c29dd41f8bb15  Centene_SUD_Data_extract_medical_20190301_20200229.txt
bf3c7f50fcfa3ee55c3c42e298175c7208b8dd58  Centene_SUD_Data_extract_medical_20200301_20210228.txt
7c66a51e3671852c8c7e84c9b0f63cd628a58e22  Centene_SUD_Data_extract_medical_20210301_20220228.txt
9321
9320
9319
psql axial_ods -c 'select client_process.import_files_to_raw(9321)' 12451330
psql axial_ods -c 'select client_process.import_files_to_raw(9320)' 9802710
psql axial_ods -c 'select client_process.import_files_to_raw(9319)' 9940600


---member 
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','eligibility','Centene_SUD_Data_extract_member_20190301_20200229.txt','eb327782784cee15f6e99a646957ef4f5995aab5')"
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','eligibility','Centene_SUD_Data_extract_member_20200301_20210228.txt','9005d521882ef1cc75b6b7378117da2a717bb6f9')"
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','eligibility','Centene_SUD_Data_extract_member_20210301_20220228.txt','2ba3c9c6e22ea015f75a308a575b9fb178548a71')"
eb327782784cee15f6e99a646957ef4f5995aab5  Centene_SUD_Data_extract_member_20190301_20200229.txt
9005d521882ef1cc75b6b7378117da2a717bb6f9  Centene_SUD_Data_extract_member_20200301_20210228.txt
2ba3c9c6e22ea015f75a308a575b9fb178548a71  Centene_SUD_Data_extract_member_20210301_20220228.txt
9311
9310
9309
psql axial_ods -c 'select client_process.import_files_to_raw(9311)' 1492896
psql axial_ods -c 'select client_process.import_files_to_raw(9310)' 1308175
psql axial_ods -c 'select client_process.import_files_to_raw(9309)' 1460885


---provider (bad)
--psql axial_ods -c "select client_process.insert_file_log('oa_cent','provider_identity','Centene_SUD_Data_extract_provider_20190301_20200229.txt','450615faa64647c179e7ae6b0fd84e75eda002f4')"
--psql axial_ods -c "select client_process.insert_file_log('oa_cent','provider_identity','Centene_SUD_Data_extract_provider_20200301_20210228.txt','b36d9da9c4b347dcec4776c3bffb020ebab0d003')"
--psql axial_ods -c "select client_process.insert_file_log('oa_cent','provider_identity','Centene_SUD_Data_extract_provider_20210301_20220228.txt','cf6cc85301a9e52f9086e4bf1341f77360b01ca7')"
450615faa64647c179e7ae6b0fd84e75eda002f4  Centene_SUD_Data_extract_provider_20190301_20200229.txt
b36d9da9c4b347dcec4776c3bffb020ebab0d003  Centene_SUD_Data_extract_provider_20200301_20210228.txt
cf6cc85301a9e52f9086e4bf1341f77360b01ca7  Centene_SUD_Data_extract_provider_20210301_20220228.txt
9314
9313
9312
psql axial_ods -c 'select client_process.import_files_to_raw(9314)'
psql axial_ods -c 'select client_process.import_files_to_raw(9313)'
psql axial_ods -c 'select client_process.import_files_to_raw(9312)'


---rx
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','rx_claims','Centene_SUD_Data_extract_rx_20190301_20200229.txt','74eac0efc3eeebd754bba1a117802920aa834ac0')"
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','rx_claims','Centene_SUD_Data_extract_rx_20200301_20210228.txt','81e87d2cda64c248be4cd3254d62cbc863fb7e01')"
---psql axial_ods -c "select client_process.insert_file_log('oa_cent','rx_claims','Centene_SUD_Data_extract_rx_20210301_20220228.txt','81131091df487a8487507052f1d127e23febcdcd')"
74eac0efc3eeebd754bba1a117802920aa834ac0  Centene_SUD_Data_extract_rx_20190301_20200229.txt
81e87d2cda64c248be4cd3254d62cbc863fb7e01  Centene_SUD_Data_extract_rx_20200301_20210228.txt
81131091df487a8487507052f1d127e23febcdcd  Centene_SUD_Data_extract_rx_20210301_20220228.txt
9317
9316
9315
psql axial_ods -c 'select client_process.import_files_to_raw(9317)' 4899052
psql axial_ods -c 'select client_process.import_files_to_raw(9316)' 4219019
psql axial_ods -c 'select client_process.import_files_to_raw(9315)' 4235022





------gateway pa
---med_claims
psql axial_ods -c "select client_process.insert_file_log('oa_gtwpa','medical_claims','Gateway_Medical_Claims.txt','912547c96dde2c4e6935e5fc2ed8a483e6be9e45')"
---eligibility
psql axial_ods -c "select client_process.insert_file_log('oa_gtwpa','eligibility','Gateway_Member_Eligibility.txt','3084c783e421531f91d89a6e7db1122f45cdd41c')"
---rx
psql axial_ods -c "select client_process.insert_file_log('oa_gtwpa','rx_claims','Gateway_Pharmacy_Claims.txt','bc92925d650ad3efefa3ce300d79a64718849516')"
---provider
psql axial_ods -c "select client_process.insert_file_log('oa_gtwpa','provider_identity','Gateway_Provider_Identity.txt','277a01025daad3a3789429b31901e565c03c917f')"


9493 Gateway_Provider_Identity.txt
9492 Gateway_Pharmacy_Claims.txt
9491 Gateway_Member_Eligibility.txt
9490 Gateway_Medical_Claims.txt

---psql axial_ods -c 'select client_process.import_files_to_raw(9493)' 
---psql axial_ods -c 'select client_process.import_files_to_raw(9492)' 
---psql axial_ods -c 'select client_process.import_files_to_raw(9491)' 
---psql axial_ods -c 'select client_process.import_files_to_raw(9490)' 


----------

psql axial_ods -c "select client_process.insert_file_log('csin','medical_claims','CareSource_MedicalClaims_History_2018_20220414141412.txt','e89ae6cab709f2326f26081819fab48e46de5e64')"
e89ae6cab709f2326f26081819fab48e46de5e64

psql axial_ods -c 'select client_process.import_files_to_raw(9539)'


-----
psql axial_ods -c "select client_process.insert_file_log('azch','medical_claims','AzCH_Marketplace_Medical_Claims_20220411-20220417.txt','ce4454df538c8a5fa45cde45111dce96d97bfd43')"
psql axial_ods -c 'select client_process.import_files_to_raw(9619)'

9619
ce4454df538c8a5fa45cde45111dce96d97bfd43



psql axial_ods -c "select client_process.insert_file_log('azch','census','AzCH_Medicaid_HIE_Data_20220427.txt','4750359b92caf7390a931060f52e5db31781b0e0')"
psql axial_ods -c 'select client_process.import_files_to_raw(9707)'



psql axial_ods -c "select client_process.insert_file_log('hmkde_md','adt','HighmarkDE_Medicaid_Census_20220428.txt','c481af9591de2aa99e744eb82e4ba3b825dac051')"
psql axial_ods -c 'select client_process.import_files_to_raw(9725)'


psql axial_ods -c "select client_process.insert_file_log('hmkde_md','prior_auth','HighmarkDE_Medicaid_Prior_Authorization_20220428.txt','19df972326b5cc9d2830b2d04003792abdc21831')"
psql axial_ods -c 'select client_process.import_files_to_raw(9726)'