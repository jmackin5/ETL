----Tuesdays analytic update, REDSHIFT TO EXASOL

1- ssh jmackinnon@10.4.6.170

2- screen -r -d <screen-number>

3- /usr/opt/EXASuite-7/EXASolution-7.0.4/bin/Console/exaplus -u jmackinnon -c localhost:8563

4- EXECUTE SCRIPT ANALYTICS.MEDICAL_EVENTS_LOAD_FROM_REDSHIFT();

5 - QA on exasol 

SELECT * FROM (
SELECT 'PATIENT_MEDICAL_ENCOUNTER' tbl, client_name,run_id,count(1)
FROM ANALYTICS.PATIENT_MEDICAL_ENCOUNTER
GROUP BY client_name,run_id
union
SELECT 'PATIENT_MEDICAL_ENCOUNTER_CLAIM',client_name,run_id,count(1)
FROM ANALYTICS.PATIENT_MEDICAL_ENCOUNTER_CLAIM
GROUP BY client_name,run_id
union
SELECT 'PATIENT_MEDICAL_ENCOUNTER_DIAGNOSIS',client_name,run_id,count(1)
FROM ANALYTICS.PATIENT_MEDICAL_ENCOUNTER_DIAGNOSIS
GROUP BY client_name,run_id) A ORDER BY 1,2;