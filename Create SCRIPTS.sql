--INSERT INTO REPORTING.PROCESS_TRACKER (PROCESS_NAME, RUNTIME) VALUES('OA_CNTIN.MONTHLY_TABLE_LOAD', now())
--delete FROM REPORTING.PROCESS_TRACKER  WHERE PROCESS_NAME = 'OA_CNTIN.MONTHLY_TABLE_LOAD'
--- change 85 to 85 and OA_CNTIN to OA_CNTIN

SELECT * FROM OA_CNTIN.MST_MEMBER_PROVIDER_XREF WHERE client_id = 85;
SELECT * FROM OA_CNTIN.MAT_ADHERENCE_DAYS WHERE client_id = 85;



query([[create or replace table "OA_CNTIN"."MST_MEMBER_RESULTS" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(50),"REPORTING_YYYY_MM" varchar(2000),"MEMBER_OPIOID_PAIN_FLAG" INTEGER,"MEMBER_OPIOID_MAT_FLAG" INTEGER,"REPORTING_DATE" date,"AXIAL_MEMBER_ID" BIGINT,"MEMBER_MEDD" DOUBLE,"MEMBER_OPIOID_DOSE_FLAG" INTEGER,"MEMBER_DOSE_GROUP" varchar(15),"MEMBER_MEDD_GREATER_THAN_90_FLAG" INTEGER,"MEMBER_MEDD_GREATER_THAN_120_FLAG" INTEGER,"MEMBER_OUD_EVALUTION_SUGGESTED" INTEGER,"MEMBER_OPIOID_MULTIPRESCRIBER_CRITICAL_FLAG" INTEGER,"MEMBER_MULTIPRESCRIBER_FLAG" INTEGER,"MEMBER_OPIOID_MULTI_PHARMACY_FLAG" INTEGER,"MEMBER_OPIOID_POLYDRUG_CRITICAL_FLAG" INTEGER,"MEMBER_CHRONIC_OPIOID_USE_FLAG" INTEGER,"MEMBER_MEDICAL_DIAGNOSIS_FLAG" INTEGER,"MEMBER_OPIOID_DURATION_FLAG" INTEGER,"MEMBER_OPIOID_USE_DISORDER_FLAG" INTEGER,"MEMBER_PREGNANCY_DETECTION_FLAG" INTEGER,"MEMBER_SUBSTANCE_USE_DISORDER_DIAGNOSIS_ON_OPIOIDS_FLAG" INTEGER,"MEMBER_OPIOID_POLYDRUG_FLAG" INTEGER,"MEMBER_OPIOID_OVERDOSE_AT_ER_FLAG" INTEGER,"MEMBER_ER_VISIT_WHILE_TAKING_OPIOIDS_FLAG" INTEGER,"MEMBER_ER_VISITS_PRESCRIBER_FLAG" INTEGER,"MEMBER_ORR_TIER" INTEGER)]])
query([[import into "OA_CNTIN"."MST_MEMBER_RESULTS" from jdbc at ods_reporting statement 'select client_id,client_name,reporting_yyyy_mm,member_opioid_pain_flag,member_opioid_mat_flag,reporting_date,axial_member_id,member_medd,member_opioid_dose_flag,member_dose_group,member_medd_greater_than_90_flag,member_medd_greater_than_120_flag,member_oud_evalution_suggested,member_opioid_multiprescriber_critical_flag,member_multiprescriber_flag,member_opioid_multi_pharmacy_flag,member_opioid_polydrug_critical_flag,member_chronic_opioid_use_flag,member_medical_diagnosis_flag,member_opioid_duration_flag,member_opioid_use_disorder_flag,member_pregnancy_detection_flag,member_substance_use_disorder_diagnosis_on_opioids_flag,member_opioid_polydrug_flag,member_opioid_overdose_at_er_flag,member_er_visit_while_taking_opioids_flag,member_er_visits_prescriber_flag,member_orr_tier from master.mst_member_results']])
query([[COMMIT]])






EXECUTE SCRIPT OA_CNTIN.MONTHLY_TABLE_LOAD();

CREATE OR REPLACE LUA SCRIPT OA_CNTIN."MONTHLY_TABLE_LOAD" () RETURNS ROWCOUNT AS
query([[INSERT INTO REPORTING.PROCESS_TRACKER (PROCESS_NAME, RUNTIME) VALUES('OA_CNTIN.MONTHLY_TABLE_LOAD', now())]])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MST_MEMBER_MONTH_BY_LOB" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(15),"AXIAL_MEMBER_ID" INTEGER,"REPORTING_YYYY_MM" varchar(2000),"REPORTING_DATE" date,"LOB" varchar(2000),"INSURED_STATUS" varchar(2000),"PLAN_NAME" varchar(2000),"AGE_DURING_MEMBER_YYYY_MM" DOUBLE)]])
query([[import into "OA_CNTIN"."MST_MEMBER_MONTH_BY_LOB" from jdbc at ods_reporting statement 'select client_id,client_name,axial_member_id,reporting_yyyy_mm,reporting_date,lob,insured_status,plan_name,age_during_member_yyyy_mm from master.mst_member_month_by_lob']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MST_MEMBER_PROVIDER_XREF" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(15),"AXIAL_MEMBER_ID" BIGINT,"PROVIDER_NPI" varchar(2000),"REPORTING_YYYY_MM" varchar(2000),"REPORTING_DATE" date,"MEMBER_PROVIDER_MEDICAL_CLAIM_FLAG" INTEGER,"MEMBER_PROVIDER_RX_CLAIM_FLAG" INTEGER,"MEMBER_PROVIDER_OPIOID_PAIN_FLAG" INTEGER,"MEMBER_PROVIDER_OPIOID_MAT_FLAG" INTEGER)]])
query([[import into "OA_CNTIN"."MST_MEMBER_PROVIDER_XREF" from jdbc at ods_reporting statement 'select client_id,client_name,axial_member_id,provider_npi,reporting_yyyy_mm,reporting_date,member_provider_medical_claim_flag,member_provider_rx_claim_flag,member_provider_opioid_pain_flag,member_provider_opioid_mat_flag from master.mst_member_provider_xref']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MST_MEMBER_RESULTS" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(50),"REPORTING_YYYY_MM" varchar(2000),"MEMBER_OPIOID_PAIN_FLAG" INTEGER,"MEMBER_OPIOID_MAT_FLAG" INTEGER,"REPORTING_DATE" date,"AXIAL_MEMBER_ID" BIGINT,"MEMBER_MEDD" DOUBLE,"MEMBER_OPIOID_DOSE_FLAG" INTEGER,"MEMBER_DOSE_GROUP" varchar(15),"MEMBER_MEDD_GREATER_THAN_90_FLAG" INTEGER,"MEMBER_MEDD_GREATER_THAN_120_FLAG" INTEGER,"MEMBER_OUD_EVALUTION_SUGGESTED" INTEGER,"MEMBER_OPIOID_MULTIPRESCRIBER_CRITICAL_FLAG" INTEGER,"MEMBER_MULTIPRESCRIBER_FLAG" INTEGER,"MEMBER_OPIOID_MULTI_PHARMACY_FLAG" INTEGER,"MEMBER_OPIOID_POLYDRUG_CRITICAL_FLAG" INTEGER,"MEMBER_CHRONIC_OPIOID_USE_FLAG" INTEGER,"MEMBER_MEDICAL_DIAGNOSIS_FLAG" INTEGER,"MEMBER_OPIOID_DURATION_FLAG" INTEGER,"MEMBER_OPIOID_USE_DISORDER_FLAG" INTEGER,"MEMBER_PREGNANCY_DETECTION_FLAG" INTEGER,"MEMBER_SUBSTANCE_USE_DISORDER_DIAGNOSIS_ON_OPIOIDS_FLAG" INTEGER,"MEMBER_OPIOID_POLYDRUG_FLAG" INTEGER,"MEMBER_OPIOID_OVERDOSE_AT_ER_FLAG" INTEGER,"MEMBER_ER_VISIT_WHILE_TAKING_OPIOIDS_FLAG" INTEGER,"MEMBER_ER_VISITS_PRESCRIBER_FLAG" INTEGER,"MEMBER_ORR_TIER" INTEGER)]])
query([[import into "OA_CNTIN"."MST_MEMBER_RESULTS" from jdbc at ods_reporting statement 'select client_id,client_name,reporting_yyyy_mm,member_opioid_pain_flag,member_opioid_mat_flag,reporting_date,axial_member_id,member_medd,member_opioid_dose_flag,member_dose_group,member_medd_greater_than_90_flag,member_medd_greater_than_120_flag,member_oud_evalution_suggested,member_opioid_multiprescriber_critical_flag,member_multiprescriber_flag,member_opioid_multi_pharmacy_flag,member_opioid_polydrug_critical_flag,member_chronic_opioid_use_flag,member_medical_diagnosis_flag,member_opioid_duration_flag,member_opioid_use_disorder_flag,member_pregnancy_detection_flag,member_substance_use_disorder_diagnosis_on_opioids_flag,member_opioid_polydrug_flag,member_opioid_overdose_at_er_flag,member_er_visit_while_taking_opioids_flag,member_er_visits_prescriber_flag,member_orr_tier from master.mst_member_results']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MST_PROVIDER_DIM" ("CLIENT_NAME" varchar(50),"CLIENT_ID" INTEGER,"PROVIDER_NPI" varchar(2000),"PROVIDER_ID" varchar(2000),"PROVIDER_NAME" varchar(2000),"PROVIDER_FIRST_NAME" varchar(2000),"PROVIDER_LAST_NAME" varchar(2000),"PROVIDER_MIDDLE_NAME" varchar(2000),"PROVIDER_ADDRESS_1" varchar(2000),"PROVIDER_ADDRESS_2" varchar(2000),"PROVIDER_CITY" varchar(2000),"PROVIDER_STATE" varchar(2000),"PROVIDER_ZIP" varchar(2000),"PROVIDER_PHONE" varchar(2000),"PROVIDER_PHONE_EXTENSION" varchar(2000),"PROVIDER_FAX" varchar(2000),"PROVIDER_EMAIL" varchar(2000),"PROVIDER_TYPE" varchar(2000),"PROVIDER_EFFECTIVE_DATE" date,"PROVIDER_TERMINATION_DATE" date,"PROVIDER_TERM_REASON_CODE" varchar(2000),"PROVIDER_TERM_REASON_DESCRIPTION" varchar(2000),"PROVIDER_NETWORK_FLAG" INTEGER,"PROVIDER_SPECIALTY_CODE" varchar(2000),"PROVIDER_SPECIALTY_DESCRIPTION" varchar(2000),"PROVIDER_TAXONOMY_CODE" varchar(2000),"PROVIDER_GROUP_ID" varchar(2000),"PROVIDER_GROUP_NAME" varchar(2000))]])
query([[import into "OA_CNTIN"."MST_PROVIDER_DIM" from jdbc at ods_reporting statement 'select client_name,client_id,provider_npi,provider_id,provider_name,provider_first_name,provider_last_name,provider_middle_name,provider_address_1,provider_address_2,provider_city,provider_state,provider_zip,provider_phone,provider_phone_extension,provider_fax,provider_email,provider_type,provider_effective_date,provider_termination_date,provider_term_reason_code,provider_term_reason_description,provider_network_flag,provider_specialty_code,provider_specialty_description,provider_taxonomy_code,provider_group_id,provider_group_name from master.mst_provider_dim']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MST_PROVIDER_RESULTS" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(50),"REPORTING_YYYY_MM" varchar(250),"PROVIDER_NPI" varchar(250),"PROVIDER_PEER_GROUP" varchar(250),"PROVIDER_MEDD" DOUBLE,"PROVIDER_OPIOID_OVERDOSE_AT_ER_RISK_FLAG" INTEGER,"PROVIDER_ER_VISIT_WHILE_TAKING_OPIOIDS_RISK_FLAG" INTEGER,"PROVIDER_ER_VISITS_PRESCRIBER_RISK_FLAG" INTEGER,"PROVIDER_CHRONIC_OPIOID_USE_FLAG" INTEGER,"PROVIDER_DRUG_TESTING_FLAG" INTEGER,"PROVIDER_EARLY_OPIOID_REFILL_RISK_FLAG" INTEGER,"PROVIDER_MEDICAL_DIAGNOSIS_RISK_FLAG" INTEGER,"PROVIDER_OPIOID_DOSE_RISK_FLAG" INTEGER,"PROVIDER_OPIOID_DURATION_RISK_FLAG" INTEGER,"PROVIDER_OPIOID_PANEL_RISK_FLAG" INTEGER,"PROVIDER_OPIOID_POLYDRUG_RISK_FLAG" INTEGER,"PROVIDER_OPIOIDS_PRESCRIBED_DURING_PREGNANCY_RISK_FLAG" INTEGER,"PROVIDER_OPIOID_TAPER_RISK_FLAG" INTEGER,"PROVIDER_OPIOID_USE_DISORDER_RISK_FLAG" INTEGER,"PROVIDER_PREGNANCY_DETECTION_RISK_FLAG" INTEGER,"PROVIDER_ROUTINE_EKG_ON_METHADONE_RISK_FLAG" INTEGER,"PROVIDER_SUBSTANCE_USE_DISORDER_DIAGNOSIS_ON_OPIOIDS_RISK_FLAG" INTEGER,"PROVIDER_BEHAVIORAL_HEALTH_DIAGNOSIS_RISK_FLAG" INTEGER,"PROVIDER_OPIOID_MULTI_PHARMACY_RISK_FLAG" INTEGER,"PROVIDER_OPIOID_MULTI_PRESCRIBER_RISK_FLAG" INTEGER)]])
query([[import into "OA_CNTIN"."MST_PROVIDER_RESULTS" from jdbc at ods_reporting statement 'select client_id,client_name,reporting_yyyy_mm,provider_npi,provider_peer_group,provider_medd,provider_opioid_overdose_at_er_risk_flag,provider_er_visit_while_taking_opioids_risk_flag,provider_er_visits_prescriber_risk_flag,provider_chronic_opioid_use_flag,provider_drug_testing_flag,provider_early_opioid_refill_risk_flag,provider_medical_diagnosis_risk_flag,provider_opioid_dose_risk_flag,provider_opioid_duration_risk_flag,provider_opioid_panel_risk_flag,provider_opioid_polydrug_risk_flag,provider_opioids_prescribed_during_pregnancy_risk_flag,provider_opioid_taper_risk_flag,provider_opioid_use_disorder_risk_flag,provider_pregnancy_detection_risk_flag,provider_routine_ekg_on_methadone_risk_flag,provider_substance_use_disorder_diagnosis_on_opioids_risk_flag,provider_behavioral_health_diagnosis_risk_flag,provider_opioid_multi_pharmacy_risk_flag,provider_opioid_multi_prescriber_risk_flag from master.mst_provider_results']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."PROVIDER_GROUP_XREF_DIM" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(15),"PROVIDER_NPI" varchar(2000),"PROVIDER_GROUP_ID" varchar(2000),"GROUP_NAME" varchar(2000),"PROVIDER_EFFECTIVE_DATE" date,"PROVIDER_TERMINATION_DATE" date)]])
query([[import into "OA_CNTIN"."PROVIDER_GROUP_XREF_DIM" from jdbc at ods_reporting statement 'select client_id,client_name,provider_npi,provider_group_id,group_name,provider_effective_date,provider_termination_date from master.provider_group_xref_dim']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."PROVIDER_NAME_DIM" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(15),"PROVIDER_NPI" varchar(2000),"PROVIDER_FULL_NAME" varchar(2000),"PROVIDER_FIRST_NAME" varchar(2000),"PROVIDER_LAST_NAME" varchar(2000))]])
query([[import into "OA_CNTIN"."PROVIDER_NAME_DIM" from jdbc at ods_reporting statement 'select client_id,client_name,provider_npi,provider_full_name,provider_first_name,provider_last_name from master.provider_name_dim']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MEMBER_ADDRESS_DIM" ("CLIENT_NAME" varchar(50),"CLIENT_ID" INTEGER,"AXIAL_MEMBER_ID" BIGINT,"MEMBER_ADDRESS_1" varchar(2000),"MEMBER_ADDRESS_2" varchar(2000),"MEMBER_CITY" varchar(2000),"MEMBER_STATE" varchar(2000),"MEMBER_ZIP" varchar(2000),"REGION" varchar(2000),"MEMBER_HOMELESS_FLAG" bool,"LATITUDE" DOUBLE,"LONGITUDE" DOUBLE,"GEOM_QUALITY_RATING" INTEGER,"COUNTY_NAME" varchar(2000))]])
query([[import into "OA_CNTIN"."MEMBER_ADDRESS_DIM" from jdbc at ods_reporting statement 'select client_name,client_id,axial_member_id,member_address_1,member_address_2,member_city,member_state,member_zip,region,member_homeless_flag,latitude,longitude,geom_quality_rating,county_name from master.member_address_dim']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MEMBER_DIM" ("CLIENT_NAME" varchar(50),"CLIENT_ID" INTEGER,"AXIAL_MEMBER_ID" BIGINT,"CLIENT_MEMBER_ID" varchar(2000),"MEMBER_DATE_OF_BIRTH" date,"MEMBER_GENDER" varchar(2000),"MEMBER_FIRST_NAME" varchar(2000),"MEMBER_LAST_NAME" varchar(2000),"MEMBER_MIDDLE_INITIAL" varchar(2000),"MEMBER_HOME_PHONE" varchar(2000),"MEMBER_CELL_PHONE" varchar(2000),"MEMBER_DATE_OF_DEATH" date,"MEMBER_CAUSE_OF_DEATH" varchar(2000),"MEMBER_RACE" varchar(2000),"MEMBER_LANGUAGE_CODE" varchar(2000),"MEMBER_REGION" varchar(2000),"MEMBER_HOMELESS_FLAG" bool)]])
query([[import into "OA_CNTIN"."MEMBER_DIM" from jdbc at ods_reporting statement 'select client_name,client_id,axial_member_id,client_member_id,member_date_of_birth,member_gender,member_first_name,member_last_name,member_middle_initial,member_home_phone,member_cell_phone,member_date_of_death,member_cause_of_death,member_race,member_language_code,member_region,member_homeless_flag from master.member_dim']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MEMBER_PROVIDER_RESULTS_FACT" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(50),"AXIAL_MEMBER_ID" BIGINT,"NPI" varchar(250),"REPORTING_YYYY_MM" varchar(250),"REPORTING_DATE" date,"IS_OPIOID" bool,"EARLY_REFILL_RISK" DOUBLE,"FULLY_TAPERED_RISK" DOUBLE,"METHADONE_EKG_RISK" DOUBLE,"DRUG_TESTING" DOUBLE,"EARLY_OPIOID_REFILL_RISK" DOUBLE,"OPIOID_PANEL_RISK" DOUBLE,"OPIOIDS_PRESCRIBED_DURING_PREGNANCY_RISK" DOUBLE,"ROUTINE_EKG_ON_METHADONE_RISK" DOUBLE,"OPIOID_TAPER_RISK" DOUBLE,"MEDD" DOUBLE,"MED" DOUBLE,"OPIOID_PAIN_DAYS" INTEGER)]])
query([[import into "OA_CNTIN"."MEMBER_PROVIDER_RESULTS_FACT" from jdbc at ods_reporting statement 'select client_id,client_name,axial_member_id,npi,reporting_yyyy_mm,reporting_date,is_opioid,early_refill_risk,fully_tapered_risk,methadone_ekg_risk,drug_testing,early_opioid_refill_risk,opioid_panel_risk,opioids_prescribed_during_pregnancy_risk,routine_ekg_on_methadone_risk,opioid_taper_risk,medd,med,opioid_pain_days from master.member_provider_results_fact']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MAT_ADHERENCE_DAYS" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(50),"AXIAL_MEMBER_ID" BIGINT,"ADHERENCE_YYYY_MM" varchar(2000),"ADHERENCE_DATE" date,"CLAIM_SERVICE_FROM_DATE" date,"CLAIM_SERVICE_TO_DATE" date,"CLAIM_DAY_SUPPLY" INTEGER,"SOURCE_CATEGORY" varchar(24),"AXIAL_CLAIM_ID" BIGINT,"PROVIDER_NPI" varchar(2000),"TX_TYPE" varchar(2000),"SUB_TX_TYPE" varchar(2000),"ADMINISTRATION_TYPE" varchar(2000),"SCRIPT_MAT_DAYS" INTEGER,"MAT_DAYS" INTEGER)]])
query([[import into "OA_CNTIN"."MAT_ADHERENCE_DAYS" from jdbc at postgres_ods statement 'select client_id,client_name,axial_member_id,adherence_yyyy_mm,adherence_date,claim_service_from_date,claim_service_to_date,claim_day_supply,source_category,axial_claim_id,provider_npi,tx_type,sub_tx_type,administration_type,script_mat_days,mat_days from dataops_to_reporting.mat_adherence_days']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MEMBER_EOL" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(50),"AXIAL_MEMBER_ID" BIGINT,"REPORTING_DATE" date,"REPORTING_YYYY_MM" varchar(2000),"MEMBER_HOSPICE_FLAG" INTEGER,"MEMBER_CANCER_FLAG" INTEGER)]])
query([[import into "OA_CNTIN"."MEMBER_EOL" from jdbc at ods_reporting statement 'select client_id,client_name,axial_member_id,reporting_date,reporting_yyyy_mm,member_hospice_flag,member_cancer_flag from master.member_eol']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MEMBER_EOL_EVENT" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(50),"AXIAL_MEMBER_ID" BIGINT,"AXIAL_CLAIM_ID" BIGINT,"AXIAL_CLAIM_LINE_ID" INTEGER,"SERVICE_FROM_DATE" date,"SERVICE_TO_DATE" date,"MEDICAL_CLAIM_EVENT_TYPE_CODE" varchar(250),"MEDICAL_CLAIM_EVENT_REASON" varchar(250),"ICD_CODE_TYPE" INTEGER,"ICD_CODE" varchar(250),"ICD_CODE_POSITION" INTEGER)]])
query([[import into "OA_CNTIN"."MEMBER_EOL_EVENT" from jdbc at ods_reporting statement 'select client_id,client_name,axial_member_id,axial_claim_id,axial_claim_line_id,service_from_date,service_to_date,medical_claim_event_type_code,medical_claim_event_reason,icd_code_type,icd_code,icd_code_position from master.member_eol_event']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."PROVIDER_ADDRESS_DIM" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(15),"PROVIDER_NPI" varchar(2000),"PROVIDER_ADDRESS1" varchar(2000),"PROVIDER_ADDRESS2" varchar(2000),"PROVIDER_CITY" varchar(2000),"PROVIDER_STATE" varchar(2000),"PROVIDER_ZIP" varchar(2000),"PROVIDER_PHONE" varchar(2000),"PROVIDER_EMAIL" varchar(2000),"COUNTY_NAME" varchar(100),"LATITUDE" DOUBLE,"LONGITUDE" DOUBLE,"GEOM_QUALITY_RATING" INTEGER)]])
query([[import into "OA_CNTIN"."PROVIDER_ADDRESS_DIM" from jdbc at ods_reporting statement 'select client_id,client_name,provider_npi,provider_address1,provider_address2,provider_city,provider_state,provider_zip,provider_phone,provider_email,county_name,latitude,longitude,geom_quality_rating from master.provider_address_dim']])
query([[COMMIT]])
query([[create or replace table "OA_CNTIN"."MEMBER_SUBSCRIBER" ("CLIENT_ID" INTEGER,"CLIENT_NAME" varchar(50),"AXIAL_MEMBER_ID" BIGINT,"CLIENT_MEMBER_ID" varchar(2000),"SUBSCRIBER_ID" varchar(2000),"EFFECTIVE_DATE" date,"EFFECTIVE_YYYY_MM" varchar(2000),"TERMINATION_DATE" date,"TERMINATION_YYYY_MM" varchar(2000))]])
query([[import into "OA_CNTIN"."MEMBER_SUBSCRIBER" from jdbc at ods_reporting statement 'select client_id,client_name,axial_member_id,client_member_id,subscriber_id,effective_date,effective_yyyy_mm,termination_date,termination_yyyy_mm from master.member_subscriber']])
query([[COMMIT]])
query([[UPDATE reporting.process_tracker SET runtime = now() WHERE process_name = 'OA_CNTIN.MONTHLY_TABLE_LOAD']])



EXECUTE SCRIPT OA_TEST."RAW_MEDICAL_CLAIM_LOAD" ('<<client Name>>');
EXECUTE SCRIPT OA_TEST."RAW_RX_CLAIM_LOAD" ('<<client Name>>');


-- STAGING.MEDICAL_CLAIM definition

CREATE TABLE OA_CNTIN.MEDICAL_CLAIM (
		CLIENT_ID DECIMAL(18,0),
		AXIAL_MEMBER_ID DECIMAL(36,0),
		CLIENT_MEMBER_ID VARCHAR(2000) UTF8,
		CLIENT_CLAIM_KEY VARCHAR(2000) UTF8,
		CLAIM_ID VARCHAR(2000) UTF8,
		CLAIM_LINE VARCHAR(2000) UTF8,
		AXIAL_CLAIM_ID DECIMAL(36,0),
		AXIAL_CLAIM_LINE_ID DECIMAL(18,0),
		CLIENT_CLAIM_STATUS VARCHAR(2000) UTF8,
		AXIAL_CLAIM_STATUS VARCHAR(2000) UTF8,
		ORIGINAL_CLAIM_ID VARCHAR(2000) UTF8,
		CLAIM_TYPE VARCHAR(2000) UTF8,
		AXIAL_CLAIM_TYPE VARCHAR(2000) UTF8,
		SERVICE_FROM_DATE DATE,
		SERVICE_TO_DATE DATE,
		DATE_RECEIVED DATE,
		DATE_PAID DATE,
		ADMIT_DATE DATE,
		ADMIT_SOURCE VARCHAR(2000) UTF8,
		DISCHARGE_DATE DATE,
		DISCHARGE_STATUS VARCHAR(2000) UTF8,
		SERVICING_PROVIDER_ID VARCHAR(2000) UTF8,
		SERVICING_PROVIDER_NPI VARCHAR(2000) UTF8,
		SERVICING_PROVIDER_EIN VARCHAR(2000) UTF8,
		SERVICING_PROVIDER_DEA VARCHAR(2000) UTF8,
		PAR_FLAG VARCHAR(2000) UTF8,
		SERVICING_PROVIDER_NETWORK VARCHAR(2000) UTF8,
		BILLING_PROVIDER_ID VARCHAR(2000) UTF8,
		BILLING_PROVIDER_NPI VARCHAR(2000) UTF8,
		BILLING_PROVIDER_EIN VARCHAR(2000) UTF8,
		ATTENDING_PROVIDER_NPI VARCHAR(2000) UTF8,
		REFERRING_PROVIDER_NPI VARCHAR(2000) UTF8,
		AUTH_NUMBER VARCHAR(2000) UTF8,
		ICD_PROCEDURE_CODE_TYPE VARCHAR(2000) UTF8,
		ICD_PROCEDURE_CODE VARCHAR(2000) UTF8,
		PLACE_OF_SERVICE VARCHAR(2000) UTF8,
		BILL_TYPE VARCHAR(2000) UTF8,
		DRG_VERSION VARCHAR(2000) UTF8,
		DRG_CODE VARCHAR(2000) UTF8,
		PROCEDURE_CODE VARCHAR(2000) UTF8,
		PROCEDURE_CODE_MOD1 VARCHAR(2000) UTF8,
		PROCEDURE_CODE_MOD2 VARCHAR(2000) UTF8,
		PROCEDURE_CODE_MOD3 VARCHAR(2000) UTF8,
		UNITS VARCHAR(2000) UTF8,
		REVENUE_CODE VARCHAR(2000) UTF8,
		CAPITATION_FLAG VARCHAR(2000) UTF8,
		AMOUNT_BILLED DOUBLE,
		AMOUNT_ALLOWED DOUBLE,
		AMOUNT_TOTAL_PAID DOUBLE,
		AMOUNT_CAPITATED DOUBLE,
		AMOUNT_COPAY DOUBLE,
		AMOUNT_DEDUCTIBLE DOUBLE,
		AMOUNT_COINSURANCE DOUBLE,
		AMOUNT_PATIENT_PAID DOUBLE,
		AMOUNT_COB DOUBLE,
		AMOUNT_INSURANCE_PAID DOUBLE,
		TYPE_OF_SERVICE VARCHAR(2000) UTF8,
		PROVIDER_SPECIALTY VARCHAR(2000) UTF8,
		PROVIDER_TAXONOMY VARCHAR(2000) UTF8,
		PEER_GROUP VARCHAR(2000) UTF8,
		EMPLOYER_GROUP_NUMBER VARCHAR(2000) UTF8,
		LOB VARCHAR(2000) UTF8,
		PLAN_CODE VARCHAR(2000) UTF8,
		FILE_LOG_ID DECIMAL(36,0),
		RECORD_SOURCE_ID DECIMAL(36,0),
		LOAD_TIMESTAMP TIMESTAMP,
		RECORD_START TIMESTAMP,
		RECORD_END TIMESTAMP,
		AXIAL_MEMBER_ID_PARTITION DECIMAL(9,0),
		MED_ECON_IS_PAIN DECIMAL(18,0),
		IS_ED_DETAIL DECIMAL(18,0),
		AXIAL_POS VARCHAR(2000) UTF8,
		EXASOL_LOADTIME TIMESTAMP WITH LOCAL TIME ZONE
);


-- STAGING.MEDICAL_CLAIM_DIAGNOSIS_LONG definition

CREATE TABLE OA_CNTIN.MEDICAL_CLAIM_DIAGNOSIS_LONG (
		CLIENT_ID DECIMAL(18,0),
		AXIAL_MEMBER_ID DECIMAL(36,0),
		AXIAL_CLAIM_ID DECIMAL(36,0),
		AXIAL_CLAIM_LINE_ID DECIMAL(18,0),
		SERVICE_FROM_DATE DATE,
		SERVICE_TO_DATE DATE,
		PLACE_OF_SERVICE VARCHAR(2000) UTF8,
		ICD_TYPE DECIMAL(18,0),
		ICD_POSITION_NAME VARCHAR(2000) UTF8,
		ICD_POSITION VARCHAR(2000) UTF8,
		ICD_FORMATTED_CODE VARCHAR(2000) UTF8,
		ICD_UNFORMATTED_CODE VARCHAR(2000) UTF8,
		FILE_LOG_ID DECIMAL(36,0),
		RECORD_SOURCE_ID DECIMAL(36,0),
		LOAD_TIMESTAMP TIMESTAMP,
		AXIAL_MEMBER_ID_PARTITION DECIMAL(9,0),
		MED_ECON_IS_PAIN DECIMAL(18,0),
		EXASOL_LOADTIME TIMESTAMP
);


-- STAGING.MEDICAL_CLAIM_DIAGNOSIS_WIDE definition

CREATE TABLE OA_CNTIN.MEDICAL_CLAIM_DIAGNOSIS_WIDE (
		CLIENT_ID DECIMAL(18,0),
		AXIAL_MEMBER_ID DECIMAL(36,0),
		AXIAL_CLAIM_ID DECIMAL(36,0),
		AXIAL_CLAIM_LINE_ID DECIMAL(18,0),
		SERVICE_FROM_DATE DATE,
		SERVICE_TO_DATE DATE,
		PLACE_OF_SERVICE VARCHAR(2000) UTF8,
		ICD_TYPE DECIMAL(18,0),
		ADMIT_DIAGNOSIS VARCHAR(2000) UTF8,
		PRIMARY_DIAGNOSIS_CODE VARCHAR(2000) UTF8,
		ICD_CODE_1 VARCHAR(2000) UTF8,
		ICD_CODE_2 VARCHAR(2000) UTF8,
		ICD_CODE_3 VARCHAR(2000) UTF8,
		ICD_CODE_4 VARCHAR(2000) UTF8,
		ICD_CODE_5 VARCHAR(2000) UTF8,
		ICD_CODE_6 VARCHAR(2000) UTF8,
		ICD_CODE_7 VARCHAR(2000) UTF8,
		ICD_CODE_8 VARCHAR(2000) UTF8,
		ICD_CODE_9 VARCHAR(2000) UTF8,
		ICD_CODE_10 VARCHAR(2000) UTF8,
		ICD_CODE_11 VARCHAR(2000) UTF8,
		ICD_CODE_12 VARCHAR(2000) UTF8,
		ICD_CODE_13 VARCHAR(2000) UTF8,
		ICD_CODE_14 VARCHAR(2000) UTF8,
		ICD_CODE_15 VARCHAR(2000) UTF8,
		ICD_CODE_16 VARCHAR(2000) UTF8,
		ICD_CODE_17 VARCHAR(2000) UTF8,
		ICD_CODE_18 VARCHAR(2000) UTF8,
		ICD_CODE_19 VARCHAR(2000) UTF8,
		ICD_CODE_20 VARCHAR(2000) UTF8,
		ICD_CODE_21 VARCHAR(2000) UTF8,
		ICD_CODE_22 VARCHAR(2000) UTF8,
		ICD_CODE_23 VARCHAR(2000) UTF8,
		ICD_CODE_24 VARCHAR(2000) UTF8,
		ICD_CODE_25 VARCHAR(2000) UTF8,
		FILE_LOG_ID DECIMAL(36,0),
		RECORD_SOURCE_ID DECIMAL(36,0),
		LOAD_TIMESTAMP TIMESTAMP,
		AXIAL_MEMBER_ID_PARTITION DECIMAL(9,0),
		EXASOL_LOADTIME TIMESTAMP WITH LOCAL TIME ZONE
);

-- OA_CSIN.MEDICAL_CLAIM_DX_LONG definition

CREATE TABLE OA_CNTIN.MEDICAL_CLAIM_DX_LONG (
		CLIENT_ID DECIMAL(18,0),
		CLIENT_NAME VARCHAR(50) UTF8,
		AXIAL_MEMBER_ID DECIMAL(36,0),
		AXIAL_CLAIM_ID DECIMAL(36,0),
		AXIAL_CLAIM_LINE_ID DECIMAL(18,0),
		SERVICE_FROM_DATE DATE,
		SERVICE_FROM_YYYY_MM VARCHAR(7) UTF8,
		SERVICE_TO_DATE DATE,
		SERVICE_TO_YYYY_MM VARCHAR(7) UTF8,
		PLACE_OF_SERVICE VARCHAR(2000) UTF8,
		ICD_TYPE DECIMAL(18,0),
		ICD_POSITION_NAME VARCHAR(2000) UTF8,
		ICD_POSITION VARCHAR(2000) UTF8,
		ICD_FORMATTED_CODE VARCHAR(2000) UTF8,
		ICD_UNFORMATTED_CODE VARCHAR(2000) UTF8,
		FILE_LOG_ID DECIMAL(36,0),
		RECORD_SOURCE_ID DECIMAL(36,0),
		LOAD_TIMESTAMP TIMESTAMP,
		AXIAL_MEMBER_ID_PARTITION DECIMAL(9,0),
		MED_ECON_IS_PAIN DECIMAL(18,0),
		MST_MEDICAL_CLAIMS_PRIMARY_DX DECIMAL(1,0),
		EXASOL_LOADTIME TIMESTAMP
);
-- STAGING.RX_CLAIM definition

CREATE TABLE OA_CNTIN.RX_CLAIM (
		CLIENT_ID DECIMAL(18,0),
		AXIAL_MEMBER_ID DECIMAL(36,0),
		CLIENT_MEMBER_ID VARCHAR(2000) UTF8,
		CLIENT_CLAIM_KEY VARCHAR(2000) UTF8,
		CLAIM_ID VARCHAR(2000) UTF8,
		CLAIM_LINE VARCHAR(2000) UTF8,
		AXIAL_CLAIM_ID DECIMAL(36,0),
		AXIAL_CLAIM_LINE_ID DECIMAL(18,0),
		AXIAL_PRESCRIPTION_FILL_ID DECIMAL(18,0),
		CLIENT_CLAIM_STATUS VARCHAR(2000) UTF8,
		AXIAL_CLAIM_STATUS VARCHAR(2000) UTF8,
		ORIGINAL_CLAIM_ID VARCHAR(2000) UTF8,
		DATE_WRITTEN DATE,
		SERVICE_FROM_DATE DATE,
		SERVICE_TO_DATE DATE,
		PRESCRIPTION_NUMBER VARCHAR(2000) UTF8,
		REFILL_NUMBER VARCHAR(2000) UTF8,
		DATE_PAID DATE,
		NDC VARCHAR(2000) UTF8,
		QUANTITY VARCHAR(2000) UTF8,
		DAYS_SUPPLY VARCHAR(2000) UTF8,
		COMPOUND_CODE VARCHAR(2000) UTF8,
		IS_GENERIC VARCHAR(2000) UTF8,
		FORMULARY_FLAG VARCHAR(2000) UTF8,
		PRESCRIBER_ID VARCHAR(2000) UTF8,
		PRESCRIBER_NPI VARCHAR(2000) UTF8,
		PRESCRIBER_EIN VARCHAR(2000) UTF8,
		PRESCRIBER_DEA VARCHAR(2000) UTF8,
		PHARMACY_ID VARCHAR(2000) UTF8,
		PHARMACY_NPI VARCHAR(2000) UTF8,
		AMOUNT_INGREDIENT_COST DOUBLE,
		AMOUNT_DISPENSING_FEE DOUBLE,
		AMOUNT_COPAY DOUBLE,
		AMOUNT_COINSURANCE DOUBLE,
		AMOUNT_DEDUCTIBLE DOUBLE,
		AMOUNT_PATIENT_PAID DOUBLE,
		AMOUNT_INSURANCE_PAID DOUBLE,
		AMOUNT_TOTAL_PAID DOUBLE,
		AMOUNT_ALLOWED DOUBLE,
		AMOUNT_COB DOUBLE,
		EMPLOYER_GROUP_NUMBER VARCHAR(2000) UTF8,
		LOB VARCHAR(2000) UTF8,
		PLAN_CODE VARCHAR(2000) UTF8,
		SPECIALTY_FLAG VARCHAR(2000) UTF8,
		FILE_LOG_ID DECIMAL(36,0),
		RECORD_SOURCE_ID DECIMAL(36,0),
		LOAD_TIMESTAMP TIMESTAMP,
		RECORD_START TIMESTAMP,
		RECORD_END TIMESTAMP,
		AXIAL_MEMBER_ID_PARTITION DECIMAL(9,0),
		EXASOL_LOADTIME TIMESTAMP
);	

DELETE FROM OA_CNTIN.MEDICAL_CLAIM;
DELETE FROM OA_CNTIN.MEDICAL_CLAIM_DIAGNOSIS_WIDE;
DELETE FROM OA_CNTIN.MEDICAL_CLAIM_DIAGNOSIS_LONG;
DELETE FROM OA_CNTIN.MST_MEDICAL_CLAIMS;


CREATE OR replace LUA SCRIPT OA_CNTIN."RAW_MEDICAL_CLAIM_LOAD" (CLIENTNAME) RETURNS ROWCOUNT AS
DCLIENTID = query([[ SELECT DIM_ID FROM reference.client WHERE CLIENT_NAME = Lower(:cn) ]],{cn=CLIENTNAME})[1][1]
query([[DELETE FROM OA_CNTIN.MEDICAL_CLAIM WHERE client_id = :ci]],{ci=DCLIENTID})
IMPORTQUERY = query([[SELECT 'select client_id,axial_member_id,client_member_id,client_claim_key,claim_id,claim_line,axial_claim_id,axial_claim_line_id,client_claim_status,axial_claim_status,original_claim_id,claim_type,axial_claim_type,service_from_date,service_to_date,date_received,date_paid,admit_date,admit_source,discharge_date,discharge_status,servicing_provider_id,servicing_provider_npi,servicing_provider_ein,servicing_provider_dea,par_flag,servicing_provider_network,billing_provider_id,billing_provider_npi,billing_provider_ein,attending_provider_npi,referring_provider_npi,auth_number,icd_procedure_code_type,icd_procedure_code,place_of_service,bill_type,drg_version,drg_code,procedure_code,procedure_code_mod1,procedure_code_mod2,procedure_code_mod3,units,revenue_code,capitation_flag,amount_billed,amount_allowed,amount_total_paid,amount_capitated,amount_copay,amount_deductible,amount_coinsurance,amount_patient_paid,amount_cob,amount_insurance_paid,type_of_service,provider_specialty,provider_taxonomy,peer_group,employer_group_number,lob,plan_code,file_log_id,record_source_id,  load_timestamp ,  record_start , record_end ,axial_member_id_partition,med_econ_is_pain,is_ed_detail,axial_pos, now() from client_production.medical_claim_'||:cn]],{cn=CLIENTNAME})[1][1]
query([[import into OA_CNTIN.MEDICAL_CLAIM from jdbc at postgres_ods STATEMENT :iq ]],{iq = IMPORTQUERY})
query([[COMMIT]])
query([[DELETE FROM OA_CNTIN.MEDICAL_CLAIM_DIAGNOSIS_WIDE WHERE client_id = :ci]],{ci=DCLIENTID})
IMPORTQUERY = query([[SELECT 'select client_id,axial_member_id,axial_claim_id,axial_claim_line_id,service_from_date,service_to_date,place_of_service,icd_type,admit_diagnosis,primary_diagnosis_code,icd_code_1,icd_code_2,icd_code_3,icd_code_4,icd_code_5,icd_code_6,icd_code_7,icd_code_8,icd_code_9,icd_code_10,icd_code_11,icd_code_12,icd_code_13,icd_code_14,icd_code_15,icd_code_16,icd_code_17,icd_code_18,icd_code_19,icd_code_20,icd_code_21,icd_code_22,icd_code_23,icd_code_24,icd_code_25,file_log_id,record_source_id,case when  load_timestamp > ''9999-12-31 23:59:59.999'' then ''9999-12-31 23:59:59.999'' when load_timestamp < ''0001-01-01'' then ''0001-01-01'' else load_timestamp end,axial_member_id_partition, now() from client_production.medical_claim_diagnosis_wide_'||:cn]],{cn=CLIENTNAME})[1][1]
query([[import into OA_CNTIN.MEDICAL_CLAIM_DIAGNOSIS_WIDE from jdbc at postgres_ods statement :iq ]],{iq = IMPORTQUERY})
query([[COMMIT]])
query([[DELETE FROM OA_CNTIN.MEDICAL_CLAIM_DIAGNOSIS_LONG WHERE client_id = :ci]],{ci=DCLIENTID})
IMPORTQUERY = query([[ Select 'select client_id,axial_member_id,axial_claim_id,axial_claim_line_id,service_from_date,service_to_date,place_of_service,icd_type,icd_position_name,icd_position,icd_formatted_code,icd_unformatted_code,file_log_id,record_source_id,case when  load_timestamp > ''9999-12-31 23:59:59.999'' then ''9999-12-31 23:59:59.999'' when load_timestamp < ''0001-01-01'' then ''0001-01-01'' else load_timestamp end,axial_member_id_partition,med_econ_is_pain, now() from client_production.medical_claim_diagnosis_long_'||:cn]],{cn=CLIENTNAME})[1][1]
query([[import into OA_CNTIN.MEDICAL_CLAIM_DIAGNOSIS_LONG from jdbc at postgres_ods statement :iq ]],{iq = IMPORTQUERY})
query([[COMMIT]]);

CREATE OR replace LUA SCRIPT OA_CNTIN."RAW_RX_CLAIM_LOAD" (CLIENTNAME) RETURNS ROWCOUNT AS
DCLIENTID = query([[ SELECT DIM_ID FROM reference.client WHERE CLIENT_NAME = Lower(:cn) ]],{cn=CLIENTNAME})[1][1]
query([[DELETE FROM OA_CNTIN.RX_CLAIM WHERE client_id = :ci]],{ci=DCLIENTID})
IMPORTQUERY = query([[SELECT 'select client_id,axial_member_id,client_member_id,client_claim_key,claim_id,claim_line,axial_claim_id,axial_claim_line_id,axial_prescription_fill_id,client_claim_status,axial_claim_status,original_claim_id,date_written,service_from_date,service_to_date,prescription_number,refill_number,date_paid,ndc,quantity,days_supply,compound_code,is_generic,formulary_flag,prescriber_id,prescriber_npi,prescriber_ein,prescriber_dea,pharmacy_id,pharmacy_npi,amount_ingredient_cost,amount_dispensing_fee,amount_copay,amount_coinsurance,amount_deductible,amount_patient_paid,amount_insurance_paid,amount_total_paid,amount_allowed,amount_cob,employer_group_number,lob,plan_code,specialty_flag,file_log_id,record_source_id,case when  load_timestamp > ''9999-12-31 23:59:59.999'' then ''9999-12-31 23:59:59.999'' when load_timestamp < ''0001-01-01'' then ''0001-01-01'' else load_timestamp end,case when  record_start > ''9999-12-31 23:59:59.999'' then ''9999-12-31 23:59:59.999'' when record_start < ''0001-01-01'' then ''0001-01-01'' else record_start end,case when  record_end > ''9999-12-31 23:59:59.999'' then ''9999-12-31 23:59:59.999'' when record_end < ''0001-01-01'' then ''0001-01-01'' else record_end end,axial_member_id_partition, now() from client_production.rx_claim_'||:cn]],{cn=CLIENTNAME})[1][1]
query([[import into OA_CNTIN.RX_CLAIM from jdbc at postgres_ods STATEMENT :iq ]],{iq = IMPORTQUERY})
query([[COMMIT]]);

CREATE OR REPLACE LUA SCRIPT OA_CNTIN."MEDICAL_CLAIM_PROD_LOAD" (CLIENTNAME) RETURNS ROWCOUNT AS
CLIENTID = query([[ SELECT CLIENT_ID FROM reference.client WHERE CLIENT_NAME = Lower(:cn) ]],{cn=CLIENTNAME})[1][1]
--query([[DELETE FROM OA_CNTIN.MST_MEDICAL_CLAIMS WHERE client_id = :ci]],{ci = CLIENTID})
query([[CREATE OR REPLACE TABLE OA_CNTIN.MST_MEDICAL_CLAIMS AS
SELECT c.client_id,
    c.client_name AS client_name,
    mc.axial_member_id,
    mc.client_member_id,
    mc.client_claim_key,
    mc.claim_id,
    mc.claim_line,
    mc.axial_claim_id,
    mc.axial_claim_line_id,
    mc.client_claim_status,
    mc.axial_claim_status,
    mc.original_claim_id,
    mc.claim_type,
    mc.axial_claim_type,
    mc.service_from_date,
    left(mc.service_from_date, 7) AS service_from_yyyy_mm,
    cast(left(mc.service_from_date, 7) || '-01' AS DATE) AS service_from_reporting_date,
    mc.service_to_date,
    left(mc.service_to_date, 7) AS service_to_yyyy_mm,
    CAST(left(mc.service_to_date, 7) || '-01' AS DATE) AS service_to_reporting_date,
    mc.date_received,
    mc.date_paid,
    left(mc.date_paid, 7) AS paid_yyyy_mm,
    CASE WHEN mc.date_paid IS NOT NULL THEN cast(left(mc.date_paid, 7) || '-01' AS DATE ) ELSE NULL END AS paid_reporting_date,
    mc.admit_date,
    left(mc.admit_date, 7) AS admit_yyyy_mm,
    CASE WHEN mc.admit_date IS NOT NULL THEN cast(left(mc.admit_date, 7) || '-01' AS date) ELSE NULL END AS admit_reporting_date,
    mc.admit_source,
    mc.discharge_date,
    left(mc.discharge_date, 7) AS discharge_yyyy_mm,
    CASE WHEN mc.discharge_date IS NOT NULL THEN cast(left(mc.discharge_date, 7) || '-01' AS date) ELSE NULL END AS discharge_reporting_date,
    mc.discharge_status,
    mc.servicing_provider_id,
    mc.servicing_provider_npi,
    mc.servicing_provider_ein,
    mc.billing_provider_npi,
    mc.billing_provider_ein,
    mc.par_flag,
    mc.servicing_provider_network,
    mc.referring_provider_npi,
    mc.auth_number,
    mc.icd_procedure_code_type,
    mc.icd_procedure_code,
    mc.place_of_service,
    pos.place_of_service_name,
    mc.axial_pos AS axial_place_of_service,
    apos.place_of_service_name AS axial_place_of_service_name,
    CAST(NULL AS varchar(256)) AS asam_lod,
    mc.bill_type,
    mc.drg_version,
    mc.drg_code,
    mc.procedure_code,
    mc.procedure_code_mod1,
    mc.procedure_code_mod2,
    mc.units,
    mc.revenue_code,
    mc.amount_billed,
    mc.amount_allowed,
    mc.amount_total_paid,
    mc.amount_copay,
    mc.amount_deductible,
    mc.amount_coinsurance,
    mc.amount_patient_paid,
    mc.amount_cob,
    mc.amount_insurance_paid,
    mc.lob,
    mc.plan_code AS plan_name,
    mcw.icd_type,
        CASE
            WHEN mc.client_id in (2, 3, 4, 11) THEN replace(mcw.primary_diagnosis_code, '.', '')
            ELSE replace(mcw.icd_code_1, '.', '')
        END AS icd_code_1,
        CASE
            WHEN mc.client_id in (2, 3, 4, 11) THEN replace(mcw.icd_code_1, '.', '')
            ELSE replace(mcw.icd_code_2, '.', '')
        END AS icd_code_2,
        CASE
            WHEN mc.client_id in (2, 3, 4, 11) THEN replace(mcw.icd_code_2, '.', '')
            ELSE replace(mcw.icd_code_3, '.', '')
        END AS icd_code_3,
        CASE
            WHEN mc.client_id in (2, 3, 4, 11) THEN replace(mcw.icd_code_3, '.', '')
            ELSE replace(mcw.icd_code_4, '.', '')
        END AS icd_code_4,
        CASE
            WHEN mc.client_id in (2, 3, 4, 11) THEN dxp.icd_short_description
            ELSE dx1.icd_short_description
        END AS icd_code_1_desc,
        CASE
            WHEN mc.client_id in (2, 3, 4, 11) THEN dx1.icd_short_description
            ELSE dx2.icd_short_description
        END AS icd_code_2_desc,
        CASE
            WHEN mc.client_id in (2, 3, 4, 11) THEN dx2.icd_short_description
            ELSE dx3.icd_short_description
        END AS icd_code_3_desc,
        CASE
            WHEN mc.client_id in (2, 3, 4, 11) THEN dx3.icd_short_description
            ELSE dx4.icd_short_description
        END AS icd_code_4_desc,
    mcw.admit_diagnosis,
    mc.med_econ_is_pain AS med_econ_is_pain_flag,
    mc.is_ed_detail AS ed_claim_flag,
    mc.axial_claim_id || '|' || mc.axial_claim_line_id AS axial_claim_id_line_id,
    mc.exasol_loadtime
   FROM OA_CNTIN.medical_claim mc
     INNER JOIN  REFERENCE.client c ON c.dim_id = mc.client_id
     LEFT JOIN OA_CNTIN.medical_claim_diagnosis_wide mcw ON mc.axial_claim_id = mcw.axial_claim_id AND mc.axial_claim_line_id = mcw.axial_claim_line_id AND mc.axial_member_id = mcw.axial_member_id
     LEFT JOIN REFERENCE.icd_diagnosis dxp ON replace(mcw.primary_diagnosis_code, '.', '') = dxp.icd_unformatted_code AND mcw.icd_type = dxp.icd_type
     LEFT JOIN REFERENCE.icd_diagnosis dx1 ON replace(mcw.icd_code_1, '.', '') = dx1.icd_unformatted_code AND mcw.icd_type = dx1.icd_type
     LEFT JOIN REFERENCE.icd_diagnosis dx2 ON replace(mcw.icd_code_2, '.', '') = dx2.icd_unformatted_code AND mcw.icd_type = dx2.icd_type
     LEFT JOIN REFERENCE.icd_diagnosis dx3 ON replace(mcw.icd_code_3, '.', '') = dx3.icd_unformatted_code AND mcw.icd_type = dx3.icd_type
     LEFT JOIN REFERENCE.icd_diagnosis dx4 ON replace(mcw.icd_code_4, '.', '') = dx4.icd_unformatted_code AND mcw.icd_type = dx4.icd_type
     LEFT JOIN REFERENCE.place_of_service pos ON pos.place_of_service_code = mc.place_of_service
     LEFT JOIN REFERENCE.place_of_service apos ON apos.place_of_service_code = mc.axial_pos
     WHERE c.client_id = :ci]],{ci=CLIENTID})
query([[DELETE FROM OA_CNTIN.MEDICAL_CLAIM_DX_LONG mcdl WHERE client_id = :ci]],{ci=CLIENTID})
query([[INSERT INTO OA_CNTIN.MEDICAL_CLAIM_DX_LONG 
	SELECT 
			c.CLIENT_ID ,
			c.CLIENT_NAME ,
			AXIAL_MEMBER_ID ,
			AXIAL_CLAIM_ID ,
			AXIAL_CLAIM_LINE_ID ,
			SERVICE_FROM_DATE,
			LEFT(SERVICE_FROM_DATE,7 )SERVICE_FROM_YYYY_MM,
			SERVICE_TO_DATE,
			LEFT(SERVICE_TO_DATE,7 ) SERVICE_TO_YYYY_MM,
			PLACE_OF_SERVICE ,
			ICD_TYPE ,
			ICD_POSITION_NAME ,
			ICD_POSITION ,
			ICD_FORMATTED_CODE ,
			ICD_UNFORMATTED_CODE ,
			FILE_LOG_ID ,
			RECORD_SOURCE_ID ,
			LOAD_TIMESTAMP ,
			AXIAL_MEMBER_ID_PARTITION ,
			MED_ECON_IS_PAIN,
			CASE 
				WHEN dx.CLIENT_ID IN (2, 3, 4, 11) AND ICD_POSITION = 0 THEN 1
				WHEN dx.CLIENT_ID NOT IN (2, 3, 4, 11) AND ICD_POSITION = 1 THEN 1
				ELSE 0
			END AS mst_medical_claims_primary_dx,
			exasol_loadtime
	FROM 
		 OA_CNTIN.MEDICAL_CLAIM_DIAGNOSIS_LONG dx
	INNER JOIN 
		REFERENCE.client c ON dx.CLIENT_ID = c.dim_id
	WHERE c.client_id = :ci]],{ci=CLIENTID})

CREATE OR replace LUA SCRIPT OA_CNTIN."RX_CLAIM_PROD_LOAD" (CLIENTNAME) RETURNS ROWCOUNT AS
CLIENTID = query([[ SELECT CLIENT_ID FROM reference.client WHERE CLIENT_NAME = Lower(:cn) ]],{cn=CLIENTNAME})[1][1]
--query([[DELETE FROM OA_CNTIN.MST_RX_CLAIMS WHERE client_id = :ci]],{ci = CLIENTID})
query([[CREATE OR REPLACE TABLE OA_CNTIN.mst_rx_claims AS
SELECT distinct c.client_id,
    c.client_name AS client_name,
    rx.axial_member_id,
    rx.client_member_id,
    rx.client_claim_key,
    rx.claim_id,
    rx.claim_line,
    rx.axial_claim_id,
    rx.axial_claim_line_id,
    rx.axial_prescription_fill_id,
    rx.client_claim_status,
    rx.axial_claim_status,
    rx.original_claim_id,
    rx.date_written,
    rx.service_from_date,
    left(rx.service_from_date, 7) AS service_from_yyyy_mm,
    left(rx.service_from_date, 7) || '-01' AS service_from_month,
    rx.service_to_date,
    left(rx.service_to_date, 7) AS service_to_yyyy_mm,
    left(rx.service_to_date, 7) || '-01' AS service_to_month,
    rx.prescription_number,
    rx.refill_number,
    rx.date_paid,
    rx.ndc,
    rx.quantity,
    rx.days_supply,
    rx.compound_code,
    ndc.ndc_brand_name AS brand_name,
    ndc.strength,
    rx.is_generic,
    rx.formulary_flag,
    rx.prescriber_id,
    rx.prescriber_npi,
    rx.pharmacy_id,
    rx.pharmacy_npi,
    rx.amount_ingredient_cost,
    rx.amount_dispensing_fee,
    rx.amount_copay,
    rx.amount_coinsurance,
    rx.amount_deductible,
    rx.amount_patient_paid,
    rx.amount_insurance_paid,
    rx.amount_total_paid,
    rx.amount_allowed,
    rx.amount_cob,
    rx.employer_group_number,
    rx.lob,
    rx.plan_code,
    rx.specialty_flag,
        CASE
            WHEN ndc.opioid_type = 'opioid_pain' THEN 1
            ELSE 0
        END AS is_opioid_flag,
        CASE
            WHEN ndc.opioid_type = 'opioid_MAT' THEN 1
            ELSE 0
        END AS is_mat_flag,
     rx.exasol_loadtime
   FROM OA_CNTIN.rx_claim rx
     JOIN reference.client c ON c.dim_id = rx.client_id
     LEFT JOIN REFERENCE.ndc_drug ndc ON rx.ndc = ndc.ndc_11_id
    WHERE c.client_id = :ci]],{ci = CLIENTID});

 SELECT * FROM reference.client ORDER BY 1 
   
CREATE LUA SCRIPT OA_CNTIN."ASL_TRACKER" () RETURNS ROWCOUNT AS
query([[CREATE OR REPLACE table OA_CNTIN.asl_tracker as
select 																																--Step 4
	 z.client_id
	,z.client_name
	,z.reporting_yyyy_mm
	,z.reporting_date
	,z.axial_member_id
	,z.lob
	,z.eligibility_flag
	,case when eligibility_flag = 0 and prev_month = 1 then 'Leaver'
		when eligibility_flag = 0  then null
		when eligibility_flag = 1 and coalesce(prev_month,1) = 1 then 'Stayer'
		when eligibility_flag = 1 and prev_month = 0 then 'Add'
 	end as status
 from																																--Step 3
	(select 
		 fl.*
		,lag(fl.eligibility_flag,1) over(partition by axial_member_id, lob order by reporting_yyyy_mm desc) as next_month
		,lead(fl.eligibility_flag,1) over(partition by axial_member_id, lob order by reporting_yyyy_mm desc) as prev_month
	from
		(select 																													--Step 2
			 base.*
			,case when elig.axial_member_id is null then 0 else 1 end as eligibility_flag 
		from 
			(select 																												--Step 1
				 CLIENT_ID
				,CLIENT_NAME
				,x.reporting_yyyy_mm
				,CAST(x.reporting_yyyy_mm||'-01' AS DATE ) AS reporting_date 
				,b.axial_member_id 
				,lob
			from 
				REFERENCE.dim_date_reporting x 
			cross join 
				(select DISTINCT CLIENT_ID, CLIENT_NAME, axial_member_id, lob from OA_CNTIN.MST_MEMBER_MONTH_BY_LOB ) b
			where 
				x.reporting_yyyy_mm <= (SELECT max(reporting_yyyy_mm) FROM OA_CNTIN.MST_MEMBER_MONTH_BY_LOB )
			) base
		left join
			(select distinct 
				 axial_member_id
				 ,lob
				,reporting_yyyy_mm
			from 
				OA_CNTIN.MST_MEMBER_MONTH_BY_LOB ) elig
		on elig.axial_member_id = base.axial_member_id and base.reporting_yyyy_mm= elig.reporting_yyyy_mm and COALESCE(base.lob, '0')= COALESCE(elig.lob,'0')
		) fl
	)z]]);

CREATE OR REPLACE LUA SCRIPT OA_CNTIN."ENHANCED_OUD" () RETURNS ROWCOUNT AS
query([[
CREATE or REPLACE table OA_CNTIN.ENHANCED_OUD AS 
WITH mat_rx_claims AS (
	SELECT DISTINCT 
    	CLIENT_ID, 
    	CLIENT_NAME, 
    	AXIAL_MEMBER_ID, 
    	CAST(ADHERENCE_YYYY_MM||'-01' AS DATE) AS SERVICE_FROM_DATE, 
    	ADHERENCE_YYYY_MM AS SERVICE_FROM_YYYY_MM, 
    	CLAIM_SERVICE_FROM_DATE, 
    	CAST(NULL AS VARCHAR(20)) AS PRESCRIBER_NPI, 
    	TX_TYPE AS NDC, 
    	TX_TYPE AS NDC_BRAND_NAME   
    FROM OA_CNTIN.MAT_ADHERENCE_DAYS AS mad
),
oud_dx_claims AS (
    SELECT DISTINCT 
         client_id
        ,client_name
        ,axial_member_id
        ,REPLACE(icd_code_1,'.','') AS oud_dx
        ,REPLACE(icd_code_1,'.','') AS icd_code_1
        ,REPLACE(icd_code_2,'.','') AS icd_code_2 
        ,REPLACE(icd_code_3,'.','') AS icd_code_3 
        ,REPLACE(icd_code_4,'.','') AS icd_code_4 
        ,service_from_date
        ,servicing_provider_npi
        ,place_of_service
        ,axial_place_of_service
        , 1 AS oud_dx_placement 
    FROM 
        OA_CNTIN.mst_medical_claims 
    WHERE 
        icd_type||'-'||REPLACE(icd_code_1,'.','')  IN  (SELECT icd_type||'-'||icd_unformatted_code FROM STAGING.ICD_DIAGNOSIS x WHERE x.IS_OPIOID_USE_DISORDER IS true) 
    UNION ALL 
    SELECT DISTINCT 
         client_id
        ,client_name
        ,axial_member_id
        ,REPLACE(icd_code_2,'.','') AS oud_dx
        ,REPLACE(icd_code_1,'.','') AS icd_code_1 
        ,REPLACE(icd_code_2,'.','') AS icd_code_2 
        ,REPLACE(icd_code_3,'.','') AS icd_code_3 
        ,REPLACE(icd_code_4,'.','') AS icd_code_4 
        ,service_from_date
        ,servicing_provider_npi
        ,place_of_service
        ,axial_place_of_service
        , 2 AS oud_dx_placement 
    FROM 
        OA_CNTIN.mst_medical_claims
    WHERE 
        icd_type||'-'||REPLACE(icd_code_2,'.','')  IN  (SELECT icd_type||'-'||icd_unformatted_code FROM STAGING.ICD_DIAGNOSIS x WHERE x.IS_OPIOID_USE_DISORDER IS true) 
        UNION ALL 
        SELECT DISTINCT 
         client_id
        ,client_name
        ,axial_member_id
        ,REPLACE(icd_code_3,'.','') AS oud_dx
        ,REPLACE(icd_code_1,'.','') AS icd_code_1 
        ,REPLACE(icd_code_2,'.','') AS icd_code_2 
        ,REPLACE(icd_code_3,'.','') AS icd_code_3 
        ,REPLACE(icd_code_4,'.','') AS icd_code_4 
        ,service_from_date
        ,servicing_provider_npi
        ,place_of_service
        ,axial_place_of_service
        , 3 AS oud_dx_placement 
    FROM 
        OA_CNTIN.mst_medical_claims
    WHERE 
        icd_type||'-'||REPLACE(icd_code_3,'.','')  IN  (SELECT icd_type||'-'||icd_unformatted_code FROM STAGING.ICD_DIAGNOSIS x WHERE x.IS_OPIOID_USE_DISORDER IS true) 
        UNION ALL 
    SELECT DISTINCT 
         client_id
        ,client_name
        ,axial_member_id
        ,REPLACE(icd_code_4,'.','') AS oud_dx
        ,REPLACE(icd_code_1,'.','') AS icd_code_1 
        ,REPLACE(icd_code_2,'.','') AS icd_code_2 
        ,REPLACE(icd_code_3,'.','') AS icd_code_3 
        ,REPLACE(icd_code_4,'.','') AS icd_code_4 
        ,service_from_date
        ,servicing_provider_npi
        ,place_of_service
        ,axial_place_of_service
        , 4 AS oud_dx_placement 
    FROM 
        OA_CNTIN.mst_medical_claims
    WHERE 
        icd_type||'-'||REPLACE(icd_code_4,'.','')  IN  (SELECT icd_type||'-'||icd_unformatted_code FROM STAGING.ICD_DIAGNOSIS x WHERE x.IS_OPIOID_USE_DISORDER IS true) 
        ),
remission_claim AS (
    SELECT DISTINCT 
         client_id
        ,client_name
        ,axial_member_id
        ,service_from_date
        ,SERVICE_from_YYYY_MM
        ,servicing_provider_npi
        ,place_of_service
        ,axial_place_of_service
        , CASE 
            WHEN REPLACE(icd_code_1,'.','')  IN  ('F1111','F1121') THEN 1
            WHEN REPLACE(icd_code_2,'.','')  IN  ('F1111','F1121') THEN 2 
            WHEN REPLACE(icd_code_3,'.','')  IN  ('F1111','F1121') THEN 3 
            WHEN REPLACE(icd_code_4,'.','')  IN  ('F1111','F1121') THEN 4 
        END AS rem_dx_placement
    FROM 
        OA_CNTIN.mst_medical_claims
    WHERE 
        (REPLACE(icd_code_1,'.','')  IN  ('F1111','F1121') 
        OR REPLACE(icd_code_2,'.','')  IN  ('F1111','F1121')  
        OR REPLACE(icd_code_3,'.','')  IN  ('F1111','F1121')  
        OR REPLACE(icd_code_4,'.','')  IN  ('F1111','F1121')))
SELECT 
     elig.client_id
    ,elig.client_name
    ,elig.axial_member_id
    ,elig.reporting_yyyy_mm 
    ,CAST(concat(elig.reporting_yyyy_mm,'-01')AS DATE) AS reporting_date
    ,elig.lob
    ,COALESCE(case when f11_diag.axial_member_id  is null and fmat.service_from_date is null then 0 else 1 END,0) as oud_flag
    ,COALESCE(case when moud.axial_member_id  is null and mat.axial_member_id is null then 0 else 1 END,0) as oud_current_month_flag
    ,COALESCE(CASE 
        when f11_diag.service_from_date is null and fmat.service_from_date is null then null
        when f11_diag.service_from_date <= fmat.service_from_date or (f11_diag.service_from_date is not null and fmat.service_from_date is null) then
            CASE WHEN left(f11_diag.service_from_date,7) <= elig.REPORTING_YYYY_MM AND f11_diag.service_from_date IS NOT NULL THEN 1 ELSE 0 end
        when f11_diag.service_from_date > fmat.service_from_date or (f11_diag.service_from_date is null and fmat.service_from_date is not null) THEN 
            case WHEN left(fmat.service_from_date,7) <= elig.REPORTING_YYYY_MM AND fmat.service_from_date IS NOT NULL  THEN 1 ELSE 0 end
        END , 0) as POST_FIRST_OUD_FLAG
    ,case WHEN  f11_diag.service_from_date <= fmat.service_from_date or (f11_diag.service_from_date is not null and fmat.service_from_date is null) then f11_diag.service_from_date else fmat.service_from_date END as FIRST_OUD_DATE
    ,CASE 
        when f11_diag.service_from_date is null and  fmat.service_from_date is null  then null 
        when f11_diag.service_from_date <= fmat.service_from_date or (f11_diag.service_from_date is not null and fmat.service_from_date is null)  then floor(MONTHS_BETWEEN(CAST(concat(elig.reporting_yyyy_mm,'-01')AS DATE),CAST(concat(LEFT(f11_diag.service_from_date,7),'-01') AS date)))
        when f11_diag.service_from_date > fmat.service_from_date or (f11_diag.service_from_date is null and fmat.service_from_date is not null)  then floor(MONTHS_BETWEEN(CAST(concat(elig.reporting_yyyy_mm,'-01')AS DATE),CAST(concat(LEFT(fmat.service_from_date,7),'-01') AS date)))
    end as MONTHS_BETWEEN_REPORTING_DATE_AND_FIRST_OUD_DATE
    ,f11_diag.oud_dx AS first_oud_dx
    ,f11_diag.servicing_provider_npi AS first_oud_dx_servicing_provider_npi
    ,f11_diag.service_from_date AS first_oud_dx_date
    ,CASE WHEN f11_diag.service_from_date IS NULL THEN NULL ELSE floor(MONTHS_BETWEEN(CAST(concat(elig.reporting_yyyy_mm,'-01')AS DATE),CAST(concat(LEFT(f11_diag.service_from_date,7),'-01') AS date))) END AS  MONTHS_BETWEEN_REPORTING_DATE_AND_FIRST_OUD_DX_DATE
    ,f11_diag.place_of_service AS first_oud_dx_pos_code
    ,f11_diag.axial_place_of_service as first_oud_dx_axial_pos_code
    ,f11_diag.axial_pos_name AS first_oud_dx_axial_pos_name
    ,f11_diag.provider_peer_group AS first_oud_dx_provider_peer_group
    ,f11_diag.oud_dx_placement AS first_oud_dx_placement
    ,f11_diag.icd_code_1 AS first_oud_dx_icd_code_1
    ,f11_diag.icd_code_2  AS first_oud_dx_icd_code_2
    ,f11_diag.icd_code_3  AS first_oud_dx_icd_code_3
    ,f11_diag.icd_code_4  AS first_oud_dx_icd_code_4
    ,COALESCE(CASE WHEN f11_diag.axial_member_id IS NULL THEN 0 ELSE 1 END,0) AS  oud_dx_flag
    ,COALESCE(CASE WHEN left(f11_diag.service_from_date,7) <= elig.REPORTING_YYYY_MM AND f11_diag.service_from_date IS NOT NULL THEN 1 ELSE 0 END,0) AS post_first_oud_dx_flag
    ,COALESCE(CASE WHEN moud.axial_member_id IS NULL THEN 0 else 1 END,0) AS oud_dx_current_month_flag
    ,fmat.claim_service_from_date AS first_mat_date
    ,CASE WHEN fmat.service_from_date IS NULL THEN NULL ELSE floor(MONTHS_BETWEEN(CAST(concat(elig.reporting_yyyy_mm,'-01')AS DATE),CAST(concat(LEFT(fmat.service_from_date,7),'-01') AS date))) END AS MONTHS_BETWEEN_REPORTING_DATE_AND_FIRST_MAT_DATE
    ,COALESCE(CASE WHEN fmat.service_from_date IS NULL THEN 0 ELSE 1 END,0) AS mat_flag
    ,COALESCE(CASE WHEN left(fmat.service_from_date,7) <= elig.REPORTING_YYYY_MM AND fmat.service_from_date IS NOT NULL  THEN 1 ELSE 0 END,0) as post_first_mat_flag
    ,COALESCE(CASE WHEN mat.axial_member_id IS NULL THEN 0 ELSE 1 END,0) AS  mat_current_month_flag
    ,fmat.prescriber_npi AS first_mat_provider_npi
    ,fmat.PROVIDER_PEER_GROUP as first_mat_provider_npi_peer_group
    ,COALESCE(elig.member_eligible_flag_current_month,0) AS eligible_flag_current_month
    ,COALESCE(elig.member_eligible_flag_previous_month,0) AS eligible_flag_previous_month
    ,elig.member_current_eligibility_status AS current_eligibility_status
    ,frem.service_from_date AS first_remission_dx_date 
    ,frem.place_of_service AS first_remission_dx_pos_code
    ,frem.axial_place_of_service AS first_remission_dx_axial_pos_code
    ,frem.axial_pos_name AS first_remission_dx_axial_pos_name
    ,COALESCE(CASE WHEN frem.axial_member_id IS NOT NULL THEN 1 ELSE 0 END,0) AS remission_flag
    ,COALESCE(CASE WHEN left(frem.service_from_date,7) <= elig.REPORTING_YYYY_MM AND frem.service_from_date IS NOT NULL THEN 1 ELSE 0 END,0) AS post_first_remission_flag
    ,COALESCE(eol.MEMBER_CANCER_FLAG,0) AS MEMBER_CANCER_FLAG
    ,COALESCE(eol.MEMBER_HOSPICE_FLAG,0) AS MEMBER_HOSPICE_FLAG
    ,COALESCE(eollt.MEMBER_CANCER_FLAG,0) AS MEMBER_lifetime_CANCER_FLAG
    ,COALESCE(eollt.MEMBER_HOSPICE_FLAG,0) AS MEMBER_lifetime_HOSPICE_FLAG
    ,COALESCE(CASE WHEN nalrx.axial_member_id IS NULL THEN 0 ELSE 1 END ,0) AS current_month_naloxone_rx_flag
    ,COALESCE(nalrx.naloxone_rx_count, 0) AS current_month_naloxone_rx_count 
    ,COALESCE(mbrr.MEMBER_MEDD ,0 ) AS member_medd 
FROM (SELECT 
    x.client_id
    ,x.client_name 
    ,x.axial_member_id 
    ,x.reporting_yyyy_mm 
    ,x.lob
    ,CASE WHEN x.status IN ('Stayer', 'Add') AND x.status  IS NOT NULL  THEN 1 ELSE 0 END AS  member_eligible_flag_current_month
    ,CASE WHEN x.status IN ('Stayer', 'Leaver') AND x.status  IS NOT NULL  THEN 1 ELSE 0 END AS member_eligible_flag_previous_month
    ,x.status AS member_current_eligibility_status 
    FROM OA_CNTIN.asl_tracker x WHERE status IN ('Add', 'Stayer')) elig
LEFT JOIN (
    SELECT 
         x.client_name
        ,x.axial_member_id
        ,x.oud_dx
        ,x.icd_code_1 
        ,x.icd_code_2 
        ,x.icd_code_3 
        ,x.icd_code_4 
        ,x.servicing_provider_npi
        ,x.place_of_service
        ,x.axial_place_of_service
        ,pos.PLACE_OF_SERVICE_NAME AS axial_pos_name
        ,pos.PLACE_OF_SERVICE_DESCRIPTION AS axial_pos_description
        ,x.service_from_date
        ,x.oud_dx_placement
        ,pr.provider_peer_group
    FROM (
        SELECT 
             z.client_name
            ,z.axial_member_id
            ,z.oud_dx
            ,z.icd_code_1 
            ,z.icd_code_2 
            ,z.icd_code_3 
            ,z.icd_code_4 
            ,z.servicing_provider_npi
            ,z.place_of_service
            ,z.axial_place_of_service
            ,z.service_from_date
            ,z.oud_dx_placement
            ,left(z.service_from_date,7) AS reporting_yyyy_mm
            ,ROW_NUMBER() OVER (PARTITION BY z.client_name,z.axial_member_id ORDER BY z.service_from_date asc, oud_dx_placement ASC, axial_place_of_service ASC) oud_order
        FROM oud_dx_claims z
        ) x
    LEFT JOIN 
    (
    SELECT 
         pri.client_name 
        ,pri.provider_npi 
        ,pri.reporting_yyyy_mm 
        ,pri.provider_peer_group  
    FROM OA_CNTIN.MST_PROVIDER_RESULTS pri        
    ) pr ON x.reporting_yyyy_mm=pr.reporting_yyyy_mm AND x.client_name=pr.client_name AND x.servicing_provider_npi = pr.provider_npi
    LEFT JOIN 
    STAGING.PLACE_OF_SERVICE pos ON x.axial_place_of_service=pos.PLACE_OF_SERVICE_CODE
    WHERE 
        oud_order = 1
    ) f11_diag ON elig.client_name = f11_diag.client_name AND elig.axial_member_id = f11_diag.axial_member_id
LEFT JOIN 
    (select     
            axial_member_id
            ,left(service_from_date,7) AS reporting_yyyy_mm
        FROM oud_dx_claims GROUP BY 1,2) moud
        ON elig.axial_member_id = moud.axial_member_id AND elig.reporting_yyyy_mm=moud.reporting_yyyy_mm
LEFT JOIN (
    SELECT 
         q.client_name
        ,q.axial_member_id
        ,q.prescriber_npi
        ,q.service_from_date
        ,q.claim_service_from_date
        ,q.reporting_yyyy_mm
        ,mpr.PROVIDER_PEER_GROUP
    FROM (
        SELECT DISTINCT 
             client_name
            ,axial_member_id
            ,service_from_yyyy_mm reporting_yyyy_mm
            ,claim_service_from_date
            ,prescriber_npi
            ,service_from_date
            ,ROW_NUMBER() OVER ( PARTITION BY client_name,axial_member_id ORDER BY claim_service_from_date ASC, service_from_yyyy_mm asc) mat_order
        FROM 
            mat_rx_claims 
        ) q
    LEFT JOIN
        OA_CNTIN.MST_PROVIDER_RESULTS mpr ON q.reporting_yyyy_mm=mpr.REPORTING_YYYY_MM AND q.CLIENT_NAME=mpr.CLIENT_NAME AND q.prescriber_npi=mpr.PROVIDER_NPI
    WHERE mat_order = 1
    ) fmat ON elig.client_name = fmat.client_name AND elig.axial_member_id = fmat.axial_member_id
LEFT JOIN 
        (SELECT DISTINCT 
             mti.client_name
            ,mti.axial_member_id
            ,mti.service_from_yyyy_mm AS reporting_yyyy_mm
        FROM mat_rx_claims mti
        ) mat ON elig.client_name = mat.client_name AND elig.axial_member_id = mat.axial_member_id AND elig.REPORTING_YYYY_MM= mat.reporting_yyyy_mm 
LEFT JOIN 
    (SELECT 
        in_rem.axial_member_id
        ,in_rem.place_of_service
        ,in_rem.axial_place_of_service
        ,pos.PLACE_OF_SERVICE_NAME AS axial_pos_name
        ,pos.PLACE_OF_SERVICE_DESCRIPTION AS axial_pos_description
        ,in_rem.service_from_date
        ,in_rem.rem_dx_placement
    from(select DISTINCT
            axial_member_id
            ,service_from_yyyy_mm AS reporting_yyyy_mm
            ,service_from_date
            ,PLACE_OF_SERVICE
            ,AXIAL_PLACE_OF_SERVICE
            ,rem_dx_placement
            ,ROW_NUMBER() OVER ( PARTITION BY axial_member_id ORDER BY service_from_date ASC) AS rem_order
            FROM remission_claim) in_rem
            LEFT JOIN 
                 STAGING.PLACE_OF_SERVICE pos ON in_rem.axial_place_of_service=pos.PLACE_OF_SERVICE_CODE
            WHERE rem_order = 1 
            ) frem ON frem.axial_member_id=elig.axial_member_id
LEFT JOIN 
    OA_CNTIN.MEMBER_EOL eol ON elig.axial_member_id=eol.AXIAL_MEMBER_ID AND eol.REPORTING_YYYY_MM=elig.REPORTING_YYYY_MM
LEFT JOIN 
	(SELECT 
		AXIAL_MEMBER_ID, 
		max(MEMBER_HOSPICE_FLAG) AS MEMBER_HOSPICE_FLAG, 
		max(MEMBER_CANCER_FLAG) AS MEMBER_CANCER_FLAG 
	FROM 
		OA_CNTIN.MEMBER_EOL GROUP BY 1) eollt ON elig.axial_member_id=eollt.AXIAL_MEMBER_ID
LEFT JOIN 
	(SELECT CLIENT_ID, SERVICE_FROM_YYYY_MM AS REPORTING_YYYY_MM , AXIAL_MEMBER_ID, count(*) AS naloxone_rx_count FROM OA_CNTIN.MST_RX_CLAIMS mrc 
		INNER JOIN STAGING.NDC_DRUG  ndc ON mrc.NDC = ndc.NDC_11_ID  WHERE ndc.IS_NALOXONE IS TRUE GROUP BY 1,2,3 ) nalrx
	ON elig.axial_member_id=nalrx.axial_member_id AND nalrx.REPORTING_YYYY_MM=elig.REPORTING_YYYY_MM
LEFT JOIN OA_CNTIN.MST_MEMBER_RESULTS mbrr
 ON mbrr.AXIAL_MEMBER_ID = elig.axial_member_id AND mbrr.REPORTING_YYYY_MM = elig.REPORTING_YYYY_MM 
]]);

CREATE OR REPLACE LUA SCRIPT OA_CNTIN."ENHANCED_SUD" () RETURNS ROWCOUNT AS
query([[
CREATE OR REPLACE TABLE OA_CNTIN.ENHANCED_SUD AS 
WITH 
sud_claims AS ( 
    SELECT DISTINCT 
         client_id 
        ,client_name 
        ,axial_member_id 
        ,REPLACE(icd_code_1,'.','') AS sud_dx 
        ,REPLACE(icd_code_1,'.','') AS icd_code_1
        ,REPLACE(icd_code_2,'.','') AS icd_code_2 
        ,REPLACE(icd_code_3,'.','') AS icd_code_3 
        ,REPLACE(icd_code_4,'.','') AS icd_code_4 
        ,service_from_date 
        ,servicing_provider_npi 
        ,place_of_service 
        ,axial_place_of_service 
        , 1 AS sud_dx_placement  
    FROM 
        OA_CNTIN.mst_medical_claims 
    WHERE 
        icd_type||'-'||REPLACE(icd_code_1,'.','')  IN  (SELECT icd_type||'-'||icd_unformatted_code FROM STAGING.ICD_DIAGNOSIS x WHERE x.IS_SUD IS TRUE ) 
    UNION ALL 
    SELECT DISTINCT 
         client_id
        ,client_name
        ,axial_member_id
        ,REPLACE(icd_code_2,'.','') AS sud_dx
        ,REPLACE(icd_code_1,'.','') AS icd_code_1 
        ,REPLACE(icd_code_2,'.','') AS icd_code_2 
        ,REPLACE(icd_code_3,'.','') AS icd_code_3 
        ,REPLACE(icd_code_4,'.','') AS icd_code_4 
        ,service_from_date
        ,servicing_provider_npi
        ,place_of_service
        ,axial_place_of_service
        , 2 AS sud_dx_placement 
    FROM 
        OA_CNTIN.mst_medical_claims
    WHERE 
        icd_type||'-'||REPLACE(icd_code_2,'.','')  IN  (SELECT icd_type||'-'||icd_unformatted_code FROM STAGING.ICD_DIAGNOSIS x WHERE x.IS_SUD IS TRUE ) 
        UNION ALL 
        SELECT DISTINCT 
         client_id
        ,client_name
        ,axial_member_id
        ,REPLACE(icd_code_3,'.','') AS sud_dx
        ,REPLACE(icd_code_1,'.','') AS icd_code_1 
        ,REPLACE(icd_code_2,'.','') AS icd_code_2 
        ,REPLACE(icd_code_3,'.','') AS icd_code_3 
        ,REPLACE(icd_code_4,'.','') AS icd_code_4 
        ,service_from_date
        ,servicing_provider_npi
        ,place_of_service
        ,axial_place_of_service
        , 3 AS sud_dx_placement 
    FROM 
        OA_CNTIN.mst_medical_claims
    WHERE 
        icd_type||'-'||REPLACE(icd_code_3,'.','')  IN  (SELECT icd_type||'-'||icd_unformatted_code FROM STAGING.ICD_DIAGNOSIS x WHERE x.IS_SUD IS TRUE) 
        UNION ALL 
        SELECT DISTINCT 
         client_id
        ,client_name
        ,axial_member_id
        ,REPLACE(icd_code_4,'.','') AS sud_dx
        ,REPLACE(icd_code_1,'.','') AS icd_code_1 
        ,REPLACE(icd_code_2,'.','') AS icd_code_2 
        ,REPLACE(icd_code_3,'.','') AS icd_code_3 
        ,REPLACE(icd_code_4,'.','') AS icd_code_4 
        ,service_from_date
        ,servicing_provider_npi
        ,place_of_service
        ,axial_place_of_service
        , 4 AS sud_dx_placement 
    FROM 
        OA_CNTIN.mst_medical_claims
    WHERE 
        icd_type||'-'||REPLACE(icd_code_4,'.','')  IN  (SELECT icd_type||'-'||icd_unformatted_code FROM STAGING.ICD_DIAGNOSIS x WHERE x.IS_SUD IS TRUE))
SELECT 
		 base.CLIENT_ID  
		,base.CLIENT_NAME 
		,base.AXIAL_MEMBER_ID  
		,base.REPORTING_YYYY_MM  
		,base.REPORTING_DATE 
		,base.LOB 
		,COALESCE(CASE 
			WHEN sud_diag.axial_member_id IS NOT NULL OR oud_flag= 1 THEN 1 ELSE 0 
		END,0) AS SUD_FLAG
		,COALESCE(CASE 
			WHEN base.POST_FIRST_OUD_FLAG = 1 THEN 1
			WHEN cast(LEFT(CAST(COALESCE(sud_diag.service_from_date,'2999-01-01') AS VARCHAR(256)), 7)||'-01' AS date) <= base.REPORTING_DATE THEN 1
			ELSE 0
		END,0) AS post_first_sud_flag
		,CASE 	
			WHEN sud_diag.service_from_date IS NULL  AND FIRST_OUD_DATE IS NULL THEN NULL 
			WHEN sud_diag.service_from_date <= COALESCE(FIRST_OUD_DATE,'2999-12-25') THEN sud_diag.service_from_date 
			WHEN COALESCE(sud_diag.service_from_date,'2999-10-31') > FIRST_OUD_DATE THEN FIRST_OUD_DATE 
		END AS FIRST_SUD_DATE
        ,sud_diag.sud_dx AS FIRST_SUD_DX
        ,sud_diag.servicing_provider_npi AS FIRST_SUD_DX_SERVICING_PROVIDER_NPI 
        ,sud_diag.service_from_date AS FIRST_sud_dx_date
        ,sud_diag.place_of_service AS FIRST_SUD_DX_POS_CODE 
    	,sud_diag.axial_place_of_service AS FIRST_SUD_DX_AXIAL_POS_CODE 
    	,sud_diag.axial_pos_name AS FIRST_SUD_DX_AXIAL_POS_NAME
    	,sud_diag.provider_peer_group AS FIRST_SUD_DX_PROVIDER_PEER_GROUP
        ,sud_diag.icd_code_1 AS FIRST_SUD_DX_ICD_CODE_1
        ,sud_diag.icd_code_2  AS FIRST_SUD_DX_ICD_CODE_2
        ,sud_diag.icd_code_3 AS FIRST_SUD_DX_ICD_CODE_3
        ,sud_diag.icd_code_4 AS FIRST_SUD_DX_ICD_CODE_4
		,COALESCE(CASE
			WHEN sud_diag.axial_member_id IS NULL THEN 0 ELSE 1 
		END,0) AS sud_dx_flag
		,COALESCE(CASE
			WHEN sud_diag.service_from_date IS NULL THEN 0
			WHEN cast(LEFT(CAST(COALESCE(sud_diag.service_from_date,'2999-01-01') AS VARCHAR(256)), 7)||'-01' AS date) <= base.reporting_date THEN 1 ELSE 0 
		END,0) AS POST_FIRST_SUD_DX_FLAG
		,COALESCE(CASE WHEN msud.axial_member_id IS NULL THEN 0 ELSE 1 END,0) AS SUD_DX_CURRENT_MONTH_FLAG
		,CASE WHEN 
			COALESCE(CASE WHEN msud.axial_member_id IS NULL THEN 0 ELSE 1 END,0) = 1 or COALESCE (OUD_CURRENT_MONTH_FLAG,0) = 1 THEN 1 ELSE 0 
		END AS SUD_CURRENT_MONTH_FLAG			
		,OUD_FLAG --
		,OUD_CURRENT_MONTH_FLAG--
		,POST_FIRST_OUD_FLAG--
		,FIRST_OUD_DATE --
		,MONTHS_BETWEEN_REPORTING_DATE_AND_FIRST_OUD_DATE --
		,FIRST_OUD_DX --
		,FIRST_OUD_DX_SERVICING_PROVIDER_NPI --
		,FIRST_OUD_DX_DATE --
		,MONTHS_BETWEEN_REPORTING_DATE_AND_FIRST_OUD_DX_DATE --
		,FIRST_OUD_DX_POS_CODE --
		,FIRST_OUD_DX_AXIAL_POS_CODE --
		,FIRST_OUD_DX_AXIAL_POS_NAME --
		,FIRST_OUD_DX_PROVIDER_PEER_GROUP --
		,FIRST_OUD_DX_PLACEMENT --
		,FIRST_OUD_DX_ICD_CODE_1 --
		,FIRST_OUD_DX_ICD_CODE_2 --
		,FIRST_OUD_DX_ICD_CODE_3 --
		,FIRST_OUD_DX_ICD_CODE_4 --
		,OUD_DX_FLAG --
		,COALESCE(POST_FIRST_OUD_DX_FLAG,0) AS POST_FIRST_OUD_DX_FLAG --
		,OUD_DX_CURRENT_MONTH_FLAG --
		,FIRST_MAT_DATE 
		,MONTHS_BETWEEN_REPORTING_DATE_AND_FIRST_MAT_DATE 
		,MAT_FLAG
		,POST_FIRST_MAT_FLAG
		,MAT_CURRENT_MONTH_FLAG
		,FIRST_MAT_PROVIDER_NPI 
		,FIRST_MAT_PROVIDER_NPI_PEER_GROUP 
		,ELIGIBLE_FLAG_CURRENT_MONTH
		,ELIGIBLE_FLAG_PREVIOUS_MONTH
		,CURRENT_ELIGIBILITY_STATUS
		,FIRST_REMISSION_DX_DATE
		,FIRST_REMISSION_DX_POS_CODE 
		,FIRST_REMISSION_DX_AXIAL_POS_CODE 
		,FIRST_REMISSION_DX_AXIAL_POS_NAME 
		,REMISSION_FLAG
		,POST_FIRST_REMISSION_FLAG
		,COALESCE(MEMBER_CANCER_FLAG,0) AS MEMBER_CANCER_FLAG
		,COALESCE(MEMBER_HOSPICE_FLAG,0) AS MEMBER_HOSPICE_FLAG
		,COALESCE(MEMBER_LIFETIME_CANCER_FLAG,0) AS MEMBER_LIFETIME_CANCER_FLAG
		,COALESCE(MEMBER_LIFETIME_HOSPICE_FLAG,0) AS MEMBER_LIFETIME_HOSPICE_FLAG
		,base.CURRENT_MONTH_NALOXONE_RX_FLAG 
		,BASE.CURRENT_MONTH_NALOXONE_RX_COUNT 
		,nal12.naloxone_12_month_rx_count
		,base.MEMBER_MEDD 	
FROM 
	OA_CNTIN.ENHANCED_OUD base
LEFT JOIN 
	(SELECT  x.client_name
        ,x.axial_member_id
        ,x.sud_dx
        ,x.icd_code_1 
        ,x.icd_code_2 
        ,x.icd_code_3 
        ,x.icd_code_4 
        ,x.servicing_provider_npi
        ,x.place_of_service
        ,x.axial_place_of_service
        ,pos.PLACE_OF_SERVICE_NAME AS axial_pos_name
        ,pos.PLACE_OF_SERVICE_DESCRIPTION AS axial_pos_description
        ,x.service_from_date
        ,x.sud_dx_placement
        ,pr.provider_peer_group
        ,pos.place_of_service_description 
        FROM (
        SELECT 
             z.client_name
            ,z.axial_member_id
            ,z.sud_dx
            ,z.icd_code_1 
            ,z.icd_code_2 
            ,z.icd_code_3 
            ,z.icd_code_4 
            ,z.servicing_provider_npi
            ,z.place_of_service
            ,z.axial_place_of_service
            ,z.service_from_date
            ,z.sud_dx_placement
            ,left(z.service_from_date,7) AS reporting_yyyy_mm
            ,ROW_NUMBER() OVER (PARTITION BY z.client_name,z.axial_member_id ORDER BY z.service_from_date asc, sud_dx_placement ASC, axial_place_of_service ASC) sud_order
        FROM SUD_CLAIMS z
        ) x 
        LEFT JOIN 
    STAGING.PLACE_OF_SERVICE pos ON x.axial_place_of_service=pos.PLACE_OF_SERVICE_CODE
    LEFT JOIN 
    (
    SELECT 
         pri.client_name 
        ,pri.provider_npi 
        ,pri.reporting_yyyy_mm 
        ,pri.provider_peer_group  
    FROM OA_CNTIN.MST_PROVIDER_RESULTS pri        
    ) pr ON x.reporting_yyyy_mm=pr.reporting_yyyy_mm AND x.client_name=pr.client_name AND x.servicing_provider_npi = pr.provider_npi
    WHERE x.sud_order = 1
    ) sud_diag ON base.client_name = sud_diag.client_name AND base.axial_member_id = sud_diag.axial_member_id
LEFT JOIN 
    (select    
            axial_member_id
            ,left(service_from_date,7) AS reporting_yyyy_mm
        FROM sud_claims GROUP BY 1,2) msud
          ON base.axial_member_id = msud.axial_member_id AND base.reporting_yyyy_mm=msud.reporting_yyyy_mm
LEFT JOIN 
	(SELECT 
		a.AXIAL_MEMBER_ID, 
		a.REPORTING_YYYY_MM, 
		sum(b.CURRENT_MONTH_NALOXONE_RX_FLAG ) AS naloxone_12_month_rx_count 
	FROM 
		OA_CNTIN.ENHANCED_OUD a 
	LEFT JOIN 
		OA_CNTIN.ENHANCED_OUD b  ON a.AXIAL_MEMBER_ID =b.AXIAL_MEMBER_ID AND b.REPORTING_date <= a.REPORTING_DATE AND b.REPORTING_DATE >= ADD_MONTHS(a.REPORTING_DATE, -11 )
	GROUP BY 1,2) nal12 ON nal12.axial_member_id = base.AXIAL_MEMBER_ID  AND nal12.reporting_yyyy_mm = base.REPORTING_YYYY_MM 
]]);

CREATE OR REPLACE LUA SCRIPT OA_CNTIN."KPI_SUMMARY_DETAIL" () RETURNS ROWCOUNT AS
query([[CREATE OR REPLACE TABLE zz_crb.kpi_time_test AS 
SELECT now() AS time_info, 'start' AS p_acction ]])
query([[create or replace table OA_CNTIN.DRUG_TESTED_MEMBERS AS 
SELECT DISTINCT
	axial_member_id,
	mc.SERVICE_FROM_YYYY_MM AS assessed_through_yyyy_mm
FROM
	OA_CNTIN.MST_MEDICAL_CLAIMS AS MC
INNER JOIN
 (SELECT HCPCS FROM STAGING.PROCEDURE_CODE WHERE IS_DRUG_TEST = true) PC
ON PC.HCPCS = MC.PROCEDURE_CODE]])
query([[
CREATE OR REPLACE TABLE OA_CNTIN.ANALGESIC_CLAIMS as  
SELECT DISTINCT 
	CLIENT_ID, CLIENT_NAME , SERVICE_FROM_YYYY_MM AS REPORTING_YYYY_MM, AXIAL_MEMBER_ID 
FROM 
	OA_CNTIN.MST_RX_CLAIMS rx 
INNER JOIN 
	STAGING.NDC_DRUG ndc  
	ON ndc.NDC_11_ID =rx.NDC 
WHERE IS_NON_OPIOID_PAIN_MEDICATION is true 
]])
query([[
CREATE OR REPLACE TABLE OA_CNTIN.OUD_TWELVE_MONTH  AS 
SELECT DISTINCT 
	AXIAL_MEMBER_ID, 
	REPORTING_DATE
FROM 
	OA_CNTIN.ENHANCED_OUD eoud 
WHERE EXISTS 
	(SELECT 1 
	FROM 
		OA_CNTIN.ENHANCED_OUD ec 
	WHERE 
		eoud.AXIAL_MEMBER_ID =ec.AXIAL_MEMBER_ID 
		AND ec.oud_current_month_flag = 1
		AND ec.REPORTING_DATE BETWEEN add_months(eoud.REPORTING_DATE, -11) AND eoud.reporting_date )
]])
query([[	
CREATE OR REPLACE TABLE OA_CNTIN.SUD_TWELVE_MONTH  AS 
SELECT DISTINCT 
	AXIAL_MEMBER_ID, 
	REPORTING_DATE
FROM 
	OA_CNTIN.ENHANCED_SUD esud 
WHERE EXISTS 
	(SELECT 1 
	FROM 
		OA_CNTIN.ENHANCED_SUD ec 
	WHERE 
		esud.AXIAL_MEMBER_ID =ec.AXIAL_MEMBER_ID 
		AND ec.sud_current_month_flag = 1
		AND ec.REPORTING_DATE BETWEEN add_months(esud.REPORTING_DATE, -11) AND esud.reporting_date )
]])
query([[COMMIT]])
query([[CREATE OR REPLACE TABLE OA_CNTIN.ROLLING_OPIOID_PROVIDER_COUNT  AS 
SELECT 
		KSD.REPORTING_DATE,
		KSD.AXIAL_MEMBER_ID,
		COUNT(DISTINCT CASE WHEN MMPX.MEMBER_PROVIDER_OPIOID_PAIN_FLAG=1 THEN MMPX.PROVIDER_NPI ELSE NULL END)AS OPIOID_PROVIDER_COUNT
	FROM 
		(SELECT  * FROM OA_CNTIN.ASL_TRACKER at2 WHERE ELIGIBILITY_FLAG  = 1) ksd
	LEFT JOIN 
		OA_CNTIN.MST_MEMBER_PROVIDER_XREF mmpx ON KSD.AXIAL_MEMBER_ID=MMPX.AXIAL_MEMBER_ID
	WHERE 
		MMPX.REPORTING_DATE BETWEEN ADD_MONTHS(KSD.REPORTING_DATE,-6) AND KSD.REPORTING_DATE
	GROUP BY 1,2]])
query([[COMMIT]])
query([[
CREATE OR REPLACE TABLE  OA_CNTIN.KPI_MEDCLAIM_PREP AS 
SELECT 
			medclaims.AXIAL_MEMBER_ID,
			medclaims.SERVICE_FROM_REPORTING_DATE,
			medclaims.SERVICE_FROM_DATE,
			medclaims.LOB,
			AXIAL_PLACE_OF_SERVICE,
			CASE WHEN AXIAL_PLACE_OF_SERVICE = '21' AND REVENUE_CODE IN ('0200','0201','0202','0203','0204','0205','0206','0207','0208','0209') 
				 THEN 1 ELSE NULL END AS ICU_VISIT,
			CASE WHEN AXIAL_PLACE_OF_SERVICE = '21' AND REVENUE_CODE IN ('0118','0128','0138','0148','0158') 
				 THEN 1 ELSE NULL END AS IRF_VISIT,						 
			(MEDCLAIMS.SERVICE_FROM_DATE||MEDCLAIMS.AXIAL_MEMBER_ID||MEDCLAIMS.AXIAL_PLACE_OF_SERVICE) AS VISIT_ID,
			/*(CASE WHEN (icd1.IS_OPIOID_OVERDOSE=true OR icd2.IS_OPIOID_OVERDOSE=TRUE 
					OR icd3.IS_OPIOID_OVERDOSE=TRUE  OR icd4.IS_OPIOID_OVERDOSE=TRUE) AND AXIAL_PLACE_OF_SERVICE = '23' THEN 1 ELSE 0
				END) AS OPIOID_OVERDOSE_FLAG,  
			MAX((CASE WHEN (icd1.IS_OPIOID_OVERDOSE=true OR icd2.IS_OPIOID_OVERDOSE=TRUE 
					OR icd3.IS_OPIOID_OVERDOSE=TRUE  OR icd4.IS_OPIOID_OVERDOSE=TRUE 
					OR icd1.IS_DRUG_OVERDOSE=true OR icd2.IS_DRUG_OVERDOSE=true
					OR icd3.IS_DRUG_OVERDOSE=true OR icd4.IS_DRUG_OVERDOSE=true
					OR icd1.IS_BENZODIAZEPINE_OVERDOSE =true OR icd2.IS_BENZODIAZEPINE_OVERDOSE =true
					OR icd3.IS_BENZODIAZEPINE_OVERDOSE =true OR icd4.IS_BENZODIAZEPINE_OVERDOSE =true) AND AXIAL_PLACE_OF_SERVICE = '23' THEN 1 ELSE 0
				END)) AS ANY_DRUG_OVERDOSE_FLAG, */
			(CASE WHEN ((icd1.IS_OPIOID_OVERDOSE=TRUE AND icd1.is_drug_overdose_of_interest = TRUE) OR 
						(icd2.IS_OPIOID_OVERDOSE=TRUE AND icd2.is_drug_overdose_of_interest = TRUE) OR 
						(icd3.IS_OPIOID_OVERDOSE=TRUE  AND icd3.is_drug_overdose_of_interest = TRUE) OR 
						(icd4.IS_OPIOID_OVERDOSE=TRUE AND icd4.is_drug_overdose_of_interest = TRUE)) AND 
						 AXIAL_PLACE_OF_SERVICE = '23' THEN 1 ELSE 0 END) AS OPIOID_OVERDOSE_FLAG,   
			MAX((CASE WHEN ((icd1.IS_OPIOID_OVERDOSE=true AND icd1.is_drug_overdose_of_interest = TRUE) OR 
				(icd2.IS_OPIOID_OVERDOSE=TRUE AND icd2.is_drug_overdose_of_interest = TRUE) OR 
				(icd3.IS_OPIOID_OVERDOSE=TRUE AND icd3.is_drug_overdose_of_interest = TRUE) OR 
				(icd4.IS_OPIOID_OVERDOSE=TRUE AND icd4.is_drug_overdose_of_interest = TRUE) OR 
				(icd1.IS_DRUG_OVERDOSE=true AND icd1.is_drug_overdose_of_interest = TRUE) OR 
				(icd2.IS_DRUG_OVERDOSE=TRUE AND icd2.is_drug_overdose_of_interest = TRUE) OR 
				(icd3.IS_DRUG_OVERDOSE=true AND icd3.is_drug_overdose_of_interest = TRUE) OR 
				(icd4.IS_DRUG_OVERDOSE=TRUE AND icd4.is_drug_overdose_of_interest = TRUE) OR 
				(icd1.IS_BENZODIAZEPINE_OVERDOSE =true AND icd1.is_drug_overdose_of_interest = TRUE) OR 
				(icd2.IS_BENZODIAZEPINE_OVERDOSE =TRUE AND icd2.is_drug_overdose_of_interest = TRUE) OR 
				(icd3.IS_BENZODIAZEPINE_OVERDOSE =true AND icd3.is_drug_overdose_of_interest = TRUE) OR 
				(icd4.IS_BENZODIAZEPINE_OVERDOSE =TRUE AND icd4.is_drug_overdose_of_interest = TRUE)) AND 
				AXIAL_PLACE_OF_SERVICE = '23' THEN 1 ELSE 0 END)) AS ANY_DRUG_OVERDOSE_FLAG , 
			--SUM(IFNULL(AMOUNT_INSURANCE_PAID,0)) AS AMOUNT_INSURANCE_PAID,
			SUM(IFNULL(AMOUNT_INSURANCE_PAID,0)) AS AMOUNT_INSURANCE_PAID,
			SUM(IFNULL(AMOUNT_TOTAL_PAID,0)) AS AMOUNT_TOTAL_PAID,
			SUM(IFNULL(AMOUNT_ALLOWED,0)) AS AMOUNT_ALLOWED
		FROM OA_CNTIN.MST_MEDICAL_CLAIMS AS medclaims
		LEFT JOIN STAGING.ICD_DIAGNOSIS AS icd1 ON REPLACE(medclaims.icd_code_1,'.','')=icd1.ICD_UNFORMATTED_CODE AND medclaims.ICD_TYPE=icd1.ICD_TYPE
		LEFT JOIN STAGING.ICD_DIAGNOSIS AS icd2 ON REPLACE(medclaims.icd_code_2,'.','')=icd2.ICD_UNFORMATTED_CODE AND medclaims.ICD_TYPE=icd2.ICD_TYPE
		LEFT JOIN STAGING.ICD_DIAGNOSIS AS icd3 ON REPLACE(medclaims.icd_code_3,'.','')=icd3.ICD_UNFORMATTED_CODE AND medclaims.ICD_TYPE=icd3.ICD_TYPE
		LEFT JOIN STAGING.ICD_DIAGNOSIS AS icd4 ON REPLACE(medclaims.icd_code_4,'.','')=icd4.ICD_UNFORMATTED_CODE AND medclaims.ICD_TYPE=icd4.ICD_TYPE
		GROUP BY 1,2,3,4,5,6,7,8,9
]])
query([[COMMIT]])
query([[
CREATE OR REPLACE TABLE  OA_CNTIN.KPI_SUMMARY_DETAIL  AS 
SELECT DISTINCT
		 LOB.CLIENT_NAME,
		 LOB.CLIENT_ID,
		 LOB.REPORTING_DATE,
		 left(LOB.REPORTING_DATE,7) as reporting_yyyy_mm,
		 LOB.LOB,
         LOB.AXIAL_MEMBER_ID,
         COALESCE(CASE WHEN OUD.MEMBER_HOSPICE_FLAG = 1 OR OUD.MEMBER_CANCER_FLAG = 1 IS NOT NULL THEN 1 ELSE 0 END,0) AS EOL_FLAG,
         COALESCE(CASE WHEN OUD.MEMBER_CANCER_FLAG = 1 THEN 1 ELSE 0 END, 0) AS EOL_CANCER_FLAG,
         COALESCE(CASE WHEN OUD.MEMBER_HOSPICE_FLAG = 1 THEN 1 ELSE 0 END, 0) AS EOL_HOSPICE_FLAG,
         COALESCE(CASE WHEN OUD.MEMBER_LIFETIME_HOSPICE_FLAG = 1 OR OUD.MEMBER_LIFETIME_CANCER_FLAG = 1 IS NOT NULL THEN 1 ELSE 0 END,0) AS EOL_LIFETIME_FLAG,
         COALESCE(CASE WHEN OUD.MEMBER_LIFETIME_CANCER_FLAG = 1 THEN 1 ELSE 0 END, 0) AS EOL_LIFETIME_CANCER_FLAG,
         COALESCE(CASE WHEN OUD.MEMBER_LIFETIME_HOSPICE_FLAG = 1 THEN 1 ELSE 0 END,0) AS EOL_LIFETIME_HOSPICE_FLAG,
		 OUD.FIRST_OUD_DATE,
         COALESCE(OUD.POST_FIRST_OUD_FLAG,0) AS POST_FIRST_OUD_FLAG,
		 OUD.MONTHS_BETWEEN_REPORTING_DATE_AND_FIRST_OUD_DATE,
         COALESCE(OUD.POST_FIRST_MAT_FLAG, 0) AS POST_FIRST_MAT_FLAG,
		 COALESCE(OUD.MAT_CURRENT_MONTH_FLAG,0) MAT_CURRENT_MONTH_FLAG,
	     MAX(IFNULL(OPIOID_OVERDOSE_FLAG,0)) AS OPIOID_OVERDOSE_MEMBER,
	     MAX(IFNULL(ANY_DRUG_OVERDOSE_FLAG,0)) AS ANY_DRUG_OVERDOSE_MEMBER,
         COALESCE(RESULTS.MEMBER_OPIOID_PAIN_FLAG ,0) MEMBER_OPIOID_PAIN_FLAG,
		 COALESCE(RESULTS.MEMBER_OPIOID_DOSE_FLAG ,0) MEMBER_OPIOID_DOSE_FLAG,
		 COALESCE(RESULTS.MEMBER_OPIOID_MULTIPRESCRIBER_CRITICAL_FLAG ,0) MEMBER_OPIOID_MULTIPRESCRIBER_CRITICAL_FLAG,
		 COALESCE(RESULTS.MEMBER_OPIOID_POLYDRUG_CRITICAL_FLAG ,0) MEMBER_OPIOID_POLYDRUG_CRITICAL_FLAG,
		 COALESCE(RESULTS.MEMBER_OPIOID_POLYDRUG_FLAG ,0) MEMBER_OPIOID_POLYDRUG_FLAG,
		 COALESCE(RESULTS.MEMBER_OPIOID_MULTI_PHARMACY_FLAG ,0) MEMBER_OPIOID_MULTI_PHARMACY_FLAG,
		 CASE WHEN dt.axial_member_id IS NULL THEN 0 ELSE 1 END AS MEMBER_DRUG_TEST_FLAG,
		 COALESCE(COUNT(DISTINCT VISIT_ID),0) AS TOTAL_VISITS,
	     COALESCE(COUNT(DISTINCT CASE WHEN AXIAL_PLACE_OF_SERVICE = '22' THEN VISIT_ID ELSE NULL END),0) AS OUTPATIENT_VISITS,
		 COALESCE(COUNT(DISTINCT CASE WHEN AXIAL_PLACE_OF_SERVICE = '11' THEN VISIT_ID ELSE NULL END),0) AS OFFICE_VISITS,
		 COALESCE(COUNT(DISTINCT CASE WHEN AXIAL_PLACE_OF_SERVICE = '23' THEN VISIT_ID ELSE NULL END),0) AS ED_VISITS,
		 COALESCE(COUNT(DISTINCT CASE WHEN AXIAL_PLACE_OF_SERVICE = '21' THEN VISIT_ID ELSE NULL END),0) AS IP_VISITS,
		 COALESCE(COUNT(DISTINCT CASE WHEN AXIAL_PLACE_OF_SERVICE = '24' THEN VISIT_ID ELSE NULL END),0) AS ASC_VISITS,
		 COALESCE(COUNT(DISTINCT CASE WHEN AXIAL_PLACE_OF_SERVICE = '81' THEN VISIT_ID ELSE NULL END),0) AS LAB_VISITS,
		 COALESCE(COUNT(DISTINCT CASE WHEN AXIAL_PLACE_OF_SERVICE = '20' THEN VISIT_ID ELSE NULL END),0) AS UC_VISITS,
		 COALESCE(COUNT(DISTINCT CASE WHEN AXIAL_PLACE_OF_SERVICE = '98' THEN VISIT_ID ELSE NULL END),0) AS OBS_VISITS,
		 COALESCE(COUNT(DISTINCT CASE WHEN IRF_VISIT=1 THEN VISIT_ID ELSE NULL END),0) AS IRF_VISITS,
		 COALESCE(COUNT(DISTINCT CASE WHEN AXIAL_PLACE_OF_SERVICE = '21' AND ICU_VISIT=1 THEN VISIT_ID ELSE NULL END),0) AS ICU_VISITS,
		 COALESCE(COUNT(DISTINCT CASE WHEN OPIOID_OVERDOSE_FLAG=1 THEN VISIT_ID ELSE NULL END),0) AS OPIOID_OVERDOSE_VISITS,
		 COALESCE(COUNT(DISTINCT CASE WHEN ANY_DRUG_OVERDOSE_FLAG=1 THEN VISIT_ID ELSE NULL END),0) AS ANY_DRUG_OVERDOSE_VISITS,
		 COALESCE(SUM(AMOUNT_INSURANCE_PAID),0) AS total_MEDICAL_SPEND,
		 COALESCE(SUM(AMOUNT_TOTAL_PAID),0) AS AMOUNT_TOTAL_PAID_MEDICAL_SPEND,
		 COALESCE(SUM(AMOUNT_ALLOWED),0) AS AMOUNT_ALLOWED_MEDICAL_SPEND,
		 COALESCE(SUM(DISTINCT AMOUNT_RX_PAID) ,0) AS TOTAL_RX_SPEND,
		 COALESCE(SUM(DISTINCT RX_AMOUNT_TOTAL_PAID),0) AS RX_AMOUNT_TOTAL_PAID,
		 COALESCE(SUM(DISTINCT RX_AMOUNT_ALLOWED),0) AS RX_AMOUNT_ALLOWED,
		 COALESCE(SUM(CASE WHEN AXIAL_PLACE_OF_SERVICE = '22' THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS OUTPATIENT_SPEND,
		 COALESCE(SUM(CASE WHEN AXIAL_PLACE_OF_SERVICE = '11' THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS OFFICE_SPEND,
		 COALESCE(SUM(CASE WHEN AXIAL_PLACE_OF_SERVICE = '23' THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS ED_SPEND,
		 COALESCE(SUM(CASE WHEN AXIAL_PLACE_OF_SERVICE = '21' THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS IP_SPEND,
		 COALESCE(SUM(CASE WHEN AXIAL_PLACE_OF_SERVICE = '24' THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS ASC_SPEND,
		 COALESCE(SUM(CASE WHEN AXIAL_PLACE_OF_SERVICE = '81' THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS LAB_SPEND,
		 COALESCE(SUM(CASE WHEN AXIAL_PLACE_OF_SERVICE = '20' THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS UC_SPEND,
		 COALESCE(SUM(CASE WHEN AXIAL_PLACE_OF_SERVICE = '98' THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS OBS_SPEND,
		 COALESCE(SUM(CASE WHEN IRF_VISIT=1 THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS IRF_SPEND,
		 COALESCE(SUM(CASE WHEN AXIAL_PLACE_OF_SERVICE = '21' AND ICU_VISIT=1 THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS ICU_SPEND,
		 COALESCE(SUM(CASE WHEN OPIOID_OVERDOSE_FLAG=1 THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS OPIOID_OVERDOSE_SPEND,
		 COALESCE(SUM(CASE WHEN ANY_DRUG_OVERDOSE_FLAG=1 THEN AMOUNT_INSURANCE_PAID ELSE 0 END),0) AS ANY_DRUG_OVERDOSE_SPEND,
		 COALESCE(SUM(DISTINCT OPIOID_PRESCRIPTIONS),0) AS OPIOID_PRESCRIPTIONS,
		 COALESCE(SUM(DISTINCT TOTAL_PRESCRIPTIONS),0) AS TOTAL_PRESCRIPTIONS,
		 COALESCE(oud_dx_current_month_flag,0) oud_dx_current_month_flag,--
		 COALESCE(sud_dx_current_month_flag,0) sud_dx_current_month_flag,
		 first_sud_date,
		 COALESCE(post_first_sud_flag,0) post_first_sud_flag,
		 COALESCE(oud.member_medd,0) member_medd,
		 COALESCE(CURRENT_month_naloxone_rx_flag,0) CURRENT_month_naloxone_rx_flag,
		 COALESCE(CURRENT_month_naloxone_rx_count,0) CURRENT_month_naloxone_rx_count,
		 COALESCE(naloxone_12_month_rx_count,0) naloxone_12_month_rx_count,
		 CASE WHEN o12.AXIAL_MEMBER_ID IS NULL THEN 0 ELSE 1 END AS oud_twelve_month_flag,
		 CASE WHEN s12.AXIAL_MEMBER_ID  IS NULL THEN 0 ELSE 1 END AS sud_twelve_month_flag,
		 CASE WHEN analg.AXIAL_MEMBER_ID IS NULL THEN 0 ELSE 1 END AS CURRENT_MONTH_ANALGESIC_FLAG,
		 COALESCE (RMF.OPIOID_BENZODIAZEPINE_DAYS,0) AS OPIOID_BENZO_DAYS,
		 COALESCE (T1.OPIOID_PROVIDER_COUNT,0) AS OPIOID_PROVIDER_COUNT
	FROM   
		(SELECT  * FROM OA_CNTIN.ASL_TRACKER at2 WHERE ELIGIBILITY_FLAG  = 1) AS LOB
	LEFT JOIN (SELECT  DISTINCT
			   AXIAL_MEMBER_ID,
			   REPORTING_YYYY_MM,
			   FIRST_OUD_DATE,
			   POST_FIRST_OUD_FLAG,
			   MONTHS_BETWEEN_REPORTING_DATE_AND_FIRST_OUD_DATE,
			   POST_FIRST_MAT_FLAG,
			   MAT_CURRENT_MONTH_FLAG,
			   MEMBER_HOSPICE_FLAG,
			   MEMBER_CANCER_FLAG,
			   MEMBER_LIFETIME_HOSPICE_FLAG,
			   MEMBER_LIFETIME_CANCER_FLAG,
			   oud_dx_current_month_flag,
			   sud_dx_current_month_flag,
			   first_sud_date,
			   post_first_sud_flag,
			   member_medd,
			   CURRENT_month_naloxone_rx_flag,
			   CURRENT_month_naloxone_rx_count,
			   naloxone_12_month_rx_count
		   FROM OA_CNTIN.ENHANCED_SUD
		   ) AS OUD ON LOB.AXIAL_MEMBER_ID=OUD.AXIAL_MEMBER_ID AND LOB.REPORTING_YYYY_MM=OUD.REPORTING_YYYY_MM
	LEFT JOIN OA_CNTIN.kpi_medclaim_prep AS MEDCLAIMS ON LOB.AXIAL_MEMBER_ID=MEDCLAIMS.AXIAL_MEMBER_ID AND LOB.LOB=MEDCLAIMS.LOB AND LOB.REPORTING_DATE=MEDCLAIMS.SERVICE_FROM_REPORTING_DATE		   
	LEFT JOIN OA_CNTIN.MST_MEMBER_RESULTS AS RESULTS ON LOB.REPORTING_DATE=RESULTS.REPORTING_DATE AND LOB.AXIAL_MEMBER_ID=RESULTS.AXIAL_MEMBER_ID
	LEFT JOIN OA_CNTIN.SUD_TWELVE_MONTH s12 ON lob.axial_member_id= s12.AXIAL_MEMBER_ID  AND lob.reporting_DATE=s12.REPORTING_DATE 
	LEFT JOIN OA_CNTIN.OUD_TWELVE_MONTH o12 ON lob.axial_member_id= o12.AXIAL_MEMBER_ID  AND lob.reporting_DATE=o12.REPORTING_DATE 
	LEFT JOIN OA_CNTIN.ANALGESIC_CLAIMS analg ON lob.axial_member_id= analg.AXIAL_MEMBER_ID  AND lob.reporting_yyyy_mm=analg.REPORTING_YYYY_MM 
	LEFT JOIN OA_CNTIN.drug_tested_members dt ON lob.axial_member_id= dt.AXIAL_MEMBER_ID  AND lob.reporting_yyyy_mm = dt.assessed_through_yyyy_mm 
	LEFT JOIN OA_CNTIN.ROLLING_OPIOID_PROVIDER_COUNT t1 ON t1.reporting_date = lob.reporting_date AND t1.axial_member_id = lob.axial_member_id
	LEFT JOIN STAGING.RISK_MITIGATION_FEATURES rmf ON LOB.AXIAL_MEMBER_ID=RMF.AXIAL_MEMBER_ID AND LOB.REPORTING_YYYY_MM = RMF.REPORTING_YYYY_MM
	LEFT JOIN (SELECT 
			AXIAL_MEMBER_ID,
			SERVICE_FROM_MONTH,
			LOB,
			COUNT(DISTINCT CASE WHEN IS_OPIOID_FLAG=1 THEN AXIAL_PRESCRIPTION_FILL_ID ELSE NULL END) AS OPIOID_PRESCRIPTIONS,
			COUNT(DISTINCT AXIAL_PRESCRIPTION_FILL_ID) AS TOTAL_PRESCRIPTIONS,
			SUM(IFNULL(AMOUNT_INSURANCE_PAID,0)) AS AMOUNT_RX_PAID,
			sum(IFNULL(amount_total_paid,0))AS RX_AMOUNT_TOTAL_PAID,
			sum(IFNULL(amount_allowed,0))AS RX_AMOUNT_ALLOWED
		   FROM OA_CNTIN.MST_RX_CLAIMS
		   GROUP BY 1,2,3) AS RXCLAIMS ON LOB.AXIAL_MEMBER_ID=RXCLAIMS.AXIAL_MEMBER_ID
			   			AND LOB.LOB=RXCLAIMS.LOB
			   			AND LOB.REPORTING_DATE=RXCLAIMS.SERVICE_FROM_MONTH	
GROUP BY 
		 LOB.CLIENT_NAME,
		 LOB.CLIENT_ID,
		 LOB.REPORTING_DATE,
		 LOB.REPORTING_DATE,
		 LOB.LOB,
	     LOB.AXIAL_MEMBER_ID,
	     OUD.MEMBER_CANCER_FLAG,
	     OUD.MEMBER_HOSPICE_FLAG,
	     OUD.MEMBER_LIFETIME_CANCER_FLAG,
	     OUD.MEMBER_LIFETIME_HOSPICE_FLAG,
		 OUD.FIRST_OUD_DATE,
	     OUD.POST_FIRST_OUD_FLAG,
		 OUD.MONTHS_BETWEEN_REPORTING_DATE_AND_FIRST_OUD_DATE,
	     OUD.POST_FIRST_MAT_FLAG,
		 OUD.MAT_CURRENT_MONTH_FLAG,
	     RESULTS.MEMBER_OPIOID_PAIN_FLAG,
		 RESULTS.MEMBER_OPIOID_DOSE_FLAG,
		 RESULTS.MEMBER_OPIOID_MULTIPRESCRIBER_CRITICAL_FLAG,
		 RESULTS.MEMBER_OPIOID_POLYDRUG_CRITICAL_FLAG,
		 RESULTS.MEMBER_OPIOID_POLYDRUG_FLAG,
		 RESULTS.MEMBER_OPIOID_MULTI_PHARMACY_FLAG,
		 oud_dx_current_month_flag,--
		 sud_dx_current_month_flag,
		 first_sud_date,
		 post_first_sud_flag,
		 oud.member_medd,
		 CURRENT_month_naloxone_rx_flag,
		 CURRENT_month_naloxone_rx_count,
		 naloxone_12_month_rx_count,
		 s12.AXIAL_MEMBER_ID ,
		 o12.AXIAL_MEMBER_ID ,
		 ANALG.AXIAL_MEMBER_ID ,
		 dt.axial_member_id,
		 RMF.OPIOID_BENZODIAZEPINE_DAYS,
		 T1.OPIOID_PROVIDER_COUNT
]])		
query([[DROP TABLE OA_CNTIN.ANALGESIC_CLAIMS]])
query([[DROP TABLE OA_CNTIN.OUD_TWELVE_MONTH]])
query([[DROP TABLE OA_CNTIN.SUD_TWELVE_MONTH]])
query([[DROP TABLE OA_CNTIN.DRUG_TESTED_MEMBERS]])
query([[DROP TABLE OA_CNTIN.ROLLING_OPIOID_PROVIDER_COUNT]])
query([[DROP TABLE OA_CNTIN.KPI_MEDCLAIM_PREP]])
query([[CREATE OR REPLACE  TABLE  OA_CNTIN.kpi_detail_spend_check as
 SELECT 
    client_id, 
    Client_name , 
    lob, 
    REPORTING_DATE, 
    sum(TOTAL_MEDICAL_SPEND) AS kpi_medical_spend, 
    sum(claims_ins_paid) AS  claims_ins_paid ,
    sum(CASE WHEN sUD_TWELVE_MONTH_FLAG = 1 THEN  TOTAL_MEDICAL_SPEND else 0 end ) AS sud_kpi_spend,
    sum(CASE WHEN sUD_TWELVE_MONTH_FLAG = 1 THEN  CLAIMS_INS_PAID ELSE 0 end ) AS sud_claims_spend,
    sum(CASE WHEN OUD_TWELVE_MONTH_FLAG = 1 THEN  TOTAL_MEDICAL_SPEND else 0 end ) AS oud_kpi_spend,
    sum(CASE WHEN OUD_TWELVE_MONTH_FLAG = 1 THEN  CLAIMS_INS_PAID ELSE 0 end ) AS oud_claims_spend
 FROM 
   (SELECT 
        kpi.CLIENT_ID ,
        kpi.CLIENT_NAME ,
        kpi.REPORTING_DATE ,
        kpi.LOB ,
        kpi.AXIAL_MEMBER_ID ,
        kpi.OUD_TWELVE_MONTH_FLAG,
        kpi.SUD_TWELVE_MONTH_FLAG, 
        KPI.TOTAL_MEDICAL_SPEND,
        SUM(IFNULL(CLM.AMOUNT_INSURANCE_PAID, 0)) AS CLAIMS_INS_PAID
    FROM
    OA_CNTIN.kpi_summary_detail KPI
    LEFT JOIN  OA_CNTIN.MST_MEDICAL_CLAIMS CLM
    ON KPI.CLIENT_ID = CLM.CLIENT_ID
    AND KPI.AXIAL_MEMBER_ID = CLM.AXIAL_MEMBER_ID
    AND KPI.REPORTING_YYYY_MM = CLM.SERVICE_FROM_YYYY_MM
    AND KPI.LOB = CLM.LOB
    GROUP BY 1,2,3,4,5,6,7,8
        )  GROUP BY 1,2,3,4]])
query([[INSERT into zz_crb.kpi_time_test  
SELECT now() AS time_info, 'end' AS p_acction ]]);

CREATE LUA SCRIPT OA_CNTIN."MEMBER_OUD" () RETURNS ROWCOUNT AS
query([[
CREATE OR replace TABLE OA_CNTIN.member_oud AS 
select
DISTINCT
client_id,
client_name,
axial_member_id,
FIRST_OUD_DX_DATE,
FIRST_OUD_DATE,
FIRST_MAT_DATE,
FIRST_OUD_DX_SERVICING_PROVIDER_NPI,
FIRST_OUD_DX_AXIAL_POS_CODE,
FIRST_OUD_DX_AXIAL_POS_NAME,
FIRST_MAT_PROVIDER_NPI,
FIRST_OUD_DX_ICD_CODE_1,
FIRST_OUD_DX_ICD_CODE_2,
FIRST_OUD_DX_ICD_CODE_3,
FIRST_OUD_DX_ICD_CODE_4
FROM
OA_CNTIN.ENHANCED_OUD
where OUD_FLAG = 1]]);

--EXECUTE SCRIPT OA_CNTIN.ANALYTICS_UPDATE();

CREATE OR REPLACE LUA SCRIPT OA_CNTIN."ANALYTICS_UPDATE" () RETURNS TABLE AS
query([[EXECUTE SCRIPT OA_CNTIN.asl_tracker()]])
query([[COMMIT]])
query([[EXECUTE SCRIPT OA_CNTIN.ENHANCED_OUD()]])
query([[COMMIT]])
query([[EXECUTE SCRIPT OA_CNTIN.ENHANCED_SUD()]])
query([[COMMIT]])
query([[EXECUTE SCRIPT OA_CNTIN.MEMBER_OUD()]])
query([[COMMIT]])
query([[EXECUTE SCRIPT OA_CNTIN.KPI_SUMMARY_DETAIL()]]) 
query([[COMMIT]])

