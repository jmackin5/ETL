insert into reference_tables_etl.master_file_client_extract_id values
		(1
		,'Daily_hmkde_md_extract_action_plan_'  
		,'sftp'  
		,'DataOps-ODS-redshift' 
		,'DataOps-SFTP-Highmark-Test' 
		,'axialhealthcare-knox-etl-raw' 
		,'client_files/hmkde_mcaid/Extracts/'  
		,'inbound/membership/penv/datafile/'  
		,'select * from client_extracts_etl.daily_hmkde_md_extract_action_plan;' 
		,0  )
