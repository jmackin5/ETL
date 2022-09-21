CREATE TABLE reference_tables_etl.master_file_client_extract_id (
	client_extract_id int NOT NULL,
	file_name varchar null,
	extract_to varchar NULL,
	db_secret varchar NULL,
	sftp_secret varchar NULL,
	bucket varchar null, 
	s3_file_path varchar NULL,
	sftp_file_path varchar NULL,
	query varchar null,
	util_job int8 null
	CONSTRAINT client_extract_id PRIMARY KEY
);
