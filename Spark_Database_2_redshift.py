import sys

from awsglue import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime
from pyspark.sql.functions import lit
import pytz

timezone = pytz.timezone("America/Chicago")



## initiate Glue spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)





######update arguements
#AWS Location
target_db = 'client_real_time_data'
update_s3 = True
update_redshift = True
UPDATE_CATALOG = True
UPDATE_S3 = True
UPDATE_REDSHIFT = True
s3_bucket = 'axialhealthcare-knox-etl-raw-dev'
redshift_schema = 'client_batch_daily'
JOB_RUN_ID ='aslkdjalskj10293'
ods_schema = 'client_production'






# function for sparkSql adding column to dynamicFrame



def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


def saveToS3(source_table_name
             , target_table_name
             , job_run_id
             , update_catalog=True
             , update_s3=True
             , update_redshift=True
             , target_database_name='axial_research'
             , target_schema_name=redshift_schema):
    start = datetime.datetime.now()
        
    #setting up connection
    DataSource0 = glueContext \
        .create_dynamic_frame \
        .from_options(connection_type="postgresql"
                    #   , connection_options={"dbTable": source_table_name,
                    #                         "connectionName": "axial_ods"}
                                            
                      , connection_options = {"url": "jdbc:postgresql://10.10.5.31:5432/axial_ods"
                                            , "dbtable": f"{ods_schema}.{source_table_name}" 
                                            , "user": 'jmackinnon'
                                            , "password": 

}                        
                      , transformation_ctx="DataSource0")
                      

    print('we made it here')
    #Dropping null fields
    Transform0 = DropNullFields.apply(frame=DataSource0, transformation_ctx="DropNullFields")


    if update_s3:
        
        print('updating s3')
        # Transform1 = MyTransform(glueContext, job_run_id, Transforms0)
        s3_path = f"s3://{s3_bucket}/{target_db}/{source_table_name}/"

        SqlQuery3 = f"select * , '{job_run_id}' as run_id from {source_table_name}"

        Transform1 = sparkSqlQuery(glueContext
                                  , query=SqlQuery3
                                  , mapping={source_table_name: DataSource0}
                                  , transformation_ctx="Transform0")
        if update_catalog:
            DataSink0 = glueContext.getSink(format_options={"compression": "snappy"}
                                            , path=s3_path
                                            , connection_type="s3"
                                            , updateBehavior="UPDATE_IN_DATABASE"
                                            , partitionKeys=['run_id']
                                            , enableUpdateCatalog=True
                                            , transformation_ctx="DataSink0")
            DataSink0.setCatalogInfo(catalogDatabase=target_db, catalogTableName=source_table_name)
            DataSink0.setFormat("parquet", useGlueParquetWriter=True)
        else:
            DataSink0 = glueContext.getSink(format_options={"compression": "snappy"}
                                            , path=s3_path
                                            , connection_type="s3"
                                            , partitionKeys=['run_id']
                                            , transformation_ctx="DataSink0")
            DataSink0.setFormat("parquet", useGlueParquetWriter=True)

        DataSink0.writeFrame(Transform1)
        print(f"finished copying table from snowflake {source_table_name} into s3 with run_id {job_run_id}")

    if update_redshift:

        print('Updating Redshift')
        truncate_sql_smt = f"truncate table {target_schema_name}.{target_table_name};"
        rs_connection_options = {
            "preactions": truncate_sql_smt,
            "dbtable": f"{target_schema_name}.{target_table_name}",
            "database": target_database_name}
        DataSink1 = glueContext. \
            write_dynamic_frame_from_jdbc_conf(frame=Transform0
                                              , catalog_connection="Redshift"
                                              , connection_options=rs_connection_options
                                              , redshift_tmp_dir='s3://aws-glue-temporary-877582243023-us-east-1/glue_spark_logging/',
                                              transformation_ctx="DataSink0")
        print(f"finished copying table from snowflake {source_table_name} into "
              f"redshift {target_database_name}.{target_schema_name}.{target_table_name}")
    end = datetime.datetime.now()
    print(f"Total time to extract the {source_table_name}: {end - start}")
    pass




flat_list = ["census","prior_authorization","adt"]
table_data = [[table] for table in flat_list]
print(table_data)
# list  of college data with two lists

table_name = 'table_list'
s3_path = f"s3://{s3_bucket}/{target_db}/{table_name}/"
print(s3_path)
col_name = "table_name"
# creating a dataframe


table_list_df = spark.createDataFrame(table_data, [col_name]) \
    .withColumn('loaded_time', lit(datetime.datetime.now())) \
    .withColumn('run_id', lit(JOB_RUN_ID)).repartition(1)

transform0 = DynamicFrame.fromDF(table_list_df, glueContext, 'table_list')

if UPDATE_CATALOG:
    dataSink0 = glueContext \
        .getSink(format_options={"compression": "snappy"}
                 , path=s3_path
                 , connection_type="s3"
                 , updateBehavior="UPDATE_IN_DATABASE"
                 , partitionKeys=['run_id']
                 , enableUpdateCatalog=True
                 , transformation_ctx="DataSink0")
    dataSink0.setCatalogInfo(catalogDatabase=target_db, catalogTableName=table_name)
    dataSink0.setFormat("parquet", useGlueParquetWriter=True)
else:
    dataSink0 = glueContext \
        .getSink(format_options={"compression": "snappy"}
                 , path=s3_path
                 , connection_type="s3"
                 , partitionKeys=['run_id']
                 , transformation_ctx="DataSink0")
    dataSink0.setFormat("parquet", useGlueParquetWriter=True)

dataSink0.writeFrame(transform0)

## update the table list
total = len(flat_list)
for table_name in flat_list:
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now_str} updating {table_name} and {total} table(s) to go")
    if table_name == 'prior_authorization':
        

        saveToS3(source_table_name=table_name
                 , target_table_name='authorization'
                 , job_run_id=JOB_RUN_ID
                 , update_catalog=UPDATE_CATALOG
                 , update_s3=UPDATE_S3
                 , update_redshift=UPDATE_REDSHIFT
                 , target_database_name='axial_research'
                 , target_schema_name=redshift_schema)
        total = total - 1
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{now_str} finished {table_name} and {total} table(s) to go")
    
    else: 

        saveToS3(source_table_name=table_name
                 , target_table_name=table_name
                 , job_run_id=JOB_RUN_ID
                 , update_catalog=UPDATE_CATALOG
                 , update_s3=UPDATE_S3
                 , update_redshift=UPDATE_REDSHIFT
                 , target_database_name='axial_research'
                 , target_schema_name=redshift_schema)
        total = total - 1
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{now_str} finished {table_name} and {total} table(s) to go")

print("Finished snowflake migration")

job.commit()
