#mode = "overwrite"
#url = "jdbc:postgresql://db-cluster-auna-dev-instance-1.ccpjcspl9dtx.us-east-1.rds.amazonaws.com:5432/db_consolidado_bi_no_prd_dev"
#properties = {"user": "amorales","password": "v9JgPD3#Q6LSNLY4","driver": "org.postgresql.Driver"}
#my_writer.jdbc(url=url, table="mkf_tablero_permanencia", mode=mode, properties=properties)

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ANALYTICS_BUCKET_NAME", "ANALYTICS_BUCKET_PATH", "RDS_SCHEMA_NAME", "RDS_TABLE_NAME", "JDBC_CONNECTION_URL", "USERNAME", "PASSWORD"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

analyticsBucketName = args["ANALYTICS_BUCKET_NAME"]
analyticsBucketPath = args["ANALYTICS_BUCKET_PATH"]
rdsSchemaName = args['RDS_SCHEMA_NAME']
rdsTableName = args['RDS_TABLE_NAME']
jdbcConnectionUrl = args["JDBC_CONNECTION_URL"]
username = args["USERNAME"]
password = args["PASSWORD"]

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            #"s3://auna-dlaqa-analytics-s3/structured-data/OLAP/pry-gestion-cobranza/mkf_tablero_permanencia/"
            f"s3://{analyticsBucketName}/{analyticsBucketPath}"
        ]
    },
    transformation_ctx="S3bucket_node1",
).toDF()

(S3bucket_node1.write.format('jdbc').option('url', jdbcConnectionUrl) 
            .option('user', username)
            .option('password', password)
            .option('dbtable', f'"{rdsSchemaName}"."{rdsTableName}"')
            .option('driver', 'org.postgresql.Driver')
            .mode('overwrite').save()) 
job.commit()

