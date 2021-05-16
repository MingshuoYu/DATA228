import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as func
from pyspark.context import SparkContext
from pyspark.sql import DataFrame

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "228", table_name = "hv", transformation_ctx = "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, 
mappings = [("hvfhs_license_num", "string", "hvfhs_license_num", "string"), 
("pickup_datetime", "string", "pickup_datetime", "timestamp"), 
("dropoff_datetime", "string", "dropoff_datetime", "timestamp"), 
("pulocationid", "long", "pulocationid", "long"), 
("dolocationid", "long", "dolocationid", "long"), 
("sr_flag", "long", "sr_flag", "long")], transformation_ctx = "applymapping1")
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")

sparkdf = resolvechoice2.toDF()
transform1 = sparkdf.fillna(0, subset = ['SR_Flag'])
transform2 = transform1.dropna()
result = DynamicFrame.fromDF(dataframe = transform2, glue_ctx = glueContext, name = 'result')

datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = result, catalog_connection = "redshift-east", 
connection_options = {"dbtable": "hv", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")

job.commit()