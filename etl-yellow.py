import sys
import pyspark.sql.functions as func
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "228", table_name = "yellow", transformation_ctx = "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, 
mappings = [("tpep_pickup_datetime", "string", "tpep_pickup_datetime", "timestamp"), 
("tpep_dropoff_datetime", "string", "tpep_dropoff_datetime", "timestamp"), 
("passenger_count", "long", "passenger_count", "long"), 
("trip_distance", "double", "trip_distance", "double"), 
("pulocationid", "long", "pulocationid", "long"), 
("dolocationid", "long", "dolocationid", "long"), 
("fare_amount", "double", "fare_amount", "double"), 
("tip_amount", "double", "tip_amount", "double"), 
("total_amount", "double", "total_amount", "double")], transformation_ctx = "applymapping1")

resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
sparkdf = resolvechoice2.toDF()
transform1 = sparkdf.where(func.col('tpep_pickup_datetime').between('2019-01-01', '2020-12-31'))
transform2 = transform1.dropna(subset = ['passenger_count', 'trip_distance'])
result = DynamicFrame.fromDF(dataframe = transform2, glue_ctx = glueContext, name = 'result')

datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = result, catalog_connection = "redshift-east", 
connection_options = {"dbtable": "yellow", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")

job.commit()