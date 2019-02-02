# Example job tests access to all files passed to the job runner class
import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from gluejobutils.s3 import read_json_from_s3

from pyspark.sql import functions as fns
from parallel_download import download_abp_zips
from read_zips_from_s3 import get_zip_data_from_s3
from write_parquet import convert_txt_to_parquet
from all_addresses import create_all_addresses

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'test_arg'])

print "JOB SPECS..."
print "JOB_NAME: ", args["JOB_NAME"]
print "test argument: ", args["test_arg"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# # STEP 1:  Download data to s3
# print "Starting to download data to s3"
# links = read_json_from_s3("s3://alpha-everyone/addressbase_premium/json/links.json")
# download_abp_zips(links, sc)
# print "Finished downloading data to s3"


# # STEP 2:  User spark to unzip the files and load into Spark dataframe
# print "Starting to read data from s3 zips"
# raw_data = get_zip_data_from_s3(sc)

# # STEP 3:  Split files into the various ABP tables and write to S3
# split_col = fns.split(raw_data['line'], ',')
# raw_data = raw_data.withColumn('first_col', split_col.getItem(0))

# # Deal with possible errors - if first_col is not two characters, both digits, then remove
# conformant_data = raw_data.filter(raw_data["line"].rlike("^\d{2},"))

# conformant_data.write.partitionBy("first_col").mode('overwrite').text("s3a://alpha-everyone/deleteathenaout/outtemp/")
# print "Complete writing out split csvs"

# Step 4: Convert data into parquet format
# convert_txt_to_parquet(spark)
# get_classification_schema_and_write_to_parquet(spark)

# Step 5:  Create the all addresses table
create_all_addresses(spark)