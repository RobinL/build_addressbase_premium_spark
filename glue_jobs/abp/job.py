# Example job tests access to all files passed to the job runner class
import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from gluejobutils.s3 import read_json_from_s3

from parallel_download import download_abp_zips

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'test_arg'])

print "JOB SPECS..."
print "JOB_NAME: ", args["JOB_NAME"]
print "test argument: ", args["test_arg"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

links = read_json_from_s3("s3://alpha-everyone/addressbase_premium/json/links.json")
download_abp_zips(links, sc)
