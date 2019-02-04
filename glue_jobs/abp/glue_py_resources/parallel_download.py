# For py 2 and 3 compataibility
try:
    import urllib.request as urlrequest
except ImportError:
    import urllib as urlrequest

import re

from settings import ZIP_BUCKET, ZIP_FOLDER

def download_and_put_in_s3(link):

    #Must be within function otherwise tries to pickle and send boto3 client to workers
    import boto3
    s3_client = boto3.client('s3')
    match = re.search("CSV\/(\w{2}\d{4}\.zip)", link).group(1)
    urlrequest.urlretrieve(link, "{}".format(match))
    s3_client.upload_file(match, ZIP_BUCKET, "{}/{}".format(ZIP_FOLDER, match))
    return match

def download_abp_zips(download_links, spark_context):
    rdd = spark_context.parallelize(download_links, numSlices=100)  # As I understand it, if we set num slices >= num cpus in the cluster, we will parallelise across all cpus
    rdd.map(download_and_put_in_s3).collect()