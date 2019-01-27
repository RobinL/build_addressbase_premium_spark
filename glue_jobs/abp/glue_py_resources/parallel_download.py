# For py 2 and 3 compataibility
try:
    import urllib.request as urlrequest
except ImportError:
    import urllib as urlrequest

import re

import boto3

def download_and_put_in_s3(link):

    #Must be within function otherwise tries to pickle and send boto3 client to workers
    import boto3
    s3_client = boto3.client('s3')

    # To do - make this work in Python 2 so that it runs on glue

    match = re.search("CSV\/(\w{2}\d{4}\.zip)", link).group(1)

    urlrequest.urlretrieve(link, "{}".format(match))

    s3_client.upload_file(match, "alpha-everyone", "deleteathenaout/abpzips_glue/{}".format(match))

    # Put in s3
    return match

def download_abp_zips(download_links, spark_context):
    rdd = spark_context.parallelize(download_links, numSlices=100)  # As I understand it, if we set num slices >= num cpus in the cluster, we will parallelise across all cpus
    downloaded_files = rdd.map(download_and_put_in_s3).collect()