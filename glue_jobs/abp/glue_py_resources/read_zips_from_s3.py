import boto3
import io
import zipfile

def s3_path_to_bucket_key(path):
    path = path.replace("s3://", "")
    bucket, key = path.split('/', 1)
    return bucket, key

def s3_path_to_bytes_io(path):
    """
    Example usage:
    bytes_io = s3_path_to_bytes_io("s3://bucket/file.csv")
    for line in bytes_io.readlines():
        print(line.decode("utf-8"))
    """
    s3_client = boto3.client('s3')
    bucket, key = s3_path_to_bucket_key(path)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return io.BytesIO(obj['Body'].read())

def paginate(method, **kwargs):
      client = method.__self__
      paginator = client.get_paginator(method.__name__)
      for page in paginator.paginate(**kwargs).result_key_iters():
          for result in page:
              yield result

def get_file_list_from_bucket(bucket, bucket_folder):
    s3_client = boto3.client('s3')
    contents = []
    for key in paginate(s3_client.list_objects_v2, Bucket=bucket, Prefix=bucket_folder):
        contents.append(key)
    files_list = [c["Key"] for c in contents]
    return files_list

def zip_extract(x):
    in_memory_data = s3_path_to_bytes_io("s3://{}/{}".format("alpha-everyone", x))
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    contents = file_obj.filelist[0]  #assume there is just one file.  Check it's a csv else error
    if '.csv' not in contents.filename:
        raise ValueError("OS zip file does not contain csv")
    file_bytes = file_obj.open(contents).read()
    return {"lines": file_bytes, "filename": x.split("/")[-1]}


def join_lines_file(x):
    lines = x["lines"].split("\r\n")
    # Probably easiest to
    return ['{},"{}"'.format(l, x["filename"]) for l in lines]

def get_zip_data_from_s3(spark_context):

    file_list = get_file_list_from_bucket("alpha-everyone", "deleteathenaout/abpzips_glue/")

    # As I understand it, if we set num slices >= num cpus in the cluster, we will parallelise across all cpus.
    files_rdd = spark_context.parallelize(file_list, numSlices=20)

    files_data = files_rdd.map(zip_extract)

    files_data = files_data.flatMap(join_lines_file)
    files_data = files_data.map(lambda line: (line,))

    files_data = files_data.toDF(("line",))
    return files_data
