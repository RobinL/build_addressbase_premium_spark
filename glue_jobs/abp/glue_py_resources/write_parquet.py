import json
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType


def convert_txt_to_parquet(spark):

    with open("table_names.json", "r") as f:
        table_names = json.load(f)

    # Want to partition based on filename.

    for k,v in table_names.items():
        # Some tables e.g. id 30 have very few records (possibly none?)
        print v["filename"]

        # Load in Spark schema so we read with the correct datatypes
        with open("{}.json".format(v["tablename"]), "r") as f:
            jsonschema = json.load(f)
        schema = StructType.fromJson(jsonschema)
        try:
            df =  spark.read.csv("s3a://alpha-everyone/deleteathenaout/outtemp/first_col={}".format(v["partition"]), schema=schema)
        except AnalysisException:
            continue


        headings = v["headings"]
        headings = headings + ["filename"]
        df = df.toDF(*headings)

        df = df.repartition(100, "filename")  # 100 partitions based on file name.  Note that this is actually a maximum of 100 partitions.  If there are <100 file names, there'll be a file per file name.

        df.write.parquet("s3a://alpha-everyone/deleteathenaout/abpparquet/{}".format(v["tablename"]), mode="overwrite")



try:
    import urllib.request as urlrequest
except ImportError:
    import urllib as urlrequest

import os

def get_classification_schema_and_write_to_parquet(spark):
    csv = "https://raw.githubusercontent.com/warwickshire/FME-Workspaces/master/AddressBase/Files%20for%20load/AddressBase_products_classification_scheme_1.3.csv"
    urlrequest.urlretrieve(csv, "lookup.csv")
    with open("abp_classification_scheme.json", "r") as f:
            jsonschema = json.load(f)

    schema = StructType.fromJson(jsonschema)

    look = spark.read.csv("file://{}/lookup.csv".format(os.getcwd()), encoding="latin1", header=True, schema=schema)
    look.write.parquet("s3a://alpha-everyone/deleteathenaout/abpparquet/abp_classification_scheme")
