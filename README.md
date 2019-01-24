# build_addressbase_premium_spark
Build addressbase premium using spark


## Current run order:

1.  Parallel download.

The input to this script is the OS download webpage that contains the links to all the component zip files.  It distributes the task of downloading these files to S3 across the cluster.

2. Split

Takes the input zip files and splits the lines within each zip file into its component table.  Outputs the results to csv files in s3.  

3. Apply schema and partition

Inputs the csvs, applying a schema that enforces data types.  Repartition the data based on the source zip file (operations like joins are trivially parallilisable because each zip file can be processed indepdnently).  Output to parquet.

4. Create all addresses table 

SQL to create the all addresses table and output to parquet.




#### Notes

There don't seem to be any records of type 30 any more.  I searched through all the script and couldn't find, see [here](https://github.com/RobinL/build_addressbase_premium_spark/blob/d601368ab7f8cb7555c31634b21df15428487014/find_file_with_recordtype.ipynb).

