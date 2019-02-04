from python_scripts.abp_links_from_html import abp_links_to_json
from glue_jobs.abp.glue_py_resources.settings import CONTROL_FILES_BASE_FOLDER_S3

# Extract the download links and put them in s3
s3_location = "{}/os_download_links_page.htm".format(CONTROL_FILES_BASE_FOLDER_S3)
abp_links_to_json(s3_location)

from etl_manager.etl import GlueJob

my_role = 'alpha_user_robinl'
bucket = 'alpha-everyone'

job = GlueJob('glue_jobs/abp', bucket=bucket, job_role=my_role, job_arguments={"--test_arg" : 'some_string', '--enable-metrics': ''})
job.allocated_capacity = 5
job.run_job()