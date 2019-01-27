from python_scripts.abp_links_from_html import abp_links_to_json
# from python_scripts.parallel_download import download_abp_zips

# Extract the download links and put them in s3
# s3_location = "s3://alpha-everyone/addressbase_premium/html/os_download_links_page.htm"
# abp_links_to_json(s3_location)

from etl_manager.etl import GlueJob

my_role = 'alpha_user_robinl'
bucket = 'alpha-everyone'

job = GlueJob('glue_jobs/abp', bucket=bucket, job_role=my_role, job_arguments={"--test_arg" : 'some_string'})
job.run_job()
