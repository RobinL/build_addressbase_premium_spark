FROM python:3.6

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY main.py main.py
COPY sync_glue_metadata.py sync_glue_metadata.py
COPY python_scripts python_scripts/
COPY glue_jobs glue_jobs/
COPY meta_data /meta_data
