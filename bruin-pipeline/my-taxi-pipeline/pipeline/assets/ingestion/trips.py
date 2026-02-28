"""@bruin

name: ingestion.trips
connection: gcp-default
materialization:
  type: table
  strategy: append
image: python:3.11

@bruin"""

import subprocess
import sys

# Install required package
#subprocess.run(
#    [sys.executable, "-m", "pip", "install", "google-cloud-bigquery"],
#    check=True
#)

from google.cloud import bigquery
import os
from datetime import datetime
import pandas as pd


# 🔹 UPDATE THESE
PROJECT_ID = "module-5-bruin"
DATASET = "ingestion"
TABLE = "trips"

GCS_BUCKET = "module-4-dbt"
GCS_PREFIX = "yellow-taxi/alldata/yellow"

import yaml
from google.oauth2 import service_account

# Define the path to your bruin.yml file
yaml_file_path = '.bruin.yml' # Adjust path if needed

# Load the YAML content
with open(yaml_file_path, 'r') as f:
    config = yaml.safe_load(f)

# Extract the GCP connection details from the config (adjust keys based on your YAML structure)
# This is an example, you need to match the exact keys used in your bruin.yml
#gcp_credentials_file = config.get('connections', {}).get('google_cloud_platform', {}).get('service_account_file', {})
#gcp_credentials_file = config.get('connections', {}).get('google_cloud_platform', {}).get('service_account_file')
gcp_credentials_file = config.get('connections').get('google_cloud_platform').get('service_account_file')

# Create credentials object (assuming the credentials are in a service account JSON format)
# You might need to convert the dictionary to a proper credentials object
#credentials = service_account.Credentials.from_service_account_info(gcp_credentials_info)
print(type(gcp_credentials_file))
print(gcp_credentials_file)

credentials = service_account.Credentials.from_service_account_file(gcp_credentials_file)
#project_id = credentials.project_id
#return bigquery.Client(credentials=credentials, project=project_id)

# Use the credentials with a GCP client library (e.g., BigQuery)
#client = bigquery.Client(credentials=credentials, project=credentials.project_id)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    #client = bigquery.Client(project=PROJECT_ID)

    current = start

    while current <= end:
        year = current.year
        month = current.month

        source_uri = (
            f"gs://{GCS_BUCKET}/{GCS_PREFIX}/"
            f"yellow_tripdata_{year}-{month:02d}.parquet"
        )

        print(f"Starting BigQuery load from: {source_uri}")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        load_job = client.load_table_from_uri(
            source_uri,
            f"{PROJECT_ID}.{DATASET}.{TABLE}",
            job_config=job_config,
        )

        load_job.result()

        print(f"Loaded {year}-{month:02d} successfully")

        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    # Bruin expects a dataframe return
    return pd.DataFrame()