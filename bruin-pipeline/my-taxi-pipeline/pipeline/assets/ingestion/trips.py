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
subprocess.run(
    [sys.executable, "-m", "pip", "install", "google-cloud-bigquery"],
    check=True
)

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


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    client = bigquery.Client(project=PROJECT_ID)

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