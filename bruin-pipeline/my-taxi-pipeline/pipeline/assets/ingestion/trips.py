"""@bruin

name: ingestion.trips
connection: gcp-default
image: python:3.11

@bruin"""

from google.cloud import bigquery
from datetime import datetime
import os


# ---- CONFIG ----
PROJECT_ID = "module-5-bruin"
DATASET = "ingestion"
TABLE = "trips"

GCS_BUCKET = "module-4-dbt"
GCS_PREFIX = "yellow-taxi/alldata/yellow"
# ----------------


def materialize():
    # Bruin provides these automatically
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Bruin already injects credentials
    client = bigquery.Client(project=PROJECT_ID)

    current = start

    while current <= end:
        year = current.year
        month = current.month

        source_uri = (
            f"gs://{GCS_BUCKET}/{GCS_PREFIX}/"
            f"yellow_tripdata_{year}-{month:02d}.parquet"
        )

        print(f"Loading from GCS → {source_uri}")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        load_job = client.load_table_from_uri(
            source_uri,
            f"{PROJECT_ID}.{DATASET}.{TABLE}",
            job_config=job_config,
        )

        load_job.result()  # Wait for job

        print(f"Loaded {year}-{month:02d} successfully")

        # next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    print("All months loaded successfully")

    # Bruin requires a dataframe return
    import pandas as pd
    return pd.DataFrame()