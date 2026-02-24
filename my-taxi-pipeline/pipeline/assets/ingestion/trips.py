"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

columns:
  - name: tpep_pickup_datetime
    type: timestamp
  - name: tpep_dropoff_datetime
    type: timestamp
  - name: PULocationID
    type: bigint
  - name: DOLocationID
    type: bigint
  - name: fare_amount
    type: float
  - name: taxi_type
    type: string
  - name: payment_type
    type: bigint

@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
import pyarrow.util
import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "install", "python-dateutil"])

pyarrow.util.download_tzdata_on_windows()


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    dfs = []
    current = start

    while current <= end:
        year = current.year
        month = current.month

        for taxi_type in taxi_types:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            try:
                df = pd.read_parquet(url)

                # Add taxi_type column immediately
                df["taxi_type"] = taxi_type

                dfs.append(df)
                print(f"Loaded {taxi_type} data for {year}-{month:02d}")
            except Exception as e:
                print(f"Warning: Could not load {taxi_type} data for {year}-{month:02d}: {e}")

        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    if not dfs:
        return pd.DataFrame()

    final_dataframe = pd.concat(dfs, ignore_index=True)

    # Remove timezone info (Windows fix)
    if "tpep_pickup_datetime" in final_dataframe.columns:
        final_dataframe["tpep_pickup_datetime"] = (
            pd.to_datetime(final_dataframe["tpep_pickup_datetime"])
            .dt.tz_localize(None)
        )

    if "tpep_dropoff_datetime" in final_dataframe.columns:
        final_dataframe["tpep_dropoff_datetime"] = (
            pd.to_datetime(final_dataframe["tpep_dropoff_datetime"])
            .dt.tz_localize(None)
        )

    # Keep only columns needed downstream
    required_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "taxi_type",
        "payment_type"
    ]

    final_dataframe = final_dataframe[
        [col for col in required_columns if col in final_dataframe.columns]
    ]

    return final_dataframe
