"""@bruin

name: ingestion.trips
connection: gcp-default
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


# 🔹 CHANGE THIS TO YOUR BUCKET NAME
GCS_BUCKET = "module-4-dbt/yellow-taxi/alldata"


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ.get("BRUIN_VARS", "{}")).get(
        "taxi_types", ["yellow"]
    )

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    dfs = []
    current = start

    while current <= end:
        year = current.year
        month = current.month

        for taxi_type in taxi_types:

            gcs_path = (
                f"gs://{GCS_BUCKET}/{taxi_type}/"
                f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            )

            try:
                print(f"Loading from GCS: {gcs_path}")

                df = pd.read_parquet(gcs_path, engine="pyarrow")

                df["taxi_type"] = taxi_type
                dfs.append(df)

                print(f"Loaded {taxi_type} {year}-{month:02d}")

            except Exception as e:
                print(f"WARNING: Could not load {gcs_path} — {str(e)}")
                continue

        # move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    if not dfs:
        raise RuntimeError("No data could be loaded from GCS.")

    final_dataframe = pd.concat(dfs, ignore_index=True)

    # -----------------------------
    # Timestamp cleaning
    # -----------------------------
    final_dataframe["tpep_pickup_datetime"] = (
        pd.to_datetime(final_dataframe["tpep_pickup_datetime"], errors="coerce")
        .dt.tz_localize(None)
    )

    final_dataframe["tpep_dropoff_datetime"] = (
        pd.to_datetime(final_dataframe["tpep_dropoff_datetime"], errors="coerce")
        .dt.tz_localize(None)
    )

    final_dataframe = final_dataframe.dropna(
        subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    )

    # -----------------------------
    # Explicit BigQuery-safe casting
    # -----------------------------
    final_dataframe["PULocationID"] = (
        pd.to_numeric(final_dataframe["PULocationID"], errors="coerce")
        .astype("Int64")
    )

    final_dataframe["DOLocationID"] = (
        pd.to_numeric(final_dataframe["DOLocationID"], errors="coerce")
        .astype("Int64")
    )

    final_dataframe["payment_type"] = (
        pd.to_numeric(final_dataframe["payment_type"], errors="coerce")
        .astype("Int64")
    )

    final_dataframe["fare_amount"] = pd.to_numeric(
        final_dataframe["fare_amount"], errors="coerce"
    ).astype("float64")

    final_dataframe["taxi_type"] = final_dataframe["taxi_type"].astype("string")

    # -----------------------------
    # Select only required columns
    # -----------------------------
    final_dataframe = final_dataframe[
        [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "fare_amount",
            "taxi_type",
            "payment_type",
        ]
    ]

    return final_dataframe