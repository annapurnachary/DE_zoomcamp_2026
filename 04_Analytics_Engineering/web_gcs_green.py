import os
import requests
import pandas as pd
from google.cloud import storage

# ==========================================
# CONFIG
# ==========================================
PROJECT_ID = "module-4-dbt-487218"
BUCKET_NAME = "module-4-dbt"
GCS_FOLDER = "green"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Anu_Study\zoom_camp\GCP\Module-3\module-3-bigquery.json"

# ==========================================
# UNIFIED SCHEMA (2019 + 2020 SAFE)
# ==========================================
SCHEMA = {
    "VendorID": "Int64",
    "lpep_pickup_datetime": "datetime64[ns]",
    "lpep_dropoff_datetime": "datetime64[ns]",
    "store_and_fwd_flag": "string",
    "RatecodeID": "Int64",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "ehail_fee": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "payment_type": "Int64",
    "trip_type": "Int64",
    "congestion_surcharge": "float64",
    "airport_fee": "float64",   # may not exist in 2019
}

# ==========================================
# DOWNLOAD + STANDARDIZE
# ==========================================
def process_month(year, month):
    file_name = f"green_tripdata_{year}-{month:02d}.csv.gz"
    url = f"{BASE_URL}/{file_name}"

    print(f"Downloading {file_name}...")
    response = requests.get(url)
    open(file_name, "wb").write(response.content)

    print("Reading into pandas...")
    df = pd.read_csv(file_name, compression="gzip")

    # Enforce datetime columns
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    # Ensure all expected columns exist
    for col in SCHEMA.keys():
        if col not in df.columns:
            df[col] = None

    # Enforce consistent types
    for col, dtype in SCHEMA.items():
        if "datetime" not in dtype:
            df[col] = df[col].astype(dtype)

    # Write parquet
    parquet_file = file_name.replace(".csv.gz", ".parquet")
    df.to_parquet(parquet_file, engine="pyarrow", index=False)

    return parquet_file


# ==========================================
# UPLOAD TO GCS
# ==========================================
def upload_to_gcs(file_path, year):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    blob_path = f"{GCS_FOLDER}/{year}/{os.path.basename(file_path)}"
    blob = bucket.blob(blob_path)

    print(f"Uploading to gs://{BUCKET_NAME}/{blob_path}")
    blob.upload_from_filename(file_path)


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":

    for year in [2019, 2020]:
        for month in range(1, 13):
            parquet_file = process_month(year, month)
            upload_to_gcs(parquet_file, year)

    print("Green taxi 2019–2020 successfully uploaded!")
