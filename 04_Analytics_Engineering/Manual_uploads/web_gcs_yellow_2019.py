import os
import requests
import pandas as pd
from google.cloud import storage

# ==============================
# CONFIG
# ==============================
PROJECT_ID = "module-4-dbt-487218"
BUCKET_NAME = "module-4-dbt"
GCS_FOLDER = "yellow-taxi/alldata"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Anu_Study\zoom_camp\GCP\Module-3\module-3-bigquery.json"
# ==============================
# FORCE CONSISTENT SCHEMA
# ==============================
SCHEMA = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
    "airport_fee": "float64",   # ADD THIS
}

# ==============================
# DOWNLOAD + STANDARDIZE
# ==============================
def process_month(year, month):
    file_name = f"yellow_tripdata_{year}-{month:02d}.csv.gz"
    url = f"{BASE_URL}/{file_name}"

    print(f"Downloading {file_name}...")
    response = requests.get(url)
    open(file_name, "wb").write(response.content)

    print("Reading into pandas...")
    df = pd.read_csv(file_name, compression="gzip")

    # Convert datetime columns
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # Enforce schema
    for col, dtype in SCHEMA.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    # Write standardized parquet
    parquet_file = file_name.replace(".csv.gz", ".parquet")
    df.to_parquet(parquet_file, engine="pyarrow")

    return parquet_file


# ==============================
# UPLOAD TO GCS
# ==============================
def upload_to_gcs(file_path):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    blob_path = f"{GCS_FOLDER}/{os.path.basename(file_path)}"
    blob = bucket.blob(blob_path)

    print(f"Uploading {file_path} to gs://{BUCKET_NAME}/{blob_path}")
    blob.upload_from_filename(file_path)


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    for month in range(1, 13):
        parquet_file = process_month(2019, month)
        upload_to_gcs(parquet_file)

    print("All 2020 Yellow Taxi files uploaded successfully!")
