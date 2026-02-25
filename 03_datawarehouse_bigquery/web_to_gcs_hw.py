import io
import os
import requests
import pandas as pd
from google.cloud import storage

# ============================
# CONFIG
# ============================
PROJECT_ID = "module-3-bigquery-486618"
BUCKET = "module-3-yellow-2024"
SERVICE = "yellow"
YEAR = 2024

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# ============================
# GCS Upload Function
# ============================
def upload_to_gcs(bucket_name, destination_blob_name, local_file):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(local_file)
    print(f"✅ Uploaded to gs://{bucket_name}/{destination_blob_name}")

# ============================
# Download + Upload
# ============================
def web_to_gcs(year, service):
    for month in range(1, 13):
        month_str = f"{month:02d}"

        file_name = f"{service}_tripdata_{year}-{month_str}.parquet"
        file_url = f"{BASE_URL}/{file_name}"

        print(f"⬇️ Downloading {file_url}")

        response = requests.get(file_url, stream=True)
        if response.status_code != 200:
            print(f"❌ Failed to download {file_name}")
            continue

        with open(file_name, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

        upload_to_gcs(
            BUCKET,
            f"{service}/{file_name}",
            file_name
        )

        os.remove(file_name)
        print(f"🧹 Removed local file {file_name}")

# ============================
# MAIN
# ============================
if __name__ == "__main__":
    web_to_gcs(YEAR, SERVICE)
