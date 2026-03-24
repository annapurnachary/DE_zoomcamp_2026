import io
import os
import requests
import zipfile
import pandas as pd
from google.cloud import storage

# --- Configuration ---
# The Divvy data is hosted in an S3 bucket: https://divvy-tripdata.s3.amazonaws.com
BASE_URL = "https://divvy-tripdata.s3.amazonaws.com"
BUCKET = "divvy_data_lake"  # Update this to your Terraform-created bucket

def upload_to_gcs(bucket_name, object_name, data_io):
    """Uploads a file-like object directly to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_file(data_io, rewind=True)
    print(f"Successfully uploaded to GCS: gs://{bucket_name}/{object_name}")

def divvy_to_gcs(year, month):
    # Divvy filenames usually follow: YYYYMM-divvy-tripdata.zip
    # Note: Some older years (2013-2019) use different naming (e.g., Divvy_Trips_2019_Q1.zip)
    file_prefix = f"{year}{month:02d}-divvy-tripdata"
    zip_file_name = f"{file_prefix}.zip"
    request_url = f"{BASE_URL}{zip_file_name}"

    print(f"Downloading: {request_url}")
    r = requests.get(request_url)
    
    if r.status_code != 200:
        print(f"Failed to download {zip_file_name}. Check if the file exists for this date.")
        return

    # Use BytesIO to handle the zip file in memory
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        # Find the CSV inside the ZIP (ignoring hidden __MACOSX folders)
        csv_files = [f for f in z.namelist() if f.endswith('.csv') and not f.startswith('__')]
        
        for csv_file in csv_files:
            print(f"Processing: {csv_file}")
            with z.open(csv_file) as f:
                # Read CSV into Pandas
                df = pd.read_csv(f)
                
                # Convert to Parquet in memory
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
                
                # Upload to GCS
                gcs_path = f"raw/divvy/{year}/{month:02d}/{csv_file.replace('.csv', '.parquet')}"
                upload_to_gcs(BUCKET, gcs_path, parquet_buffer)

# --- Execution ---
if __name__ == "__main__":
    # Example: Load January to March 2024
    for m in range(1, 4):
        divvy_to_gcs(2024, m)
