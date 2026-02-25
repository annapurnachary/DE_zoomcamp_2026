import requests
from google.cloud import storage

BUCKET_NAME = "module-4-dbt"
GCS_FOLDER = "fhv/2019"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

MONTHS = [f"{i:02d}" for i in range(1, 13)]

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

for month in MONTHS:
    file_name = f"fhv_tripdata_2019-{month}.parquet"
    download_url = f"{BASE_URL}/{file_name}"

    print(f"Downloading & uploading {file_name}...")

    response = requests.get(download_url, stream=True)

    if response.status_code != 200:
        print(f"❌ Failed: {file_name}")
        continue

    blob = bucket.blob(f"{GCS_FOLDER}/{file_name}")

    blob.upload_from_file(response.raw)

    print(f"✅ Uploaded {file_name}")

print("🎉 Done")
