import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parent
csv_path = BASE_DIR / "data" / "taxi_zone_lookup.csv"  # note exact filename

# Read CSV
df = pd.read_csv(csv_path, encoding="utf-8-sig")

# Rename columns to snake_case
df.columns = ["location_id", "borough", "zone", "service_zone"]

# Connect to Postgres
engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")
# Load table
df.to_sql(name="taxi_zone_lookup", con=engine, if_exists="replace", index=False)

print("✅ taxi_zone_lookup table loaded successfully!")
