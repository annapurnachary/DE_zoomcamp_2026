import argparse
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.pg_user
    password = params.pg_pass
    host = params.pg_host
    port = params.pg_port
    db = params.pg_db
    table = params.target_table
    parquet_file = params.parquet_file

    print("Reading parquet file...")
    df = pd.read_parquet(parquet_file)

    print(f"Total rows: {len(df)}")

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}'
    )

    chunk_size = 100_000
    print(f"Loading data in chunks of {chunk_size}...")

    # Create table first
    df.head(0).to_sql(
        name=table,
        con=engine,
        if_exists='replace',
        index=False
    )

    # Insert data in chunks
    for i in range(0, len(df), chunk_size):
        df.iloc[i:i + chunk_size].to_sql(
            name=table,
            con=engine,
            if_exists='append',
            index=False
        )
        print(f"Inserted rows {i} to {i + chunk_size}")

    print("✅ Data loaded successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--pg-user')
    parser.add_argument('--pg-pass')
    parser.add_argument('--pg-host')
    parser.add_argument('--pg-port')
    parser.add_argument('--pg-db')
    parser.add_argument('--target-table')
    parser.add_argument('--parquet-file')

    args = parser.parse_args()
    main(args)
