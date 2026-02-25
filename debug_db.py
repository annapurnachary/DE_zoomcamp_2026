import duckdb
import os

conn = duckdb.connect('bruin-pipeline/duckdb.db')

# Check what happens when we query each table
tables_to_try = [
    'players',
    'dataset.players',
    'main.players', 
    'ad894051fe94b5356e85b6f0879ebda33_players',  # dlt pipeline ID based name
]

print("=== Trying different table names ===\n")

for table_name in tables_to_try:
    try:
        result = conn.execute(f'SELECT * FROM {table_name} LIMIT 2').fetchall()
        print(f"✓ Found {table_name}:")
        print(f"  {result}\n")
    except Exception as e:
        print(f"✗ {table_name}: {str(e)[:80]}\n")

# List all columns and tables using PRAGMA
print("\n=== All DB Objects ===")
try:
    result = conn.execute('PRAGMA show_tables').fetchall()
    for r in result:
        print(f"  {r}")
except:
    pass

# Try selecting from specific schema
print("\n=== Trying SELECT * FROM (SELECT * FROM duckdb_views())  ===")
try:
    result = conn.execute('SELECT * FROM duckdb_views()').fetchall()
    for r in result:
        print(f"  {r}")
except Exception as e:
    print(f"Error: {e}")

conn.close()
