import duckdb

# Connect to DuckDB
conn = duckdb.connect('bruin-pipeline/duckdb.db')

# Get all tables from main schema
print("=== Tables in 'main' schema ===")
tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
if tables:
    for t in tables:
        print(f"  - {t[0]}")
else:
    print("  (none)")

# Try to query players table with different references
print("\n=== Attempting to query 'players' ===")
try:
    result = conn.execute("SELECT * FROM players LIMIT 5").fetchall()
    print("Success! Players table contents:")
    print(result)
except Exception as e:
    print(f"Error: {e}")

print("\n=== Attempting to query 'main.players' ===")
try:
    result = conn.execute("SELECT * FROM main.players LIMIT 5").fetchall()
    print("Success! Players table contents:")
    print(result)
except Exception as e:
    print(f"Error: {e}")

print("\n=== Attempting to query 'dataset.players' ===")
try:
    result = conn.execute("SELECT * FROM dataset.players LIMIT 5").fetchall()
    print("Success! Players table contents:")
    print(result)
except Exception as e:
    print(f"Error: {e}")

# List all views and tables
print("\n=== All objects (PRAGAMA) ===")
try:
    result = conn.execute("PRAGMA show_tables").fetchall()
    for r in result:
        print(f"  {r}")
except Exception as e:
    print(f"Error: {e}")

conn.close()
