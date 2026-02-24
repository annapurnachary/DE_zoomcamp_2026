import duckdb

# Connect to DuckDB
conn = duckdb.connect('bruin-pipeline/duckdb.db')

print("=== Checking for all tables ===\n")

# Try the query from player_stats.sql
try:
    print("Attempting: SELECT name, count(*) AS player_count FROM dataset.players GROUP BY 1\n")
    result = conn.execute("SELECT name, count(*) AS player_count FROM dataset.players GROUP BY 1").fetchall()
    print("✓ Results from dataset.player_stats:")
    for row in result:
        print(f"  {row}")
except Exception as e:
    print(f"✗ Error: {str(e)[:150]}")
    
    print("\n--- Trying alternate queries ---\n")
    
    # List all tables
    try:
        result = conn.execute("SELECT * FROM duckdb_tables() WHERE NOT internal").fetchall()
        print(f"Available tables: {len(result)}")
        for row in result:
            print(f"  {row[0]}.{row[1]}")
    except:
        pass

conn.close()
