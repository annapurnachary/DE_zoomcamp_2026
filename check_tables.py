import duckdb
import os

conn = duckdb.connect('bruin-pipeline/duckdb.db')

# Get ALL tables including system tables
print("=== All tables in database ===")
try:
    result = conn.execute("""
        SELECT table_schema, table_name, table_type 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name
    """).fetchall()
    
    if result:
        for schema, name, ttype in result:
            print(f"  {schema}.{name} ({ttype})")
    else:
        print("  (no tables found)")
except Exception as e:
    print(f"  Error: {e}")

# Try with duckdb_tables
print("\n=== Using duckdb_tables() ===")
try:
    result = conn.execute("SELECT schema_name, table_name FROM duckdb_tables() WHERE NOT internal").fetchall()
    for schema,name in result:
        print(f"  {schema}.{name}")
except Exception as e:
    print(f"  Error: {e}")

# Try with exact table names including underscores
print("\n=== Trying with underscores ===")
for name in ['dataset_players', 'dataset__players', 'players', 'dataset.players']:
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()
        print(f"  ✓ {name}: {result[0]} rows")
    except:
        pass

# Check the file size and modification time
print(f"\n=== Database file ===")
db_path = 'bruin-pipeline/duckdb.db'
if os.path.exists(db_path):
    st = os.stat(db_path)
    print(f"  Path: {db_path}")
    print(f"  Size: {st.st_size} bytes")
    import datetime
    mtime = datetime.datetime.fromtimestamp(st.st_mtime)
    print(f"  Last modified: {mtime}")

conn.close()
