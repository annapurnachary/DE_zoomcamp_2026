import duckdb
import os

print('Looking for duckdb files and their tables...\n')

for root, dirs, files in os.walk('.'):
    for f in files:
        if f.endswith('.db'):
            fullpath = os.path.join(root, f)
            try:
                conn = duckdb.connect(fullpath)
                tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
                table_names = [t[0] for t in tables]
                print(f"File: {fullpath}")
                print(f"  Tables: {table_names if table_names else '(none)'}")
                
                # Try to find players table
                if any('player' in t.lower() for t in table_names):
                    print("  Found player-related table!")
                    for tn in table_names:
                        if 'player' in tn.lower():
                            result = conn.execute(f"SELECT * FROM {tn} LIMIT 3").fetchall()
                            print(f"    {tn}: {result}")
                print()
            except Exception as e:
                print(f"Error with {fullpath}: {e}\n")
