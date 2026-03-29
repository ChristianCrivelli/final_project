from python_api import get_conn

# ==============================================================
# CLEAN SCHEMA DDL — Run once before any clean_*.py scripts
# Creates the 'clean' schema in SQL Server
# ==============================================================

conn = get_conn()
conn.autocommit = True
cursor = conn.cursor()

print("Connected to database ✅")

cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'clean')
        EXEC('CREATE SCHEMA clean')
""")
print("Schema 'clean' ready ✅")

cursor.close()
conn.close()
