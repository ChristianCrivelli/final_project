from python_api import get_conn

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
