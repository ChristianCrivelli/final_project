import os
from python_api import get_conn

# MARTS :  Create Views
# The role of this python file is to take the SQL scripts in the same folder and push them to the server.
# This time we dont create TABLES but VIEWS, that will help us answer our business questions.

# Typical get_conn() function to connect to the server
conn = get_conn()
conn.autocommit = True
cursor = conn.cursor()
print("Connected ✅\n")


# We load the queries pre-created
mart_files = [
    "mart_finance.sql",
    "mart_customer.sql",
    "mart_merchant.sql",
]

for filename in mart_files:
    if not os.path.exists(filename):
        print(f" {filename} not found — skipping")
        continue

    sql = open(filename, encoding="utf-8").read()

    # Split on GO — SSMS batch separator, not valid in pyodbc
    batches = [b.strip() for b in sql.split("\nGO") if b.strip()
               and not b.strip().startswith("--")]

    print(f"{filename}")
    for batch in batches:
        try:
            cursor.execute(batch)
            # extract view name for feedback
            name = [l for l in batch.splitlines() if "VIEW" in l.upper()]
            label = name[0].split("VIEW")[-1].strip() if name else "..."
            print(f"   ok . {label}")
        except Exception as e:
            print(f"   not ok. {e}")
    print()

# Verification
cursor.close()
conn.close()
print("🎉 All marts created")
