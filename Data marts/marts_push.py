import os
from python_api import get_conn

conn = get_conn()
conn.autocommit = True
cursor = conn.cursor()

print("Connected ✅")

# ---------------------------------------------------
# 1. ENSURE MART SCHEMA EXISTS
# ---------------------------------------------------
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'mart')
BEGIN
    EXEC('CREATE SCHEMA mart')
END
""")
print("Schema 'mart' ready ✅\n")

# ---------------------------------------------------
# 2. SQL FILES (YOUR MARTS)
# ---------------------------------------------------
mart_files = [
    "mart_finance.sql",
    "mart_customer.sql",
    "mart_merchant.sql",
]

for file in mart_files:

    if not os.path.exists(file):
        print(f"❌ {file} not found")
        continue

    print(f"\n📄 Running {file}")

    with open(file, "r", encoding="utf-8") as f:
        sql = f.read()

    # Split safely on GO
    batches = []
    current = []

    for line in sql.splitlines():
        if line.strip().upper() == "GO":
            if current:
                batches.append("\n".join(current))
                current = []
        else:
            current.append(line)

    if current:
        batches.append("\n".join(current))

    # Execute batches
    for i, batch in enumerate(batches, 1):
        try:
            cursor.execute(batch)
            print(f"   ✔ batch {i} OK")
        except Exception as e:
            print(f"   ❌ batch {i} failed")
            print(e)

# ---------------------------------------------------
# 3. VERIFY MART VIEWS ONLY
# ---------------------------------------------------
print("\n🔎 Checking mart views...")

cursor.execute("""
SELECT name
FROM sys.views
WHERE schema_id = SCHEMA_ID('mart')
ORDER BY name;
""")

rows = cursor.fetchall()

if not rows:
    print("⚠️ No views found in 'mart' schema")
else:
    for row in rows:
        print(f"✔ mart.{row[0]}")

cursor.close()
conn.close()

print("\n🎉 DONE - marts deployed")