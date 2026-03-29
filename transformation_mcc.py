from os import write
import csv
import pandas as pd
from python_api import get_conn

# ─────────────────────────────────────────────
# STEP 1 — CONNECT
# ─────────────────────────────────────────────
conn = get_conn()
conn.autocommit = False
cursor = conn.cursor()
cursor.fast_executemany = True

print("Connected to database ✅")

# ─────────────────────────────────────────────
# STEP 2 — LOAD FROM INGESTION LAYER
# ─────────────────────────────────────────────
df = pd.read_sql("SELECT * FROM ingestion.mcc_data", conn)
print(f"Loaded {len(df)} rows from ingestion.mcc_data")
print(f"Duplicates before cleaning: {df.duplicated().sum()}")

# ─────────────────────────────────────────────
# STEP 3 — TRANSFORMATION LOGIC
# ─────────────────────────────────────────────

# --- code: strip quotes, MCC prefix, cast to int ---
df['code'] = (
    df['code']
    .astype(str)
    .str.replace(r'^"+|"+$', '', regex=True)
    .str.replace(r'^MCC', '', regex=True)
    .str.strip()
)
df['code'] = pd.to_numeric(df['code'], errors='coerce')
df = df.dropna(subset=['code'])
df['code'] = df['code'].astype(int)

# --- description: clean quotes, strip, title case ---
df['description'] = (
    df['description']
    .fillna('')
    .str.replace('"', '', regex=False)   # remove quotes
    .str.strip()
    .str.title()
)

# --- notes: fill nulls ---
df['notes'] = (
    df['notes']
    .fillna('N/A')
    .str.strip()
)

# --- updated_by: fill nulls, lowercase ---
df['updated_by'] = (
    df['updated_by']
    .fillna('unknown')
    .str.strip()
    .str.lower()
)

# --- deduplicate ---
before = len(df)
df = df.drop_duplicates().reset_index(drop=True)
print(f"Removed {before - len(df)} duplicates")
print(f"Cleaned rows: {len(df)}")

# ─────────────────────────────────────────────
# STEP 4 — CREATE CLEAN TABLE
# ─────────────────────────────────────────────
cursor.execute("""
    IF OBJECT_ID('clean.mcc', 'U') IS NOT NULL
        DROP TABLE clean.mcc
""")
cursor.execute("""
    CREATE TABLE clean.mcc (
        code        INT           NOT NULL,
        description NVARCHAR(255) NOT NULL,
        notes       NVARCHAR(255) NOT NULL,
        updated_by  NVARCHAR(100) NOT NULL
    )
""")
print("clean.mcc created ✅")

# ─────────────────────────────────────────────
# STEP 5 — INSERT CLEAN DATA
# ─────────────────────────────────────────────
data = list(df.itertuples(index=False, name=None))
cursor.executemany(
    "INSERT INTO clean.mcc (code, description, notes, updated_by) VALUES (?, ?, ?, ?)",
    data
)
conn.commit()
print(f"Inserted {len(data)} rows into clean.mcc ✅")

# ─────────────────────────────────────────────
# STEP 6 — VERIFY
# ─────────────────────────────────────────────
count = cursor.execute("SELECT COUNT(*) FROM clean.mcc").fetchone()[0]
print(f"Final row count in clean.mcc: {count}")

cursor.close()
conn.close()
