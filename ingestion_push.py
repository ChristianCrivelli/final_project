import pandas as pd
import os
import time
from python_api import get_conn
from config import DATA_FOLDER, DB_TYPE

# 1) CONNECTION
# Again we connect to our server
conn = get_conn()
conn.autocommit = True
cursor = conn.cursor()

print("✅ Connected to database")


# 2) FILE MAP
# Basically mapping all our files
file_map = {
    "cards_data.csv":        "ingestion.cards_data",
    "mcc_data.csv":          "ingestion.mcc_data",
    "transactions_data.csv": "ingestion.transactions_data",
    "users_data.csv":        "ingestion.users_data",
}

# FUNCTION  :  SQL SERVER BULK INSERT
# This function will help us insert per bulk data from our source files.
# Indeed, transactio  happens to be really really huge and we need to establish a good way to import it.

def load_sqlserver_bulk(filepath, table):
    abs_path = os.path.abspath(filepath).replace("\\", "/")

    cursor.execute(f"TRUNCATE TABLE {table}")

    cursor.execute(f"""
        BULK INSERT {table}
        FROM '{abs_path}'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            CODEPAGE = '65001',
            TABLOCK
        )
    """)

    return cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]



# POSTGRES COPY 

def load_postgres_copy(filepath, table):
    import psycopg

    cursor.execute(f"TRUNCATE TABLE {table}")

    # IMPORTANT: separate psycopg connection required in real setups
    with open(filepath, "r", encoding="utf-8") as f:
        next(f)  # skip header

        cursor.copy(f"""
            COPY {table}
            FROM STDIN
            WITH (FORMAT CSV)
        """).write(f.read())

    return cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]



# FALLBACK for the load chuunk

def load_chunked_fallback(filepath, table, chunksize=200_000):

    cursor.execute(f"TRUNCATE TABLE {table}")
    cursor.fast_executemany = True

    total = 0

    for chunk in pd.read_csv(filepath, dtype=str, chunksize=chunksize):

        chunk = chunk.fillna("")

        # FAST STRIP (no apply)
        for col in chunk.columns:
            if chunk[col].dtype == "object":
                chunk[col] = chunk[col].str.strip()

        cols = ", ".join(chunk.columns)
        params = ", ".join(["?" for _ in chunk.columns])
        sql = f"INSERT INTO {table} ({cols}) VALUES ({params})"

        data = list(chunk.itertuples(index=False, name=None))

        cursor.executemany(sql, data)

        conn.commit()
        total += len(data)

        print(f"   ➜ {table}: {total:,} rows")

    return total



# MAIN LOOP
# Our main loop, basically, we run the functions we defined beforehand.

print("\n📂 Loading CSV files...\n")

start_total = time.time()  # getting the time to see how it is loading
# loop to get all the files
for filename, table in file_map.items():

    filepath = os.path.join(DATA_FOLDER, filename)

    if not os.path.exists(filepath):
        print(f"⚠️ Missing: {filename}")
        continue

    print(f"\n📥 {table}...")

    start = time.time()

    try:
        if DB_TYPE == "sqlserver":
            count = load_sqlserver_bulk(filepath, table)

        elif DB_TYPE == "postgres":
            count = load_postgres_copy(filepath, table)

        else:
            count = load_chunked_fallback(filepath, table)

        elapsed = time.time() - start
        print(f"✅ {table:<40} {count:>10,} rows ({elapsed:.1f}s)")

    except Exception as e:
        print(f"❌ Failed {table}: {e}")
        print("↳ fallback triggered...")

        count = load_chunked_fallback(filepath, table)
        print(f"✅ {table:<40} {count:>10,} rows (fallback)")

# VERIFICATION
# Basically checking the data of our ingestion

print("\n── Row counts ──────────────────────────────────────────")

for table in file_map.values():
    count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"   {table:<42} {count:>12,} rows")

print(f"\n🎉 Yippie. Done in {time.time() - start_total:.1f}s")

# Closing because you see it is important
cursor.close()
conn.close()
