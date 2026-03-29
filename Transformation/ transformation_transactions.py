import pandas as pd
import numpy as np
import time
from python_api import get_conn

# Connect

conn_read  = get_conn()
conn_write = get_conn()

read_cursor  = conn_read.cursor()
write_cursor = conn_write.cursor()
write_cursor.fast_executemany = True

print("Connected to database ✅")

# Constants

CHUNK_SIZE = 200_000

VALID_US_STATES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
    'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'
}

def to_python(val):
    if val is None or val is pd.NA or val is pd.NaT:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        return float(val)
    if isinstance(val, np.bool_):
        return bool(val)
    if isinstance(val, pd.Timestamp):
        return val.date()
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val

# Total row count
total_in_db = read_cursor.execute(
    "SELECT COUNT(*) FROM ingestion.transactions_data"
).fetchone()[0]
print(f"Total rows in ingestion.transactions_data: {total_in_db:,}")

# Clean table
write_cursor.execute("""
    IF OBJECT_ID('clean.transactions', 'U') IS NOT NULL
        DROP TABLE clean.transactions
""")


write_cursor.execute("""
    CREATE TABLE clean.transactions (
        id               INT            NOT NULL,
        date             DATE,
        client_id        INT,
        card_id          INT,
        amount           DECIMAL(18,2),
        is_refund        BIT            NOT NULL,
        use_chip         NVARCHAR(50),
        merchant_id      INT,
        merchant_county  VARCHAR(100), 
        merchant_state   VARCHAR(5),
        merchant_country VARCHAR(100),
        zip              NVARCHAR(20),
        mcc              INT,
        errors           NVARCHAR(255)
    )
""")
conn_write.commit()
print("clean.transactions created ✅\n")

# Clean function

def clean_chunk(chunk: pd.DataFrame) -> pd.DataFrame:

    # Id
    chunk['id'] = pd.to_numeric(chunk['id'], errors='coerce')
    chunk = chunk.dropna(subset=['id'])
    chunk['id'] = chunk['id'].astype(int)


    for col in ['client_id', 'card_id', 'merchant_id']:
        chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype('Int64')

    # Set date to Python date object
    chunk['date'] = pd.to_datetime(chunk['date'], errors='coerce').dt.date

    # Amount
    chunk['amount'] = (
        chunk['amount']
        .astype(str)
        .str.replace(r'[$,]', '', regex=True)
        .str.strip()
    )
    chunk['amount'] = pd.to_numeric(chunk['amount'], errors='coerce')

    # is_refund
    chunk['is_refund'] = (chunk['amount'] < 0).astype(bool)

    # use_chip
    chunk['use_chip'] = (
    chunk['use_chip']
    .str.strip()
    .str.replace(' Transaction', '', regex=False)
)

    # mcc 
    chunk['mcc'] = pd.to_numeric(chunk['mcc'], errors='coerce').astype('Int64')

    # Set errors to NULL
    chunk['errors'] = (
        chunk['errors']
        .replace({'': None, 'nan': None, 'None': None})
        .where(chunk['errors'].notna(), None)
    )

    # Merchant_city 
    chunk['merchant_county'] = (
    chunk['merchant_city']
    .fillna('Unknown')
    .astype(str)
    .str.replace(r'\bCounty\b', '', regex=True)
    .str.strip()
    .str.title()
    )

    # zip 
    chunk['zip'] = (
        chunk['zip']
        .fillna('')
        .astype(str)
        .str.replace(r'\.0$', '', regex=True)
        .str.strip()
        .replace('', None)
    )

    # merchant_state + merchant_country 
    raw_state = (
        chunk['merchant_state']
        .fillna('')
        .astype(str)
        .str.strip()
        .str.upper()
    )

    is_us      = raw_state.isin(VALID_US_STATES)
    is_unknown = raw_state.isin(['', 'UNKNOWN', 'NAN'])

    chunk['merchant_state']   = np.where(is_us, raw_state, None)
    chunk['merchant_country'] = np.where(
        is_us, 'USA',
        np.where(is_unknown, None, raw_state.str.title())
    )

    return chunk


COLS = [
    'id','date','client_id','card_id','amount','is_refund',
    'use_chip','merchant_id','merchant_county',
    'merchant_state','merchant_country','zip','mcc','errors'
]

INSERT_SQL = """
    INSERT INTO clean.transactions (
        id, date, client_id, card_id, amount, is_refund,
        use_chip, merchant_id, merchant_county,
        merchant_state, merchant_country, zip, mcc, errors
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


total_inserted = 0
total_dropped  = 0
start          = time.time()

for chunk in pd.read_sql(
    "SELECT * FROM ingestion.transactions_data",
    conn_read,
    chunksize=CHUNK_SIZE
):
    raw_len = len(chunk)
    chunk   = clean_chunk(chunk)
    chunk   = chunk.drop_duplicates(subset=['id','date','client_id','card_id','merchant_id'])
    total_dropped += raw_len - len(chunk)

    # to_python() on every value — the only safe way with mixed nullable types
    data = [
        tuple(to_python(v) for v in row)
        for row in chunk[COLS].itertuples(index=False, name=None)
    ]

    write_cursor.executemany(INSERT_SQL, data)
    conn_write.commit()

    total_inserted += len(data)
    elapsed = time.time() - start
    rate    = total_inserted / elapsed if elapsed else 0
    pct     = (total_inserted / total_in_db * 100) if total_in_db else 0
    print(f"   ➜ {total_inserted:>10,} / {total_in_db:,}  ({pct:.1f}%)  ~{rate:,.0f} rows/s")


count   = read_cursor.execute("SELECT COUNT(*) FROM clean.transactions").fetchone()[0]
elapsed = time.time() - start

print("\n── Summary ─────────────────────────────────────")
print(f"   Inserted : {count:,}")
print(f"   Dropped  : {total_dropped:,}")
print(f"   Time     : {elapsed:.1f}s")
print(f"   Rate     : {count/elapsed:,.0f} rows/s")

print("\n── Distribution checks ─────────────────────────")
for col, sql in [
    ("is_refund",        "SELECT is_refund, COUNT(*) FROM clean.transactions GROUP BY is_refund"),
    ("merchant_country", "SELECT TOP 10 merchant_country, COUNT(*) as n FROM clean.transactions GROUP BY merchant_country ORDER BY n DESC"),
    ("use_chip",         "SELECT use_chip, COUNT(*) FROM clean.transactions GROUP BY use_chip"),
]:
    print(f"\n  {col}:")
    for r in read_cursor.execute(sql).fetchall():
        print(f"    {str(r[0]):<25} {r[1]:>10,}")


read_cursor.close()
write_cursor.close()
conn_read.close()
conn_write.close()
print("\n✅ DONE")
