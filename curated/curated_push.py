from python_api import get_conn
import pandas as pd
import numpy as np
import time

# ==============================================================
# CONNECTION — two connections for read/write on large data
# ==============================================================
conn_read  = get_conn()
conn_write = get_conn()
conn_write.autocommit = False

read_cursor  = conn_read.cursor()
write_cursor = conn_write.cursor()
write_cursor.fast_executemany = True

print("✅ Connected to database")

# Cleaning if an instance exit before
write_cursor.execute("TRUNCATE TABLE curated.dim_cards")
write_cursor.execute("TRUNCATE TABLE curated.dim_merchant")
write_cursor.execute("TRUNCATE TABLE curated.dim_users")
write_cursor.execute("TRUNCATE TABLE curated.facts_table")
conn_write.commit()

# ==============================================================
# HELPER — safe type conversion for pyodbc
# ==============================================================
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
    except:
        pass
    return val

# ==============================================================
# LOAD CLEAN TABLES
# ==============================================================
print("\n📥 Loading clean tables...")
start = time.time()

df_cards        = pd.read_sql("SELECT * FROM clean.cards",        conn_read)
df_users        = pd.read_sql("SELECT * FROM clean.users",        conn_read)
df_mcc          = pd.read_sql("SELECT * FROM clean.mcc",          conn_read)

print(f"  cards:  {len(df_cards):,} rows")
print(f"  users:  {len(df_users):,} rows")
print(f"  mcc:    {len(df_mcc):,} rows")
print("  transactions: loading in chunks (large file)...")

# transactions too large to load all at once — build dims from chunks
# We collect unique merchants first, then stream facts
merchant_rows  = []
facts_rows_all = []
transaction_key = 1

for chunk in pd.read_sql(
    "SELECT * FROM clean.transactions",
    conn_read,
    chunksize=200_000
):
    # collect unique merchant combinations from this chunk
    merchant_cols = ['merchant_id','merchant_county','merchant_state','merchant_country','zip','mcc']
    merchant_rows.append(chunk[merchant_cols].drop_duplicates())

    # collect fact rows
    facts_rows_all.append(chunk[['id','date','client_id','card_id',
                                  'merchant_id','mcc','amount','is_refund','use_chip']])

    print(f"    ➜ chunk loaded, {len(chunk):,} rows")

df_transactions_merchants = pd.concat(merchant_rows,  ignore_index=True)
df_transactions_facts     = pd.concat(facts_rows_all, ignore_index=True)

print(f"  transactions facts: {len(df_transactions_facts):,} total rows")

# ==============================================================
# BUILD DIMENSIONS
# ==============================================================

# --- dim_cards ---
# num_cards_issued dropped from insert (not in schema INSERT list)
dim_cards = (
    df_cards[[
        'id','client_id','card_brand','card_type','card_number',
        'expires','cvv','has_chip','credit_limit',
        'acct_open_date','year_pin_last_changed','card_on_dark_web',
        'issuer_bank_name','issuer_bank_state','issuer_bank_type','issuer_risk_rating'
    ]]
    .drop_duplicates(subset=['id'])
    .sort_values('id')
    .reset_index(drop=True)
)

# --- dim_merchant ---
# Merge with mcc to get description, deduplicate on merchant_id
df_mcc['code'] = df_mcc['code'].astype(str).str.strip()
df_transactions_merchants['mcc'] = df_transactions_merchants['mcc'].astype(str).str.strip()

# Step 1: rename first, then select — avoids KeyError if rename hasn't applied yet
_m = df_transactions_merchants.merge(
    df_mcc[['code','description']], left_on='mcc', right_on='code', how='left'
)
_m = _m.rename(columns={
    'merchant_id':      'id',
    'merchant_county':    'county',
    'merchant_state':   'state',
    'merchant_country': 'country',
})
# drop rows with no merchant_id before dedup/sort
_m = _m.dropna(subset=['id'])
_m['id'] = _m['id'].astype(int)
dim_merchant = (
    _m[['id','description','county','zip','state','country']]
    .drop_duplicates(subset=['id'])
    .sort_values('id')
    .reset_index(drop=True)
)

# --- dim_users ---
dim_users = (
    df_users[[
        'id','retirement_age','birth_year','birth_month','gender',
        'address','latitude','longitude','per_capita_income',
        'yearly_income','total_debt','credit_score',
        'num_credit_cards','employment_status','education_level'
    ]]
    .drop_duplicates(subset=['id'])
    .sort_values('id')
    .reset_index(drop=True)
)
dim_users.insert(0, 'user_key', np.arange(1, len(dim_users) + 1))

# --- facts_table ---
facts_table = (
    df_transactions_facts
    .rename(columns={'mcc': 'mcc_id'})
    .sort_values('id')
    .reset_index(drop=True)
)
facts_table.insert(0, 'transaction_key', np.arange(1, len(facts_table) + 1))

print(f"\n📊 Dimensions built:")
print(f"  dim_cards:    {len(dim_cards):,}")
print(f"  dim_merchant: {len(dim_merchant):,}")
print(f"  dim_users:    {len(dim_users):,}")
print(f"  facts_table:  {len(facts_table):,}")

# ==============================================================
# INSERT — dim_cards
# ==============================================================
print("\n📤 Inserting dim_cards...")
data = [tuple(to_python(v) for v in row)
        for row in dim_cards.itertuples(index=False, name=None)]

write_cursor.executemany("""
    INSERT INTO curated.dim_cards (
        id, client_id, card_brand, card_type, card_number,
        expires, cvv, has_chip, credit_limit,
        acct_open_date, year_pin_last_changed, card_on_dark_web,
        issuer_bank_name, issuer_bank_state, issuer_bank_type, issuer_risk_rating
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
""", data)
conn_write.commit()
count = write_cursor.execute("SELECT COUNT(*) FROM curated.dim_cards").fetchone()[0]
print(f"✅ dim_cards: {count:,} rows")

# ==============================================================
# INSERT — dim_merchant
# ==============================================================
print("📤 Inserting dim_merchant...")
data = [tuple(to_python(v) for v in row)
        for row in dim_merchant.itertuples(index=False, name=None)]

write_cursor.executemany("""
    INSERT INTO curated.dim_merchant (id, description, county, zip, state, country)
    VALUES (?,?,?,?,?,?)
""", data)
conn_write.commit()
count = write_cursor.execute("SELECT COUNT(*) FROM curated.dim_merchant").fetchone()[0]
print(f"✅ dim_merchant: {count:,} rows")

# ==============================================================
# INSERT — dim_users
# ==============================================================
print("📤 Inserting dim_users...")
data = [tuple(to_python(v) for v in row)
        for row in dim_users.itertuples(index=False, name=None)]

write_cursor.executemany("""
    INSERT INTO curated.dim_users (
        user_key, id, retirement_age, birth_year, birth_month,
        gender, address, latitude, longitude,
        per_capita_income, yearly_income, total_debt,
        credit_score, num_credit_cards, employment_status, education_level
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
""", data)
conn_write.commit()
count = write_cursor.execute("SELECT COUNT(*) FROM curated.dim_users").fetchone()[0]
print(f"✅ dim_users: {count:,} rows")

# ==============================================================
# INSERT — facts_table (chunked — 13M rows)
# Smaller chunks + reconnect on pipe error to handle SQL Server
# dropping long-running connections
# ==============================================================
print("📤 Inserting facts_table (chunked)...")
CHUNK   = 100_000 
INSERT_SQL = """
    INSERT INTO curated.facts_table (
        transaction_key, id, date, client_id, card_id,
        merchant_id, mcc_id, amount, is_refund, use_chip
    ) VALUES (?,?,?,?,?,?,?,?,?,?)
"""
total = 0

for i in range(0, len(facts_table), CHUNK):
    chunk = facts_table.iloc[i:i+CHUNK]
    data  = [tuple(to_python(v) for v in row)
             for row in chunk.itertuples(index=False, name=None)]

    # retry loop — reconnect if SQL Server drops the pipe
    for attempt in range(3):
        try:
            write_cursor.executemany(INSERT_SQL, data)
            conn_write.commit()
            break
        except Exception as e:
            if attempt < 2 and ("08S01" in str(e) or "pipe" in str(e).lower()):
                print(f"   ⚠️  Connection dropped, reconnecting... (attempt {attempt+1})")
                try:
                    conn_write.close()
                except:
                    pass
                conn_write   = get_conn()
                conn_write.autocommit = False
                write_cursor = conn_write.cursor()
                write_cursor.fast_executemany = True
            else:
                raise

    total += len(data)
    pct = total / len(facts_table) * 100
    print(f"   ➜ {total:>10,} / {len(facts_table):,}  ({pct:.1f}%)")

count = write_cursor.execute("SELECT COUNT(*) FROM curated.facts_table").fetchone()[0]
print(f"✅ facts_table: {count:,} rows")

# ==============================================================
# DONE
# ==============================================================
elapsed = time.time() - start
read_cursor.close()
write_cursor.close()
conn_read.close()
conn_write.close()
print(f"\n🎉 Curated layer fully loaded in {elapsed:.1f}s")