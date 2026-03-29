import pandas as pd
import numpy as np
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
df = pd.read_sql("SELECT * FROM ingestion.cards_data", conn)
print(f"Loaded {len(df)} rows from ingestion.cards_data")
print(f"Duplicates before cleaning: {df.duplicated().sum()}")

# ─────────────────────────────────────────────
# STEP 3 — TRANSFORMATION LOGIC
# ─────────────────────────────────────────────

# --- id, client_id: cast to int ---
for col in ['id', 'client_id']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df = df.dropna(subset=['id', 'client_id'])
df['id'] = df['id'].astype(int)
df['client_id'] = df['client_id'].astype(int)

# --- card_number: strip floating .0 (pandas reads as float) ---
df['card_number'] = (
    df['card_number']
    .astype(str)
    .str.replace(r'\.0$', '', regex=True)
    .str.strip()
)

# --- card_brand: normalize to Visa / Mastercard / Amex / Discover / Unknown ---
def normalize_brand(val):
    if pd.isna(val):
        return 'Unknown'
    v = str(val).strip().lower().replace(' ', '').replace('-', '')
    # Visa variants: visa, v, vis, vvisa, vissa, v!sa (typos)
    if v in ('visa', 'v', 'vis', 'vvisa', 'vissa', 'v!sa', 'visacard'):
        return 'Visa'
    # Mastercard variants
    if v in ('mastercard', 'master card', 'mastercard', 'mc'):
        return 'Mastercard'
    # Amex variants
    if v in ('amex', 'americanexpress', 'amx', 'amex', 'ame x', 'amex'):
        return 'Amex'
    # Discover variants
    if v in ('discover', 'dis cover', 'disc'):
        return 'Discover'
    if v == 'unknown':
        return 'Unknown'
    return 'Unknown'

df['card_brand'] = df['card_brand'].apply(normalize_brand)

# --- card_type: normalize to Credit / Debit / Prepaid Debit / Unknown ---
def normalize_card_type(val):
    if pd.isna(val):
        return 'Unknown'
    v = str(val).strip().lower().replace(' ', '').replace('-', '').replace('_', '')
    # Credit
    if v in ('credit', 'cc', 'cr', 'cred', 'cedit', 'crdeit', 'credt',
             'creditcard', 'cardcredit', 'credit card'):
        return 'Credit'
    # Prepaid Debit (check before plain debit — more specific)
    if any(x in v for x in ('prepaid', 'prepayed', 'prepiad', 'ppd', 'pp',
                              'dbpp', 'dpp', 'prepaiddebit')):
        return 'Prepaid Debit'
    # Debit
    if v in ('debit', 'd', 'db', 'deb', 'debiit', 'debti', 'deibt',
             'bankdebit', 'debitcard', 'debit card'):
        return 'Debit'
    if v == 'unknown':
        return 'Unknown'
    return 'Unknown'

df['card_type'] = df['card_type'].apply(normalize_card_type)

# --- credit_limit: handle $, .00, k notation, text junk, negatives ---
def parse_credit_limit(val):
    if pd.isna(val):
        return None
    v = str(val).strip().lower().replace(',', '').replace('$', '')
    # Text junk → null
    if v in ('error_value', 'limit_unknown', 'ten thousand', '', 'nan'):
        return None
    # k notation: 5.5k → 5500
    if v.endswith('k'):
        try:
            return round(float(v[:-1]) * 1000, 2)
        except:
            return None
    # Negatives → null (credit limit can't be negative)
    try:
        result = float(v)
        return result if result >= 0 else None
    except:
        return None

df['credit_limit'] = df['credit_limit'].apply(parse_credit_limit)

# --- expires: normalize Mon-YY → MM/YYYY ---
def parse_date_field(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    try:
        return pd.to_datetime(val, format='%b-%y').strftime('%m/%Y')
    except:
        return str(val).strip()

df['expires'] = df['expires'].apply(parse_date_field)

# acct_open_date has two formats: 'Sep-02' and 'Feb 01 1996'
def parse_acct_date(val):
    if pd.isna(val) or str(val).strip() in ('', 'not available'):
        return None
    v = str(val).strip()
    for fmt in ('%b-%y', '%b %d %Y'):
        try:
            return pd.to_datetime(v, format=fmt).strftime('%m/%Y')
        except:
            continue
    return None

df['acct_open_date'] = df['acct_open_date'].apply(parse_acct_date)

# --- year_pin_last_changed: cast to int ---
df['year_pin_last_changed'] = pd.to_numeric(
    df['year_pin_last_changed'], errors='coerce'
).astype('Int64')

# --- num_cards_issued: cast to int ---
df['num_cards_issued'] = pd.to_numeric(
    df['num_cards_issued'], errors='coerce'
).astype('Int64')

# --- has_chip / card_on_dark_web: normalize to Yes / No ---
for col in ['has_chip', 'card_on_dark_web']:
    df[col] = df[col].str.strip().str.title()

# --- issuer_bank_name: strip spaces ---
df['issuer_bank_name'] = df['issuer_bank_name'].str.strip().fillna('Unknown')

# --- issuer_bank_state: normalize full names to 2-letter abbreviations ---
state_map = {
    'ILLINOIS': 'IL', 'VIRGINIA': 'VA', 'NORTH CAROLINA': 'NC',
    'MICHIGAN': 'MI', 'NEW YORK': 'NY', 'MINNESOTA': 'MN',
    'CALIFORNIA': 'CA', 'PENNSYLVANIA': 'PA'
}
df['issuer_bank_state'] = (
    df['issuer_bank_state']
    .str.strip()
    .str.upper()
    .replace(state_map)
    .fillna('Unknown')
)

# --- issuer_bank_type: normalize to National / Online / Regional ---
def normalize_bank_type(val):
    if pd.isna(val):
        return 'Unknown'
    v = str(val).strip().lower()
    if 'national' in v:
        return 'National'
    if 'online' in v:
        return 'Online'
    if 'regional' in v:
        return 'Regional'
    return 'Unknown'

df['issuer_bank_type'] = df['issuer_bank_type'].apply(normalize_bank_type)

# --- issuer_risk_rating: normalize to Low / Medium / High / Unknown ---
def normalize_risk(val):
    if pd.isna(val):
        return 'Unknown'
    v = str(val).strip().lower()
    if v in ('low', 'low risk'):
        return 'Low'
    if v in ('medium', 'med', 'medium risk'):
        return 'Medium'
    if v in ('high', 'high risk'):
        return 'High'
    return 'Unknown'

df['issuer_risk_rating'] = df['issuer_risk_rating'].apply(normalize_risk)

# --- deduplicate: drop full row dupes first, then keep first on id ---
before = len(df)
df = df.drop_duplicates()
df = df.drop_duplicates(subset='id', keep='first')
df = df.reset_index(drop=True)
print(f"Removed {before - len(df)} duplicates")
print(f"Cleaned rows: {len(df)}")

# --- diagnostic: max string length per column ---
print("\n── Max string lengths ───────────────────")
for col in df.select_dtypes(include='object').columns:
    mx = df[col].dropna().astype(str).str.len().max()
    print(f"  {col:<25} {mx}")

# ─────────────────────────────────────────────
# STEP 4 — CREATE CLEAN TABLE
# ─────────────────────────────────────────────
cursor.execute("""
    IF OBJECT_ID('clean.cards', 'U') IS NOT NULL
        DROP TABLE clean.cards
""")
cursor.execute("""
    CREATE TABLE clean.cards (
        id                    INT            NOT NULL,
        client_id             INT            NOT NULL,
        card_brand            NVARCHAR(250),
        card_type             NVARCHAR(250),
        card_number           NVARCHAR(250),
        expires               NVARCHAR(100),
        cvv                   NVARCHAR(255),
        has_chip              NVARCHAR(255),
        num_cards_issued      INT,
        credit_limit          DECIMAL(18,2),
        acct_open_date        NVARCHAR(10),
        year_pin_last_changed INT,
        card_on_dark_web      NVARCHAR(255),
        issuer_bank_name      NVARCHAR(255),
        issuer_bank_state     NVARCHAR(255),
        issuer_bank_type      NVARCHAR(200),
        issuer_risk_rating    NVARCHAR(200)
    )
""")
print("\nclean.cards created ✅")

# ─────────────────────────────────────────────
# STEP 5 — INSERT CLEAN DATA
# ─────────────────────────────────────────────

cols = ['id','client_id','card_brand','card_type','card_number',
        'expires','cvv','has_chip','num_cards_issued','credit_limit',
        'acct_open_date','year_pin_last_changed','card_on_dark_web',
        'issuer_bank_name','issuer_bank_state','issuer_bank_type','issuer_risk_rating']

# Convert all numpy/pandas types to plain Python so pyodbc can handle them
def to_python(val):
    if val is None or val is pd.NA:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    return val

data = [
    tuple(to_python(v) for v in row)
    for row in df[cols].itertuples(index=False, name=None)
]
cursor.executemany("""
    INSERT INTO clean.cards (
        id, client_id, card_brand, card_type, card_number,
        expires, cvv, has_chip, num_cards_issued, credit_limit,
        acct_open_date, year_pin_last_changed, card_on_dark_web,
        issuer_bank_name, issuer_bank_state, issuer_bank_type, issuer_risk_rating
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
""", data)
conn.commit()
print(f"Inserted {len(data)} rows into clean.cards ✅")

# ─────────────────────────────────────────────
# STEP 6 — VERIFY
# ─────────────────────────────────────────────
count = cursor.execute("SELECT COUNT(*) FROM clean.cards").fetchone()[0]
print(f"Final row count in clean.cards: {count}")

print("\n── Value distribution checks ─────────────")
for col in ['card_brand', 'card_type', 'issuer_bank_type', 'issuer_risk_rating']:
    rows = cursor.execute(
        f"SELECT {col}, COUNT(*) as n FROM clean.cards GROUP BY {col} ORDER BY n DESC"
    ).fetchall()
    print(f"\n  {col}:")
    for r in rows:
        print(f"    {str(r[0]):<20} {r[1]:>6}")

cursor.close()
conn.close()
