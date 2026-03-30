import pandas as pd
import numpy as np
from python_api import get_conn

# Connect 

conn = get_conn()
conn.autocommit = False
cursor = conn.cursor()
cursor.fast_executemany = True
print("Connected to database ✅")

# Load from ingestion 

df = pd.read_sql("SELECT * FROM ingestion.cards_data", conn)
print(f"Loaded {len(df)} rows from ingestion.cards_data")
print(f"Duplicates before cleaning: {df.duplicated().sum()}")

#________________
# Transformation
#________________

# Set id and client_id to INT
for col in ['id', 'client_id']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df = df.dropna(subset=['id', 'client_id'])
df['id'] = df['id'].astype(int)
df['client_id'] = df['client_id'].astype(int)


df['card_number'] = (
    df['card_number']
    .astype(str)
    .str.replace(r'\.0$', '', regex=True)
    .str.strip()
)

# Normalize card_brand to Visa, Mastercard, Amex, Discover, or Unknown
def normalize_brand(val):
    if pd.isna(val):
        return 'Unknown'
    v = str(val).strip().lower().replace(' ', '').replace('-', '')
    # Visa 
    if v in ('visa', 'v', 'vis', 'vvisa', 'vissa', 'v!sa', 'visacard'):
        return 'Visa'
    # Mastercard 
    if v in ('mastercard', 'master card', 'mastercard', 'mc'):
        return 'Mastercard'
    # Amex 
    if v in ('amex', 'americanexpress', 'amx', 'amex', 'ame x', 'amex'):
        return 'Amex'
    # Discover
    if v in ('discover', 'dis cover', 'disc'):
        return 'Discover'
    if v == 'unknown':
        return 'Unknown'
    return 'Unknown'

df['card_brand'] = df['card_brand'].apply(normalize_brand)

# Normalize card_type to Credit, Debit, Prepaid Debit, or Unknown 
def normalize_card_type(val):
    if pd.isna(val):
        return 'Unknown'
    v = str(val).strip().lower().replace(' ', '').replace('-', '').replace('_', '')
    # Credit
    if v in ('credit', 'cc', 'cr', 'cred', 'cedit', 'crdeit', 'credt',
             'creditcard', 'cardcredit', 'credit card'):
        return 'Credit'
    # Prepaid Debit 
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

# Transform credit_limit: 
def parse_credit_limit(val):
    if pd.isna(val):
        return None
    v = str(val).strip().lower().replace(',', '').replace('$', '')
    # Set invalid values to null
    if v in ('error_value', 'limit_unknown', 'ten thousand', '', 'nan'):
        return None
    # Get rid of the k-notation by multiplying by 1000
    if v.endswith('k'):
        try:
            return round(float(v[:-1]) * 1000, 2)
        except:
            return None
    # Set negatives to null
    try:
        result = float(v)
        return result if result >= 0 else None
    except:
        return None

df['credit_limit'] = df['credit_limit'].apply(parse_credit_limit)

# Normalize expires date
def parse_date_field(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    try:
        return pd.to_datetime(val, format='%b-%y').strftime('%m/%Y')
    except:
        return str(val).strip()

df['expires'] = df['expires'].apply(parse_date_field)

# Normalize acct_open_date
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

# Set year_pin_last_changed to INT
df['year_pin_last_changed'] = pd.to_numeric(
    df['year_pin_last_changed'], errors='coerce'
).astype('Int64')

# Set num_cards_issued to INT
df['num_cards_issued'] = pd.to_numeric(
    df['num_cards_issued'], errors='coerce'
).astype('Int64')

# Normalize has_chip and card_on_dark_web to Yes or No 
for col in ['has_chip', 'card_on_dark_web']:
    df[col] = df[col].str.strip().str.title()

# ISSUER BANK

# normalisation
# Normalize both columns
df['issuer_bank_name'] = (
    df['issuer_bank_name']
    .astype(str)
    .str.strip()
    .str.upper()
)

df['issuer_bank_state'] = (
    df['issuer_bank_state']
    .astype(str)
    .str.strip()
    .str.upper()
)

# Turn garbage into NULL
df.loc[df['issuer_bank_name'].isin(['NO', 'NONE', 'NAN', '']), 'issuer_bank_name'] = None
df.loc[df['issuer_bank_state'].isin(['NO', 'NONE', 'NAN', '']), 'issuer_bank_state'] = None


#  Fix swapped values
bank_keywords = ['BANK', 'BK', 'CHASE', 'CITI', 'CAPITAL', 'DISCOVER', 'TRUIST', 'ALLY', 'PNC']

mask = df['issuer_bank_state'].str.contains('|'.join(bank_keywords), na=False)

df.loc[mask, 'issuer_bank_name'] = df.loc[mask, 'issuer_bank_state']
df.loc[mask, 'issuer_bank_state'] = None


# --- Normalize bank names ---
bank_map = {
    'PNC BK': 'PNC BANK',
    'PNC': 'PNC BANK',

    'U.S. BK': 'U.S. BANK',
    'US BK': 'U.S. BANK',

    'DISCOVER BK': 'DISCOVER BANK',
    'DISCOVER': 'DISCOVER BANK',

    'CHASE BK': 'CHASE BANK',
    'CHASE': 'CHASE BANK',

    'BK OF AMERICA': 'BANK OF AMERICA',

    'JP MORGAN CHASE': 'JPMORGAN CHASE',
    'JP MORGAN': 'JPMORGAN CHASE',

    'ALLY BK': 'ALLY BANK',
    'ALLY': 'ALLY BANK',

    'CITI': 'CITIBANK'
}

df['issuer_bank_name'] = df['issuer_bank_name'].replace(bank_map)


# Compute state from bank 
bank_to_state = {
    'PNC BANK': 'PA',
    'JPMORGAN CHASE': 'NY',
    'CHASE BANK' : 'NY',
    'CITIBANK': 'NY',
    'BANK OF AMERICA': 'NC',
    'WELLS FARGO': 'CA',
    'U.S. BANK': 'MN',
    'CAPITAL ONE': 'VA',
    'DISCOVER BANK': 'IL',
    'ALLY BANK': 'MI',
    'TRUIST': 'NC'
}

df['issuer_bank_state'] = df['issuer_bank_name'].map(bank_to_state)

# Normalize issuer_bank_type to National, Online, or Regional
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

# Normalize issuer_risk_rating to Low, Medium, High, or Unknown
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

# Drop full row dupes first, then keep first on id 
before = len(df)
df = df.drop_duplicates()
df = df.drop_duplicates(subset='id', keep='first')
df = df.reset_index(drop=True)
print(f"Removed {before - len(df)} duplicates")
print(f"Cleaned rows: {len(df)}")

print("\n── Max string lengths ───────────────────")
for col in df.select_dtypes(include='object').columns:
    mx = df[col].dropna().astype(str).str.len().max()
    print(f"  {col:<25} {mx}")

#_____________
# Clean table
#_____________

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


cols = ['id','client_id','card_brand','card_type','card_number',
        'expires','cvv','has_chip','num_cards_issued','credit_limit',
        'acct_open_date','year_pin_last_changed','card_on_dark_web',
        'issuer_bank_name','issuer_bank_state','issuer_bank_type','issuer_risk_rating']

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
