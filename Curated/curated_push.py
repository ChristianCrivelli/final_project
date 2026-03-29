from python_api import get_conn
import pandas as pd
import numpy as np

# ==============================================================
# CONNECTION
# ==============================================================

conn = get_conn()
conn.autocommit = True
cursor = conn.cursor()
cursor.fast_executemany = True

print("✅ Connected to database")


# ==============================================================
# LOAD DATA
# ==============================================================

df_cards = pd.read_sql("SELECT * FROM clean.cards", conn)
df_transactions = pd.read_sql("SELECT * FROM clean.transactions", conn)
df_mcc = pd.read_sql("SELECT * FROM clean.mcc", conn)
df_users = pd.read_sql("SELECT * FROM clean.users", conn)

# merge transactions and mcc
df_transactions['mcc'] = df_transactions['mcc'].astype(str).str.strip()
df_mcc['code'] = df_mcc['code'].astype(str).str.strip()

df_transactions = df_transactions.merge(
    df_mcc, 
    left_on='mcc', 
    right_on='code', 
    how='left'
)

print("Succesfully Joined Merchant and Transaction data!") #sanity check

# ==============================================================
# DIMENSIONS & FACTS
# ==============================================================

# --- dim_cards ---
dim_cards = pd.DataFrame({
    "id":                   df_cards["id"],
    "client_id":            df_cards["client_id"],
    "card_brand":           df_cards["card_brand"],
    "card_type":            df_cards["card_type"],
    "card_number":          df_cards["card_number"],
    "expires":              df_cards["expires"],
    "cvv":                  df_cards["cvv"],
    "has_chip":             df_cards["has_chip"],
    "credit_limit":         df_cards["credit_limit"],
    "acct_open_date":       df_cards["acct_open_date"],
    "year_pin_last_changed":df_cards["year_pin_last_changed"],
    "card_on_dark_web":     df_cards["card_on_dark_web"],
    "issuer_bank_name":     df_cards["issuer_bank_name"],
    "issuer_bank_state":    df_cards["issuer_bank_state"],
    "issuer_bank_type":     df_cards["issuer_bank_type"],
    "issuer_risk_rating":   df_cards["issuer_risk_rating"]
}).sort_values("id").reset_index(drop=True)

dim_cards = dim_cards.drop_duplicates(subset=['id']).sort_values("id").reset_index(drop=True)


# --- dim_merchant ---
dim_merchant = pd.DataFrame({
    "id": df_transactions["merchant_id"],
    "description": df_transactions["description"],
    "county": df_transactions["merchant_county"],
    "zip": df_transactions["zip"],
    "state": df_transactions["merchant_state"],
    "country": df_transactions["merchant_country"]
})

dim_merchant = dim_merchant.drop_duplicates(subset=['id']).sort_values("id").reset_index(drop=True)

# --- dim_users ---
dim_users = pd.DataFrame({
    "id": df_users["id"],
    "retirement_age": df_users["retirement_age"],
    "birth_year": df_users["birth_year"],
    "birth_month": df_users["birth_month"],
    "gender": df_users["gender"],
    "address": df_users["address"],
    "latitude": df_users["latitude"],
    "longitude": df_users["longitude"],
    "per_capita_income": df_users["per_capita_income"],
    "yearly_income": df_users["yearly_income"],
    "total_debt": df_users["total_debt"],
    "credit_score": df_users["credit_score"],
    "num_credit_cards": df_users["num_credit_cards"],
    "employment_status": df_users["employment_status"],
    "education_level": df_users["education_level"]
}).sort_values("id").reset_index(drop=True)

dim_users = dim_users.drop_duplicates(subset=['id']).sort_values("id").reset_index(drop=True)
dim_users.insert(0, 'user_key', np.arange(1, len(dim_users) + 1))

# --- facts_table ---
facts_table = pd.DataFrame({
    "id":           df_transactions["id"],
    "date":         df_transactions["date"],
    "client_id":    df_transactions["client_id"],
    "card_id":      df_transactions["card_id"],
    "merchant_id":  df_transactions["merchant_id"],
    "mcc_id":       df_transactions["mcc"],
    "amount":       df_transactions["amount"],
    "is_refund":    df_transactions["is_refund"],
    "use_chip":     df_transactions["use_chip"]
}).sort_values("id").reset_index(drop=True)

facts_table.insert(0, 'transaction_key', np.arange(1, len(facts_table) + 1))



# ==============================================================
# HELPER — SAFE CONVERSION
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
# CONVERT DATA BEFORE INSERT
# ==============================================================

data_cards = [
    tuple(to_python(v) for v in row)
    for row in dim_cards.itertuples(index=False, name=None)
]

data_merchant = [
    tuple(to_python(v) for v in row)
    for row in dim_merchant.itertuples(index=False, name=None)
]

data_users = [
    tuple(to_python(v) for v in row)
    for row in dim_users.itertuples(index=False, name=None)
]

data_facts = [
    tuple(to_python(v) for v in row)
    for row in facts_table.itertuples(index=False, name=None)
]

# ==============================================================
# PUSH DATA
# ==============================================================

# --- dim_cards ---
cursor.executemany(
    """
    INSERT INTO curated.dim_cards (
        id, client_id, card_brand, card_type, card_number, 
        expires, cvv, has_chip, credit_limit, acct_open_date, 
        year_pin_last_changed, card_on_dark_web, issuer_bank_name, 
        issuer_bank_state, issuer_bank_type, issuer_risk_rating
    ) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    data_cards
)

count_cards = cursor.execute("SELECT COUNT(*) FROM curated.dim_cards").fetchone()[0]
print(f"✅ dim_cards: {count_cards:,} rows")


# --- dim_merchant ---
cursor.executemany(
    """
    INSERT INTO curated.dim_merchant (id, description, county, zip, state, country)
    VALUES (?, ?, ?, ?, ?, ?)
    """,
    data_merchant
)

count_merchant = cursor.execute("SELECT COUNT(*) FROM curated.dim_merchant").fetchone()[0]
print(f"✅ dim_merchant: {count_merchant:,} rows")

# --- dim_users
cursor.executemany("""
    INSERT INTO curated.dim_users (
        user_key, id, retirement_age, birth_year, birth_month,
        gender, address, latitude, longitude, per_capita_income,
        yearly_income, total_debt, credit_score, num_credit_cards,
        employment_status, education_level
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", data_users)

count_users = cursor.execute("SELECT COUNT(*) FROM curated.dim_users").fetchone()[0]
print(f"✅ dim_users: {count_users:,} rows")

# --- facts_table ---
cursor.executemany(
    """
    INSERT INTO curated.facts_table (
        transaction_key, id, date, client_id, card_id, merchant_id, 
        mcc_id, amount, is_refund, use_chip
    ) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    data_facts
)

count_facts = cursor.execute("SELECT COUNT(*) FROM curated.facts_table").fetchone()[0]
print(f"✅ facts_table: {count_facts:,} rows")


# ==============================================================
# DONE
# ==============================================================

cursor.close()
conn.close()

print("\n🎉 Curated layer successfully loaded!")
