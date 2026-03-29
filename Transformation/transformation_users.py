import pandas as pd
import numpy as np
import re
from python_api import get_conn

# Connect

conn = get_conn()
conn.autocommit = False
cursor = conn.cursor()
cursor.fast_executemany = True

print("Connected to database ✅")

# Load from ingestion

df = pd.read_sql("SELECT * FROM ingestion.users_data", conn)
print(f"Loaded {len(df)} rows from ingestion.users_data")
print(f"Duplicates before cleaning: {df.duplicated().sum()}")

#________________
# Transformation
#________________

# Set id to INT
df['id'] = pd.to_numeric(df['id'], errors='coerce')
df = df.dropna(subset=['id'])
df['id'] = df['id'].astype(int)


for col in ['current_age', 'retirement_age', 'birth_year',
            'birth_month', 'credit_score', 'num_credit_cards']:
    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

for col in ['latitude', 'longitude']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Currency columns: Remove $ and commas, multiply the k-notation by 1000
def parse_currency(val):
    if pd.isna(val):
        return None
    v = str(val).strip().replace('$', '').replace(',', '')
    if 'k' in v.lower():
        try:
            return float(v.lower().replace('k', '')) * 1000
        except:
            return None
    return pd.to_numeric(v, errors='coerce')

for col in ['per_capita_income', 'yearly_income', 'total_debt']:
    df[col] = df[col].apply(parse_currency)

# Trim address
df['address'] = df['address'].str.strip()

# Gender: title case 
df['gender'] = df['gender'].str.strip().str.title()





#  Normalize employment_status
df["employment_status"] = df["employment_status"].astype(str).str.strip()

# remove pure numbers
df.loc[df["employment_status"].str.fullmatch(r"\d+"), "employment_status"] = np.nan

df["employment_status"] = df["employment_status"].str.lower().str.strip()

employment_map = {
    "employed": "Employed",
    "self-employed": "Self-Employed",
    "self employed": "Self-Employed",
    "student": "Student",
    "retired": "Retired",
    "unemployed": "Unemployed",
    "self-employd": "Self-Employed",
    "empl0yed": "Employed",
    "un-employed": "Unemployed",
    "unemployd": "Unemployed",
    "studnt": "Student",
    "retird": "Retired",
    "ret.": "Retired",
}

df["employment_status"] = df["employment_status"].replace(employment_map)

valid_values = {
    "employed",
    "self-employed",
    "student",
    "retired",
    "unemployed"
}

df["employment_status"] = df["employment_status"].apply(
    lambda x: x if str(x).lower() in valid_values else None
)




# Normalize education_level
def extract_education(val):
    if pd.isna(val):
        return None

    v = str(val).strip().lower()

   
    v = re.sub(r"^(employed|student|retired|unemployed|self[- ]employed|\d+)\s*,\s*", "", v)

    parts = [p.strip() for p in v.split(",")]

    # take last meaningful part
    edu = parts[-1] if parts else v

    return edu

df["education_level"] = df["education_level"].apply(extract_education)

df["education_level"] = (
    df["education_level"]
    .astype(str)
    .str.lower()
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
)

education_map = {
    "bachelor degree": "Bachelor",
    "bachelor's degree": "Bachelor",
    "bachelors": "Bachelor",
    "ba/bs": "Bachelor",
    "bachelor": "Bachelor",

    "master degree": "Master",
    "master's degree": "Master",
    "masters": "Master",
    "ms/ma": "Master",
    "master": "Master",

    "high school": "High School",
    "highschool": "High School",
    "hs": "High School",

    "associate degree": "Associate",
    "associate": "Associate",
    "assoc degree": "Associate",

    "doctorate": "Doctorate",
    "doctorate degree": "Doctorate",
}

df["education_level"] = df["education_level"].replace(education_map)

valid = {"Bachelor", "Master", "High School", "Associate", "Doctorate"}

df.loc[~df["education_level"].isin(valid), "education_level"] = "Unknown"

# Deduplicate 
before = len(df)
df = df.drop_duplicates().reset_index(drop=True)
print(f"Removed {before - len(df)} duplicates")
print(f"Cleaned rows: {len(df)}")


print("\n── Max string lengths ───────────────────")
for col in df.select_dtypes(include='object').columns:
    mx = df[col].dropna().astype(str).str.len().max()
    print(f"  {col:<25} {mx}")

# ────────────
# Clean table
# ────────────
cursor.execute("""
    IF OBJECT_ID('clean.users', 'U') IS NOT NULL
        DROP TABLE clean.users
""")
cursor.execute("""
    CREATE TABLE clean.users (
        id                INT            NOT NULL,
        current_age       INT,
        retirement_age    INT,
        birth_year        INT,
        birth_month       INT,
        gender            NVARCHAR(20),
        address           NVARCHAR(255),
        latitude          FLOAT,
        longitude         FLOAT,
        per_capita_income DECIMAL(18,2),
        yearly_income     DECIMAL(18,2),
        total_debt        DECIMAL(18,2),
        credit_score      INT,
        num_credit_cards  INT,
        employment_status NVARCHAR(50),
        education_level   NVARCHAR(50)
    )
""")
print("\nclean.users created ✅")


def to_python(val):
    """Convert numpy/pandas types to plain Python for pyodbc."""
    if val is None or val is pd.NA:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        return float(val)
    if isinstance(val, np.bool_):
        return bool(val)
    return val

cols = ['id', 'current_age', 'retirement_age', 'birth_year', 'birth_month',
        'gender', 'address', 'latitude', 'longitude',
        'per_capita_income', 'yearly_income', 'total_debt',
        'credit_score', 'num_credit_cards', 'employment_status', 'education_level']

data = [
    tuple(to_python(v) for v in row)
    for row in df[cols].itertuples(index=False, name=None)
]

cursor.executemany("""
    INSERT INTO clean.users (
        id, current_age, retirement_age, birth_year, birth_month,
        gender, address, latitude, longitude,
        per_capita_income, yearly_income, total_debt,
        credit_score, num_credit_cards, employment_status, education_level
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
""", data)
conn.commit()
print(f"Inserted {len(data)} rows into clean.users ✅")


count = cursor.execute("SELECT COUNT(*) FROM clean.users").fetchone()[0]
print(f"Final row count in clean.users: {count}")

print("\n── Distribution checks ──────────────────")
for col in ['employment_status', 'education_level', 'gender']:
    print(f"\n  {col}:")
    rows = cursor.execute(
        f"SELECT {col}, COUNT(*) as n FROM clean.users GROUP BY {col} ORDER BY n DESC"
    ).fetchall()
    for r in rows:
        print(f"    {str(r[0]):<25} {r[1]:>6,}")

cursor.close()
conn.close()
