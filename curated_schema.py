from python_api import get_conn
from config import DATA_FOLDER, DB_TYPE

# ==============================================================
# CONNECTION
# ==============================================================

conn = get_conn()
conn.autocommit = True
cursor = conn.cursor()

print("✅ Connected to database")

# ==============================================================
# CREATE SCHEMA
# ==============================================================

cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'curated')
        EXEC('CREATE SCHEMA curated')
""")
print("✅ Schema 'curated' ready")

# ==============================================================
# TABLES
# ==============================================================

# ----------------------------
# dim_cards
# ----------------------------
cursor.execute("""
    IF OBJECT_ID('curated.dim_cards', 'U') IS NOT NULL
        DROP TABLE curated.dim_cards
""")
cursor.execute("""
    CREATE TABLE curated.dim_cards (
        id INT PRIMARY KEY NOT NULL,
        client_id INT,
        card_brand VARCHAR(20),
        card_type VARCHAR(20),
        card_number VARCHAR(20),
        expires VARCHAR(10),
        cvv VARCHAR(5),
        has_chip VARCHAR(5),
        credit_limit NUMERIC(18,2),
        acct_open_date VARCHAR(10),
        year_pin_last_changed INT,
        card_on_dark_web VARCHAR(5),
        issuer_bank_name VARCHAR(100),
        issuer_bank_state VARCHAR(5),
        issuer_bank_type VARCHAR(20),
        issuer_risk_rating VARCHAR(20)
    )
""")
print("✅ curated.dim_cards created")

# ----------------------------
# dim_merchant
# ----------------------------
cursor.execute("""
    IF OBJECT_ID('curated.dim_merchant', 'U') IS NOT NULL
        DROP TABLE curated.dim_merchant
""")
cursor.execute("""
    CREATE TABLE curated.dim_merchant (
        id INT PRIMARY KEY NOT NULL,
        description VARCHAR(255),
        county NVARCHAR(100),
        state VARCHAR(50),
        country NVARCHAR(100),
        zip NVARCHAR(20)
    )
""")
print("✅ curated.dim_merchant created")

# ----------------------------
# dim_users
# ----------------------------
cursor.execute("""
    CREATE TABLE curated.dim_users (
        user_key INT PRIMARY KEY NOT NULL,
        id INT NOT NULL,
        retirement_age INT,
        birth_year INT,
        birth_month INT,
        gender VARCHAR(20),
        address VARCHAR(255),
        latitude FLOAT,
        longitude FLOAT,
        per_capita_income NUMERIC(18,2),
        yearly_income NUMERIC(18,2),
        total_debt NUMERIC(18,2),
        credit_score INT,
        num_credit_cards INT,
        employment_status VARCHAR(100),
        education_level VARCHAR(100)
    )
""")
print("✅ curated.dim_users created")

# ----------------------------
# facts_table
# ----------------------------
cursor.execute("""
    IF OBJECT_ID('curated.facts_table', 'U') IS NOT NULL
        DROP TABLE curated.facts_table
""")
cursor.execute("""
    CREATE TABLE curated.facts_table (
        transaction_key  INT PRIMARY KEY NOT NULL,
        id               INT NOT NULL,
        date             DATE,
        client_id        INT,
        card_id          INT,
        merchant_id      INT,
        mcc_id           INT,
        amount           DECIMAL(18,2),
        is_refund        BIT NOT NULL,
        use_chip         VARCHAR(50),
    )
""")
print("✅ curated.facts_table created")

# ==============================================================
# DONE
# ==============================================================
cursor.close()
conn.close()
print("\n🎉 Curated schema fully created")