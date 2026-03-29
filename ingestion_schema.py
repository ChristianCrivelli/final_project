from python_api import get_conn
from config import DATABASE

# ==============================================================
# INGESTION DDL — Create schema & all staging tables
# Philosophy: raw ingestion layer — everything stored as NVARCHAR,
# no type enforcement, no constraints. Cleaning happens in clean.py
# ==============================================================

conn = get_conn()
conn.autocommit = True
cursor = conn.cursor()

print("✅ Connected to database")

# ==============================================================
# CREATE SCHEMA
# ==============================================================

cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ingestion')
        EXEC('CREATE SCHEMA ingestion')
""")
print("✅ Schema 'ingestion' ready")

# ==============================================================
# TABLES
# ==============================================================

# ----------------------------
# cards_data
# ----------------------------
cursor.execute("""
    IF OBJECT_ID('ingestion.cards_data', 'U') IS NOT NULL
        DROP TABLE ingestion.cards_data
""")
cursor.execute("""
    CREATE TABLE ingestion.cards_data (
        id                    NVARCHAR(50),
        client_id             NVARCHAR(50),
        card_brand            NVARCHAR(50),
        card_type             NVARCHAR(50),
        card_number           NVARCHAR(100),
        expires               NVARCHAR(50),
        cvv                   NVARCHAR(50),
        has_chip              NVARCHAR(10),
        num_cards_issued      NVARCHAR(10),
        credit_limit          NVARCHAR(50),
        acct_open_date        NVARCHAR(50),
        year_pin_last_changed NVARCHAR(50),
        card_on_dark_web      NVARCHAR(10),
        issuer_bank_name      NVARCHAR(100),
        issuer_bank_state     NVARCHAR(50),
        issuer_bank_type      NVARCHAR(50),
        issuer_risk_rating    NVARCHAR(50)
    )
""")
print("✅ ingestion.cards_data created")

# ----------------------------
# mcc_data
# ----------------------------
cursor.execute("""
    IF OBJECT_ID('ingestion.mcc_data', 'U') IS NOT NULL
        DROP TABLE ingestion.mcc_data
""")
cursor.execute("""
    CREATE TABLE ingestion.mcc_data (
        code        NVARCHAR(50),
        description NVARCHAR(255),
        notes       NVARCHAR(255),
        updated_by  NVARCHAR(100)
    )
""")
print("✅ ingestion.mcc_data created")

# ----------------------------
# transactions_data
# ----------------------------
cursor.execute("""
    IF OBJECT_ID('ingestion.transactions_data', 'U') IS NOT NULL
        DROP TABLE ingestion.transactions_data
""")
cursor.execute("""
    CREATE TABLE ingestion.transactions_data (
        id             NVARCHAR(50),
        date           NVARCHAR(50),
        client_id      NVARCHAR(50),
        card_id        NVARCHAR(50),
        amount         NVARCHAR(50),
        use_chip       NVARCHAR(50),
        merchant_id    NVARCHAR(50),
        merchant_city  NVARCHAR(100),
        merchant_state NVARCHAR(50),
        zip            NVARCHAR(20),
        mcc            NVARCHAR(50),
        errors         NVARCHAR(255)
    )
""")
print("✅ ingestion.transactions_data created")

# ----------------------------
# users_data
# ----------------------------
cursor.execute("""
    IF OBJECT_ID('ingestion.users_data', 'U') IS NOT NULL
        DROP TABLE ingestion.users_data
""")
cursor.execute("""
    CREATE TABLE ingestion.users_data (
        id                NVARCHAR(50),
        current_age       NVARCHAR(50),
        retirement_age    NVARCHAR(50),
        birth_year        NVARCHAR(50),
        birth_month       NVARCHAR(50),
        gender            NVARCHAR(20),
        address           NVARCHAR(255),
        latitude          NVARCHAR(50),
        longitude         NVARCHAR(50),
        per_capita_income NVARCHAR(50),
        yearly_income     NVARCHAR(50),
        total_debt        NVARCHAR(50),
        credit_score      NVARCHAR(50),
        num_credit_cards  NVARCHAR(50),
        employment_status NVARCHAR(100),
        education_level   NVARCHAR(100)
    )
""")
print("✅ ingestion.users_data created")

# ==============================================================
# DONE
# ==============================================================
cursor.close()
conn.close()
print("\n🎉 Ingestion schema fully created")
