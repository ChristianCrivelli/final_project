import pyodbc
import psycopg
from config import DB_TYPE, SERVER_NAME, DRIVER, HOST, PORT, USER, PASSWORD, DATABASE
 
 
def get_conn(database=None):
    """Return an open connection using the DB_TYPE set in config.py."""
    db_name = database if database else DATABASE
 
    if DB_TYPE == "sqlserver":
        return pyodbc.connect(
            f"DRIVER={{{DRIVER}}};"
            f"SERVER={SERVER_NAME};"
            f"DATABASE={db_name};"
            "Trusted_Connection=yes;"
        )
 
    elif DB_TYPE == "postgres":
        # Postgres uses 'postgres' as the default admin database (not 'master')
        return psycopg.connect(
            host=HOST,
            port=PORT,
            dbname=db_name,
            user=USER,
            password=PASSWORD
        )
 
    else:
        raise ValueError(f"Unsupported DB_TYPE: '{DB_TYPE}'. Use 'sqlserver' or 'postgres'.")