# Import 
from python_api import get_conn
from config import DATABASE, DB_TYPE
 

#  SQL SERVER — connect to master, create the data warehouse
if DB_TYPE == "sqlserver": # only for microsoft sql server
    conn = get_conn("master")
    conn.autocommit = True
    cursor = conn.cursor()
 # if exist then we drop it, this is intentional because it is a good way to "hard reset" our server
    cursor.execute(f"""
        IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '{DATABASE}')
        BEGIN
            ALTER DATABASE {DATABASE} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE {DATABASE};
        END
    """)
# Create a database
    cursor.execute(f"""
        CREATE DATABASE {DATABASE}
    """)
# print a nice message
    print(f"Database '{DATABASE}' reset (dropped if existed and  recreated )")
 
    cursor.close()
    conn.close()
 

#  POSTGRES — connect to postgres, create DB
# Same logic but this time it is for postgres users.
elif DB_TYPE == "postgres":
    # Must connect to default 'postgres' db to create a new one
    conn = get_conn("postgres")
    conn.autocommit = True
    cursor = conn.cursor()
 
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DATABASE}'")
    if not cursor.fetchone():
        cursor.execute(f"CREATE DATABASE {DATABASE}")
        print(f"Database '{DATABASE}' created")
    else:
        print(f"Database '{DATABASE}' already exists")
 
    cursor.close()
    conn.close()
 
#  VERIFY — connect to the new DB
conn = get_conn()
print(f"Connected to '{DATABASE}' successfully ✅")
conn.close()
 
