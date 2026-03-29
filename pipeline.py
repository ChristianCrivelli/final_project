import subprocess
import sys

# ==============================================================
# Bronze Layer
# ==============================================================
print("Creating the Database...")
subprocess.check_call([sys.executable, "setup_db.py"])

print("Initiatiating Ingestion Layer...")
subprocess.check_call([sys.executable, "ingestion_schema.py"])
    
print("Pushing Data Into The Ingestion Layer...")
subprocess.check_call([sys.executable, "ingestion_push.py"])
    
print("Ingestion Layer Complete! ✅")

# ==============================================================
# Silver Layer
# ==============================================================
print("Initiatiating Transformation Layer...")
subprocess.check_call([sys.executable, "transformation_schema.py"])
    
print("Cleaning Transactions Data...")
subprocess.check_call([sys.executable, "transformation_transactions.py"])

print("Cleaning Cards Data...")
subprocess.check_call([sys.executable, "transformation_cards.py"])

print("Cleaning MCC Data...")
subprocess.check_call([sys.executable, "transformation_mcc.py"])

print("Cleaning Users Data...")
subprocess.check_call([sys.executable, "transformation_users.py"])

print("Transformation Layer Complete! ✅")

# ==============================================================
# Gold Layer
# ==============================================================
print("Initiatiating Curated Layer...")
subprocess.check_call([sys.executable, "curated_schema.py"])

print("Curating Database...")
subprocess.check_call([sys.executable, "curated_push.py"])

print("Curated Layer Complete! ✅")

