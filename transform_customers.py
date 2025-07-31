import psycopg2
import pandas as pd
from datetime import datetime

# DB connections
bronze_conn = psycopg2.connect(
    dbname="bronze_layer_db",
    user="postgres",
    password="rathi",
    host="localhost",
    port="5432"
)
silver_conn = psycopg2.connect(
    dbname="silver_layer_db",
    user="postgres",
    password="rathi",
    host="localhost",
    port="5432"
)

# Load raw customer data
df = pd.read_sql('SELECT * FROM "bronze"."customer_staging"', bronze_conn, dtype=str)
df.columns = df.columns.str.strip()
print("Loaded:", df.columns.tolist())

# Convert types
df["dob"] = pd.to_datetime(df["dob"], errors="coerce")
df["src_transaction_date"] = pd.to_datetime(df["src_transaction_date"], errors="coerce")
df["age"] = pd.to_numeric(df["age"], errors="coerce")
df["credit_score"] = pd.to_numeric(df["credit_score"], errors="coerce")

# Normalize fields
df["claims_history"] = df["claims_history"].str.title()
df["risk_profile"] = df["risk_profile"].str.title()

# Mask DL number
df["dl_number"] = df["dl_number"].str.replace(r".", "X", regex=True)

# Mask Aadhaar (keep last 4 digits)
df["aadhaar_number"] = df["aadhaar_number"].str.replace(r"\d{8}(?=\d{4})", "XXXXXXXX", regex=True)

# Add received_at
df["received_at"] = datetime.now()

# Replace NaT/NaN with None
df = df.astype(object).where(pd.notnull(df), None)

# Drop duplicate customers
df.drop_duplicates(subset=["customer_id"], inplace=True)

# Insert into silver table
cursor = silver_conn.cursor()
for _, row in df.iterrows():
    values = tuple(row)
    placeholders = ', '.join(['%s'] * len(values))
    insert_sql = f"""
        INSERT INTO public.silver_customers ({', '.join(df.columns)})
        VALUES ({placeholders});
    """
    cursor.execute(insert_sql, values)

silver_conn.commit()
cursor.close()
bronze_conn.close()
silver_conn.close()

print("customer_staging ➜ silver_customers transformation complete.")
