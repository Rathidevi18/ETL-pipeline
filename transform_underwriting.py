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

# Load raw underwriting data
df = pd.read_sql('SELECT * FROM "bronze"."underwriting_staging"', bronze_conn, dtype=str)
df.columns = df.columns.str.strip()
print(" Loaded:", df.columns.tolist())

# Type conversions
df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce")
df["src_transaction_date"] = pd.to_datetime(df["src_transaction_date"], errors="coerce")

# Normalize approval status
df["approval_status"] = df["approval_status"].str.title()

# Add received timestamp
df["received_at"] = datetime.now()

# Replace NaT/NaN with None
df = df.astype(object).where(pd.notnull(df), None)

# Drop duplicates using composite key
df.drop_duplicates(subset=["customer_id", "policy_id"], inplace=True)

# Insert into silver table
cursor = silver_conn.cursor()
for _, row in df.iterrows():
    values = tuple(row)
    placeholders = ', '.join(['%s'] * len(values))
    insert_sql = f"""
        INSERT INTO public.silver_underwriting ({', '.join(df.columns)})
        VALUES ({placeholders});
    """
    cursor.execute(insert_sql, values)

silver_conn.commit()
cursor.close()
bronze_conn.close()
silver_conn.close()

print(" underwriting_staging  silver_underwriting transformation complete.")
