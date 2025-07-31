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

# Load raw policy data
df = pd.read_sql('SELECT * FROM "bronze"."policies_staging"', bronze_conn, dtype=str)
df.columns = df.columns.str.strip()
print("Loaded:", df.columns.tolist())

# Type conversions
df["premium"] = pd.to_numeric(df["premium"], errors="coerce")
df["coverage_amount"] = pd.to_numeric(df["coverage_amount"], errors="coerce")
df["src_transaction_date"] = pd.to_datetime(df["src_transaction_date"], errors="coerce")

# Normalize status
df["status"] = df["status"].str.title()

# Add timestamp
df["received_at"] = datetime.now()

# Replace NaT/NaN with None
df = df.astype(object).where(pd.notnull(df), None)

# Drop duplicate policies
df.drop_duplicates(subset=["policy_id"], inplace=True)

# Insert into silver table
cursor = silver_conn.cursor()
for _, row in df.iterrows():
    values = tuple(row)
    placeholders = ', '.join(['%s'] * len(values))
    insert_sql = f"""
        INSERT INTO public.silver_policies ({', '.join(df.columns)})
        VALUES ({placeholders});
    """
    cursor.execute(insert_sql, values)

silver_conn.commit()
cursor.close()
bronze_conn.close()
silver_conn.close()

print("policies_staging to  silver_policies transformation complete.")
