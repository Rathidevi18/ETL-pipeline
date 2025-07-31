import psycopg2
import pandas as pd
from datetime import datetime

# Connect to bronze and silver databases
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

# Load bronze table
df = pd.read_sql('SELECT * FROM "bronze"."customer_policy_relation_staging"', bronze_conn, dtype=str)
df.columns = df.columns.str.strip()
print("Columns:", df.columns.tolist())

# Convert dates
df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
df["src_transaction_date"] = pd.to_datetime(df["src_transaction_date"], errors="coerce")

# Normalize status
df["status"] = df["status"].str.title()

# Add timestamp
df["received_at"] = datetime.now()

#  THIS LINE FIXES NaT → None
df = df.astype(object).where(pd.notnull(df), None)

# Drop duplicates by composite key
df.drop_duplicates(subset=["customer_id", "policy_id"], inplace=True)


# Insert into silver
cursor = silver_conn.cursor()
for _, row in df.iterrows():
    values = tuple(row)
    placeholders = ', '.join(['%s'] * len(row))
    insert_sql = f"""
        INSERT INTO public.silver_customer_policy_relation ({', '.join(df.columns)})
        VALUES ({placeholders});
    """
    cursor.execute(insert_sql, values)

silver_conn.commit()
cursor.close()
bronze_conn.close()
silver_conn.close()

print("customer_policy_relation_staging tosilver_customer_policy_relation completed.")
