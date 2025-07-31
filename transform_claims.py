import psycopg2
import pandas as pd
from datetime import datetime

# PostgreSQL connections
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

# Load from bronze schema (case-sensitive with double quotes)
df = pd.read_sql('SELECT * FROM "bronze"."claims_staging"', bronze_conn, dtype=str)
print("\n🔍 Loaded columns:", df.columns.tolist())

# Strip column names
df.columns = df.columns.str.strip()

# Type conversions
df["claim_amount"] = pd.to_numeric(df["claim_amount"], errors="coerce")
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["src_transaction_date"] = pd.to_datetime(df["src_transaction_date"], errors="coerce")

# Normalize 'status' (e.g., lowercase → Title Case)
df["status"] = df["status"].str.title()

# Normalize boolean column
df["investigation_required"] = df["investigation_required"].str.lower().map({
    "yes": True, "true": True, "1": True,
    "no": False, "false": False, "0": False
})

# Add timestamp
df["received_at"] = datetime.now()

# Drop duplicates if needed
df.drop_duplicates(subset=["claim_id"], inplace=True)

# Insert into silver table
cursor = silver_conn.cursor()
for _, row in df.iterrows():
    values = tuple(row)
    placeholders = ', '.join(['%s'] * len(row))
    insert_sql = f"""
        INSERT INTO public.silver_claims ({', '.join(df.columns)})
        VALUES ({placeholders});
    """
    cursor.execute(insert_sql, values)

silver_conn.commit()
cursor.close()
bronze_conn.close()
silver_conn.close()

print("claims_staging ➜ silver_claims transformation complete.")
