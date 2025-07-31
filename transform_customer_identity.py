import psycopg2
import pandas as pd
from datetime import datetime

# Connect to source (bronze) and target (silver)
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

# Load data
df = pd.read_sql('SELECT * FROM "bronze"."customer_identity_staging"', bronze_conn, dtype=str)
df.columns = df.columns.str.strip()
print("Loaded:", df.columns.tolist())

# Mask Aadhaar: Keep last 4 digits
df["aadhaar_number"] = df["aadhaar_number"].str.replace(r"\d{8}(?=\d{4})", "XXXXXXXX", regex=True)

# Mask DL number fully
df["dl_number"] = df["dl_number"].str.replace(r".", "X", regex=True)

# Add received_at timestamp
df["received_at"] = datetime.now()

# Drop duplicate customers
df.drop_duplicates(subset=["customer_id"], inplace=True)

# Insert into silver
cursor = silver_conn.cursor()
for _, row in df.iterrows():
    values = tuple(row)
    placeholders = ', '.join(['%s'] * len(row))
    insert_sql = f"""
        INSERT INTO public.silver_customer_identity ({', '.join(df.columns)})
        VALUES ({placeholders});
    """
    cursor.execute(insert_sql, values)

silver_conn.commit()
cursor.close()
bronze_conn.close()
silver_conn.close()

print("customer_identity_staging to silver_customer_identity completed.")
