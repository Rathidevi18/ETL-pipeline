import psycopg2
import pandas as pd
from datetime import datetime

# Connect to Bronze (bronze schema)
bronze_conn = psycopg2.connect(
    dbname="bronze_layer_db",
    user="postgres",
    password="rathi",
    host="localhost",
    port="5432"
)

# Connect to Silver (public schema)
silver_conn = psycopg2.connect(
    dbname="silver_layer_db",
    user="postgres",
    password="rathi",
    host="localhost",
    port="5432"
)

# Load from bronze.insurance_dataset_staging
df = pd.read_sql('SELECT * FROM bronze."Insurance_dataset_staging" ', bronze_conn, dtype=str)
print("\n Columns loaded from Insurance_dataset_staging:")
print(df.columns.tolist())
df.columns = df.columns.str.strip()

# Data cleansing and type casting
df["DOB"] = pd.to_datetime(df["DOB"], errors="coerce")
df["Age"] = pd.to_numeric(df["Age"], errors="coerce").fillna(0).astype("Int64")
df["Credit_Score"] = pd.to_numeric(df["Credit_Score"], errors="coerce")
df["Claims_History"] = pd.to_numeric(df["Claims_History"], errors="coerce").fillna(0).astype("Int64")
df["SRC_TRANSACTION_DATE"] = pd.to_datetime(df["SRC_TRANSACTION_DATE"], errors="coerce")

# Mask Aadhaar & DL numbers
df["DL_Number"] = df["DL_Number"].str.replace(r".", "X", regex=True)
df["Aadhaar_Number"] = df["Aadhaar_Number"].str.replace(r"\d{8}", "XXXXXXXX", regex=True)

# Add received_at column
df["received_at"] = datetime.now()

# Drop duplicates
df = df.drop_duplicates(subset=["Customer_ID"])

# Insert into silver table
cursor = silver_conn.cursor()

for _, row in df.iterrows():
    values = tuple(row)
    placeholders = ', '.join(['%s'] * len(row))
    insert_sql = f"""
        INSERT INTO public.silver_insurance_dataset ({', '.join(df.columns)})
        VALUES ({placeholders});
    """
    cursor.execute(insert_sql, values)

silver_conn.commit()
cursor.close()
bronze_conn.close()
silver_conn.close()

print("insurance_dataset_staging to silver_insurance_dataset completed.")
