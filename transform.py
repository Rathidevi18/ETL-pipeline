import psycopg2
import pandas as pd
from datetime import datetime

# Connect to bronze_db
conn = psycopg2.connect(
    dbname="bronze_layer_db",
    user="postgres",
    password="rathi",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# List of staging tables (all TEXT columns)
tables = [
    "claims_staging",
    "customer_identity_staging",
    "customer_policy_relation_staging",
    "customer_staging",
    "insurance_dataset_staging",
    "policies_staging",
    "underwriting_staging"
]

# Process each table
for table in tables:
    print(f"\n Processing table: {table}")

    # Read table into Pandas (all as string/text)
    df = pd.read_sql(f"SELECT * FROM {table}", conn, dtype=str)
    df['received_at'] = datetime.now()  # Add timestamp

    # Create new table name
    new_table = f"{table}_with_ts"

    # Build CREATE TABLE SQL
    columns = df.columns.tolist()
    column_defs = ", ".join([f'"{col}" TEXT' for col in columns if col != 'received_at'])
    create_sql = f"""
    DROP TABLE IF EXISTS {new_table};
    CREATE TABLE {new_table} (
        {column_defs},
        received_at TIMESTAMP
    );
    """
    cursor.execute(create_sql)
    conn.commit()
    print(f"Created table: {new_table}")

    # Insert rows into the new table
    for _, row in df.iterrows():
        placeholders = ",".join(["%s"] * len(columns))
        insert_sql = f'INSERT INTO {new_table} ({", ".join(columns)}) VALUES ({placeholders});'
        cursor.execute(insert_sql, tuple(row))

    conn.commit()
    print(f" Loaded {len(df)} rows into: {new_table}")

# Cleanup
cursor.close()
conn.close()
print("\n All tables transformed successfully.")
