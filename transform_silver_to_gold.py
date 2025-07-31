import os
import csv
import re
import shutil
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start Spark session
spark = SparkSession.builder.appName("Gold Layer Transformation").getOrCreate()
print(" Spark session started")

# Helper to read CSVs
def read_csv(path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

# Read Silver layer CSVs (replace path if needed)
cust = read_csv("data/silver_customers.csv").dropDuplicates(["customer_id"]).alias("cust")
ci = read_csv("data/silver_customer_identity.csv").dropDuplicates(["customer_id"]).alias("ci")
cpr = read_csv("data/silver_customer_policy_relation.csv").dropDuplicates(["customer_id", "policy_id"]).alias("cpr")
pol = read_csv("data/silver_policies.csv").dropDuplicates(["policy_id"]).alias("pol")
uw = read_csv("data/silver_underwriting.csv").dropDuplicates(["customer_id", "policy_id"]).alias("uw")
cl = read_csv("data/silver_claims.csv").dropDuplicates(["customer_id"]).alias("cl")
ds = read_csv("data/silver_insurance_dataset.csv").dropDuplicates(["customer_id"]).alias("ds")

print("Silver tables loaded")

# Clean and validate data
cust = cust.withColumn("credit_score", when(col("credit_score").isNull(), lit(650)).otherwise(col("credit_score")))
cust = cust.withColumn("age", when(col("age") > 0, col("age")).otherwise(lit(0)))
cust = cust.withColumn("dob", to_date(col("dob"), "yyyy-MM-dd"))

# Risk score validation
uw = uw.withColumn("risk_score", when((col("risk_score").isNull()) | (col("risk_score") < 0), lit(None)).otherwise(col("risk_score")))

# Claims history defaulting
cust = cust.withColumn("claims_history", when(col("claims_history") < 0, lit(0)).otherwise(col("claims_history")))

# Derive policy duration
duration_expr = datediff(col("end_date"), col("start_date"))

# Perform JOINs with aliasing
step1 = cust.join(ci, cust["customer_id"] == ci["customer_id"], "left")
step2 = step1.join(cpr, cust["customer_id"] == cpr["customer_id"], "left")
step3 = step2.join(pol, cpr["policy_id"] == pol["policy_id"], "left")
step4 = step3.join(uw, (cust["customer_id"] == uw["customer_id"]) & (pol["policy_id"] == uw["policy_id"]), "left")
step5 = step4.join(cl.select("customer_id", "aadhaar_number", "dl_number", "received_at"), "customer_id", "left")
step6 = step5.join(ds, cust["customer_id"] == ds["customer_id"], "left")

# Final SELECT with all transformations
final_df = step6.select(
    cust["customer_id"],
    pol["policy_id"],
    cust["name"].alias("customer_name"),
    ci["aadhaar_number"],
    ci["dl_number"],
    cust["dob"],
    cust["age"],
    cust["city"],
    cust["state"],
    cust["country"],
    cust["credit_score"],
    cust["risk_profile"],
    cust["claims_history"],
    cpr["start_date"],
    cpr["end_date"],
    duration_expr.alias("policy_duration"),
    cpr["status"].alias("policy_status"),
    pol["policy_type"],
    pol["premium"],
    pol["coverage_amount"],
    uw["risk_score"],
    uw["approval_status"],
    ds["credit_score"].alias("ds_credit_score"),
    ds["risk_profile"].alias("ds_risk_profile"),
    ds["claims_history"].alias("ds_claims_history")
)

# Mask Aadhaar and DL
final_df = final_df.withColumn("masked_aadhaar", concat(col("aadhaar_number").substr(1, 4), lit("********")))
final_df = final_df.withColumn("masked_dl", concat(col("dl_number").substr(1, 2), lit("******")))

# Add UUID and SHA2 Hash
final_df = final_df.withColumn("record_uuid", expr("uuid()"))
final_df = final_df.withColumn("record_hash_id", sha2(concat_ws(":", "customer_id", "policy_id"), 256))
final_df = final_df.withColumn("ingested_at", current_timestamp())

# Alert: High volume updates or bad data
total = final_df.count()
null_credits = final_df.filter(col("credit_score").isNull()).count()
if null_credits / total > 0.05:
    print("Alert: More than 5% of records missing Credit Score")

claim_counts = cl.groupBy("customer_id").count().filter(col("count") > 3)
if claim_counts.count() > 0:
    print(" Alert: Customers with high number of claims detected!")

# Deduplicate
final_df = final_df.dropDuplicates(["customer_id", "policy_id"])


# PostgreSQL connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/gold_layer_db"
connection_properties = {
    "user": "postgres",
    "password": "rathi",
    "driver": "org.postgresql.Driver"
}

# Recreate the table in PostgreSQL using psycopg2
conn = psycopg2.connect(
    dbname="gold_layer_db",
    user="postgres",
    password="rathi",
    host="localhost",
    port="5432"
)
cur = conn.cursor()
cur.execute("TRUNCATE TABLE public.gold_customer_policy_summary")

'''
# Drop and recreate the table
cur.execute("DROP TABLE IF EXISTS public.gold_customer_policy_summary")
cur.execute("""
    CREATE TABLE public.gold_customer_policy_summary (
        customer_id VARCHAR,
        policy_id VARCHAR,
        customer_name VARCHAR,
        aadhaar_number VARCHAR,
        dl_number VARCHAR,
        dob DATE,
        age INT,
        city VARCHAR,
        state VARCHAR,
        country VARCHAR,
        credit_score INT,
        risk_profile VARCHAR,
        claims_history TEXT,
        start_date DATE,
        end_date DATE,
        policy_duration INT,
        policy_status VARCHAR,
        policy_type VARCHAR,
        premium FLOAT,
        coverage_amount FLOAT,
        risk_score FLOAT,
        approval_status VARCHAR,
        ds_credit_score INT,
        ds_risk_profile VARCHAR,
        ds_claims_history TEXT,
        masked_aadhaar VARCHAR,
        masked_dl VARCHAR,
        record_uuid UUID,
        record_hash_id TEXT,
        ingested_at TIMESTAMP
    )
""")'''
conn.commit()

final_df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/gold_layer_db") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.gold_customer_policy_summary") \
    .option("user", "postgres") \
    .option("password", "rathi") \
    .mode("overwrite") \
    .save()

cur.close()
conn.close()

print("Gold-layer data written directly to PostgreSQL table: public.gold_customer_policy_summary")

# Stop the Spark session (optional, depending on your workflow)
spark.stop()