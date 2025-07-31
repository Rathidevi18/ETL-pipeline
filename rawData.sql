-- Table for Customers
CREATE TABLE IF NOT EXISTS raw_customers (
    Customer_ID TEXT PRIMARY KEY,
    Name TEXT,
    DOB DATE,
    DL_Number BIGINT,
    Aadhaar_Number BIGINT,
    Age INT,
    Door_No INT,
    Street TEXT,
    City TEXT,
    State TEXT,
    Country TEXT,
    Credit_Score INT,
    Risk_Profile TEXT,
    Claims_History INT,
    SRC_TRANSACTION_DATE TIMESTAMP
);

-- Table for Customer_Policy_Relation
CREATE TABLE IF NOT EXISTS raw_customer_policy_relation (
    Customer_ID TEXT,
    Policy_ID TEXT,
    Start_Date DATE,
    End_Date DATE,
    Status TEXT,
    SRC_TRANSACTION_DATE TIMESTAMP
);

-- Table for Claims
CREATE TABLE IF NOT EXISTS raw_claims (
    Claim_ID TEXT PRIMARY KEY,
    Policy_ID TEXT,
    Claim_Amount TEXT,
    Date DATE,
    Status TEXT,
    Investigation_Required TEXT,
    SRC_TRANSACTION_DATE TIMESTAMP
);

-- Table for Underwriting
CREATE TABLE IF NOT EXISTS raw_underwriting (
    Customer_ID TEXT,
    Policy_ID TEXT,
    Risk_Score INT,
    Approval_Status TEXT,
    SRC_TRANSACTION_DATE TIMESTAMP
);

-- Table for Policies
CREATE TABLE IF NOT EXISTS raw_policies (
    Policy_ID TEXT PRIMARY KEY,
    Customer_ID TEXT,
    Policy_Type TEXT,
    Premium TEXT,
    Coverage_Amount TEXT,
    Status TEXT,
    SRC_TRANSACTION_DATE TIMESTAMP
);

-- Table for JSON: Customer Identity
CREATE TABLE IF NOT EXISTS raw_customer_identity (
    id SERIAL PRIMARY KEY,
    data JSONB
);

-- Table for JSON: Insurance Dataset
CREATE TABLE IF NOT EXISTS raw_insurance_dataset (
    id SERIAL PRIMARY KEY,
    data JSONB
);
