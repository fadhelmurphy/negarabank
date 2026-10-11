-- Load the sample dataset into PostgreSQL (bronze schema) for local testing
DROP SCHEMA IF EXISTS bronze CASCADE; CREATE SCHEMA bronze;
CREATE TABLE bronze.customers (customer_id VARCHAR(15) PRIMARY KEY, full_name VARCHAR(100), nik VARCHAR(16), phone VARCHAR(15), email VARCHAR(80), city VARCHAR(30), province VARCHAR(30), registration_date DATE, kyc_status VARCHAR(15), risk_score DECIMAL(5,2), segment VARCHAR(20));
CREATE TABLE bronze.accounts (account_id VARCHAR(20) PRIMARY KEY, customer_id VARCHAR(15), account_type VARCHAR(20), product_name VARCHAR(50), opened_date DATE, status VARCHAR(15), balance DECIMAL(15,2), credit_limit DECIMAL(15,2), interest_rate DECIMAL(5,4));
CREATE TABLE bronze.transactions (txn_id VARCHAR(25) PRIMARY KEY, account_id VARCHAR(20), txn_date TIMESTAMP, txn_type VARCHAR(20), amount DECIMAL(15,2), merchant_category VARCHAR(30), channel VARCHAR(20), reference_id VARCHAR(30), status VARCHAR(15));
CREATE TABLE bronze.credit_scores (score_id VARCHAR(20) PRIMARY KEY, customer_id VARCHAR(15), score_date DATE, model_version VARCHAR(10), credit_score INTEGER, probability_of_default DECIMAL(5,4), features_used JSONB);
\copy bronze.customers FROM 'DATA/customers.csv' CSV HEADER
\copy bronze.accounts FROM 'DATA/accounts.csv' CSV HEADER
\copy bronze.transactions FROM 'DATA/transactions.csv' CSV HEADER
\copy bronze.credit_scores FROM 'DATA/credit_scores.csv' CSV HEADER
ANALYZE;
