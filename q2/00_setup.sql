-- Persiapan Q2: index dan tabel bantu yang dipakai view.

-- index untuk join dan filter tanggal di bawah
CREATE INDEX IF NOT EXISTS ix_txn_account_date ON bronze.transactions (account_id, txn_date);
CREATE INDEX IF NOT EXISTS ix_txn_date         ON bronze.transactions (txn_date);
CREATE INDEX IF NOT EXISTS ix_accounts_customer ON bronze.accounts (customer_id);
CREATE INDEX IF NOT EXISTS ix_scores_customer   ON bronze.credit_scores (customer_id, score_date);

-- Saldo akhir bulan per akun.
-- Di produksi ini diambil dari snapshot Q1 (snapshot_date terakhir di setiap bulan).
-- Sample hanya punya saldo saat ini, jadi riwayatnya dibangun mundur:
--   saldo akhir bulan M = saldo sekarang - mutasi bersih (COMPLETED) bulan-bulan setelah M
-- (amount sudah bertanda: uang masuk positif, uang keluar negatif)
CREATE SCHEMA IF NOT EXISTS silver;
DROP TABLE IF EXISTS silver.account_month_end_balance;
CREATE TABLE silver.account_month_end_balance AS
WITH months AS (
    SELECT DISTINCT date_trunc('month', txn_date)::date AS report_month
    FROM bronze.transactions
),
monthly_flow AS (
    SELECT account_id,
           date_trunc('month', txn_date)::date AS report_month,
           SUM(amount) AS net_flow
    FROM bronze.transactions
    WHERE status = 'COMPLETED'
    GROUP BY account_id, date_trunc('month', txn_date)
)
SELECT a.account_id,
       a.customer_id,
       a.account_type,
       a.credit_limit,
       m.report_month,
       a.balance - COALESCE(SUM(f.net_flow), 0) AS month_end_balance
FROM bronze.accounts a
CROSS JOIN months m
LEFT JOIN monthly_flow f
       ON f.account_id = a.account_id
      AND f.report_month > m.report_month
WHERE a.opened_date < m.report_month + INTERVAL '1 month'
GROUP BY a.account_id, a.customer_id, a.account_type, a.credit_limit, a.balance, m.report_month;

CREATE INDEX ON silver.account_month_end_balance (customer_id, report_month);

-- Lokasi merchant.
-- Aturan "3+ kota dalam sehari" butuh kota merchant, yang tidak ada di skema. Saya asumsikan ada tabel
-- seperti ini dengan kunci reference_id dari transaksi.
DROP TABLE IF EXISTS bronze.merchant_locations;
CREATE TABLE bronze.merchant_locations AS
SELECT reference_id,
       (ARRAY['Jakarta','Surabaya','Bandung','Medan','Semarang','Makassar','Palembang',
              'Tangerang','Depok','Bekasi','Denpasar','Yogyakarta'])[1 + abs(hashtext(reference_id || 'c')) % 12] AS merchant_city
FROM (SELECT DISTINCT reference_id FROM bronze.transactions WHERE reference_id IS NOT NULL) r;
ALTER TABLE bronze.merchant_locations ADD PRIMARY KEY (reference_id);

-- Lapisan silver yang dibaca view gold (01, 02) dan model dbt.
-- Gold tidak membaca bronze langsung: bronze tetap data mentah apa adanya, silver adalah data yang sudah
-- dibersihkan dan siap dipakai. Di sample ini pembersihannya cukup buang duplikat per primary key,
-- karena tipe data sudah rapi saat load. Kolomnya sama dengan bronze, jadi hasil gold tidak berubah.
DROP TABLE IF EXISTS silver.transactions;
CREATE TABLE silver.transactions AS
SELECT DISTINCT ON (txn_id) *
FROM bronze.transactions
ORDER BY txn_id;
ALTER TABLE silver.transactions ADD PRIMARY KEY (txn_id);
CREATE INDEX ON silver.transactions (account_id, txn_date);
CREATE INDEX ON silver.transactions (txn_date);

DROP TABLE IF EXISTS silver.accounts;
CREATE TABLE silver.accounts AS
SELECT DISTINCT ON (account_id) *
FROM bronze.accounts
ORDER BY account_id;
ALTER TABLE silver.accounts ADD PRIMARY KEY (account_id);
CREATE INDEX ON silver.accounts (customer_id);

DROP TABLE IF EXISTS silver.credit_scores;
CREATE TABLE silver.credit_scores AS
SELECT DISTINCT ON (score_id) *
FROM bronze.credit_scores
ORDER BY score_id;
ALTER TABLE silver.credit_scores ADD PRIMARY KEY (score_id);
CREATE INDEX ON silver.credit_scores (customer_id, score_date);

DROP TABLE IF EXISTS silver.merchant_locations;
CREATE TABLE silver.merchant_locations AS
SELECT DISTINCT ON (reference_id) *
FROM bronze.merchant_locations
ORDER BY reference_id;
ALTER TABLE silver.merchant_locations ADD PRIMARY KEY (reference_id);

ANALYZE;
