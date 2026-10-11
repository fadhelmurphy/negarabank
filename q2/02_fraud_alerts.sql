-- Q2 (e): alert fraud. Output: customer_id, alert_type, alert_date, details_json (satu row per nasabah,
-- aturan, dan hari).
--
--  alert_type:
--   VELOCITY_5_IN_1H      5 transaksi atau lebih dalam satu jam (semua status, karena card testing
--                         muncul sebagai banyak percobaan gagal)
--   MULTI_CITY_SAME_DAY   merchant di 3 kota atau lebih dalam satu hari (hanya QRIS dan ATM)
--   AMOUNT_GT_3X_30D_AVG  transaksi keluar di atas 3x rata-rata nasabah selama 30 hari sebelumnya
CREATE OR REPLACE VIEW gold.v_fraud_alerts AS
WITH txn AS (
    -- setiap transaksi beserta customer_id-nya.
    -- Transaksi hanya punya account_id, padahal semua aturan per nasabah, jadi join ke accounts sekali di sini.
    SELECT a.customer_id,
           t.txn_id,
           t.txn_date,
           t.txn_type,
           ABS(t.amount) AS amount_abs,
           t.channel,
           t.status,
           t.reference_id
    FROM silver.transactions t
    JOIN silver.accounts a ON a.account_id = t.account_id
),

txn_last_hour AS (
    -- untuk setiap transaksi, berapa transaksi nasabah yang sama dalam satu jam
    -- sebelumnya. RANGE dengan INTERVAL memakai waktu sungguhan; ROWS hanya menghitung N row terakhir,
    -- walaupun jaraknya berhari-hari. Semua status dihitung, karena card testing muncul sebagai percobaan gagal.
    SELECT customer_id,
           txn_date,
           COUNT(*) OVER (PARTITION BY customer_id
                          ORDER BY txn_date
                          RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS txn_count_1h
    FROM txn
),
velocity_alerts AS (
    -- ambil transaksi yang hitungannya mencapai 5, lalu satu alert per nasabah per hari.
    SELECT customer_id,
           'VELOCITY_5_IN_1H' AS alert_type,
           txn_date::date     AS alert_date,
           jsonb_build_object('max_txn_in_1_hour', MAX(txn_count_1h),
                              'first_flagged_at',  MIN(txn_date),
                              'last_flagged_at',   MAX(txn_date),
                              'threshold',         5) AS details_json
    FROM txn_last_hour
    WHERE txn_count_1h >= 5
    GROUP BY customer_id, txn_date::date
),

multi_city_alerts AS (
    -- hitung kota merchant berbeda per nasabah per hari, alert kalau 3 atau lebih.
    -- Hanya QRIS dan ATM, karena keduanya butuh nasabah hadir secara fisik di merchant.
    -- Transfer lewat app dan web bisa dari mana saja, jadi hanya akan menghasilkan alert palsu.
    SELECT t.customer_id,
           'MULTI_CITY_SAME_DAY' AS alert_type,
           t.txn_date::date      AS alert_date,
           jsonb_build_object('city_count', COUNT(DISTINCT m.merchant_city),
                              'cities',     string_agg(DISTINCT m.merchant_city, ', '),
                              'threshold',  3) AS details_json
    FROM txn t
    JOIN silver.merchant_locations m ON m.reference_id = t.reference_id
    WHERE t.status IN ('COMPLETED', 'PENDING')
      AND t.channel IN ('QRIS', 'ATM_NETWORK')
    GROUP BY t.customer_id, t.txn_date::date
    HAVING COUNT(DISTINCT m.merchant_city) >= 3
),

outgoing_with_avg AS (
    -- untuk setiap transaksi keluar, rata-rata nominal nasabah selama 30 hari sebelumnya.
    -- EXCLUDE CURRENT ROW mengeluarkan transaksi itu dari rata-ratanya sendiri; kalau ikut, transaksi besar
    -- akan menaikkan rata-rata dan menyembunyikan dirinya. Jumlahnya dipakai untuk syarat riwayat minimal.
    SELECT customer_id,
           txn_id,
           txn_date,
           amount_abs,
           AVG(amount_abs) OVER (PARTITION BY customer_id
                                 ORDER BY txn_date
                                 RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW
                                 EXCLUDE CURRENT ROW) AS avg_30d,
           COUNT(*)        OVER (PARTITION BY customer_id
                                 ORDER BY txn_date
                                 RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW
                                 EXCLUDE CURRENT ROW) AS txn_count_30d
    FROM txn
    WHERE status = 'COMPLETED'
      AND txn_type IN ('DEBIT', 'TRANSFER_OUT', 'PAYMENT')   -- uang keluar; FEE dibebankan oleh bank
),
amount_alerts AS (
    -- alert kalau transaksi lebih dari 3x rata-rata itu, satu alert per nasabah per hari.
    SELECT customer_id,
           'AMOUNT_GT_3X_30D_AVG' AS alert_type,
           txn_date::date         AS alert_date,
           jsonb_build_object('txn_ids',         string_agg(txn_id, ', '),
                              'max_amount',      MAX(amount_abs),
                              'max_ratio',       ROUND(MAX(amount_abs / avg_30d), 2),
                              'threshold_ratio', 3) AS details_json
    FROM outgoing_with_avg
    WHERE txn_count_30d >= 5               -- butuh riwayat dulu sebelum menilai "tidak biasa"
      AND amount_abs > 3 * avg_30d
    GROUP BY customer_id, txn_date::date
)

-- Select akhir: ketiga aturan punya kolom yang sama, jadi UNION ALL menggabungkannya jadi satu output.
SELECT * FROM velocity_alerts
UNION ALL
SELECT * FROM multi_city_alerts
UNION ALL
SELECT * FROM amount_alerts;
