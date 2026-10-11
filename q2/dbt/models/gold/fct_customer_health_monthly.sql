-- Q2 (f): scorecard dari 01_customer_health_scorecard.sql sebagai model dbt incremental.
-- Run pertama (atau --full-refresh): membangun semua bulan.
-- Run berikutnya: hanya membangun ulang bulan terakhir di tabel, bulan sebelumnya (untuk transaksi telat),
-- dan bulan baru kalau ada. Baris bulan-bulan itu dihapus lalu dimasukkan lagi (delete+insert).
{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = ['customer_id', 'report_month']
) }}

WITH monthly_balance AS (
    -- CTE 1: total saldo per nasabah per bulan.
    -- Satu nasabah bisa punya beberapa akun (tabungan, pinjaman, kartu...), jadi saldo akunnya dijumlahkan.
    -- Saldo dan limit kartu kredit dijumlahkan terpisah karena dibutuhkan untuk utilisasi kredit.
    SELECT customer_id,
           report_month,
           COUNT(*)                                                                AS num_accounts,
           SUM(month_end_balance)                                                  AS total_balance,
           SUM(CASE WHEN account_type = 'CREDIT_CARD' THEN month_end_balance END) AS cc_balance,
           SUM(CASE WHEN account_type = 'CREDIT_CARD' THEN credit_limit END)      AS cc_credit_limit
    FROM {{ source('silver', 'account_month_end_balance') }}
    {% if is_incremental() %}
    -- incremental: satu bulan lebih awal dari jendela rebuild, supaya LAG punya bulan sebelumnya
    WHERE report_month >= (SELECT MAX(report_month) FROM {{ this }}) - INTERVAL '2 month'
    {% endif %}
    GROUP BY customer_id, report_month
),
balance_with_prev AS (
    -- CTE 2: taruh saldo bulan lalu di sebelah saldo bulan ini.
    -- LAG melihat baris sebelumnya dari nasabah yang sama (urut bulan), jadi perubahan bulan ke bulan
    -- bisa dihitung di select akhir tanpa join tabel ke dirinya sendiri.
    SELECT *,
           LAG(total_balance) OVER (PARTITION BY customer_id ORDER BY report_month) AS prev_month_balance
    FROM monthly_balance
),
monthly_txn AS (
    -- CTE 3: angka transaksi per nasabah per bulan.
    -- Conditional aggregation: setiap SUM(CASE ...) menghitung satu jenis transaksi, setiap AVG(CASE ...)
    -- merata-ratakan satu channel. Transaksi cukup dibaca sekali, tidak perlu satu query per jenis.
    -- Hanya COMPLETED yang dihitung; FAILED dan REVERSED punya kolom sendiri.
    -- Transaksi diringkas di sini, sebelum join, jadi join berikutnya bekerja di tabel kecil.
    SELECT a.customer_id,
           date_trunc('month', t.txn_date)::date AS report_month,
           SUM(CASE WHEN t.status = 'COMPLETED' THEN 1 ELSE 0 END)                                 AS txn_count_total,
           SUM(CASE WHEN t.status = 'COMPLETED' AND t.txn_type = 'DEBIT' THEN 1 ELSE 0 END)        AS txn_count_debit,
           SUM(CASE WHEN t.status = 'COMPLETED' AND t.txn_type = 'CREDIT' THEN 1 ELSE 0 END)       AS txn_count_credit,
           SUM(CASE WHEN t.status = 'COMPLETED' AND t.txn_type = 'TRANSFER_IN' THEN 1 ELSE 0 END)  AS txn_count_transfer_in,
           SUM(CASE WHEN t.status = 'COMPLETED' AND t.txn_type = 'TRANSFER_OUT' THEN 1 ELSE 0 END) AS txn_count_transfer_out,
           SUM(CASE WHEN t.status = 'COMPLETED' AND t.txn_type = 'PAYMENT' THEN 1 ELSE 0 END)      AS txn_count_payment,
           SUM(CASE WHEN t.status = 'COMPLETED' AND t.txn_type = 'FEE' THEN 1 ELSE 0 END)          AS txn_count_fee,
           SUM(CASE WHEN t.status IN ('FAILED', 'REVERSED') THEN 1 ELSE 0 END)                     AS txn_count_failed_or_reversed,
           ROUND(AVG(CASE WHEN t.status = 'COMPLETED' AND t.channel = 'MOBILE_APP'  THEN ABS(t.amount) END), 2) AS avg_amt_mobile_app,
           ROUND(AVG(CASE WHEN t.status = 'COMPLETED' AND t.channel = 'WEB'         THEN ABS(t.amount) END), 2) AS avg_amt_web,
           ROUND(AVG(CASE WHEN t.status = 'COMPLETED' AND t.channel = 'QRIS'        THEN ABS(t.amount) END), 2) AS avg_amt_qris,
           ROUND(AVG(CASE WHEN t.status = 'COMPLETED' AND t.channel = 'ATM_NETWORK' THEN ABS(t.amount) END), 2) AS avg_amt_atm_network,
           ROUND(AVG(CASE WHEN t.status = 'COMPLETED' AND t.channel = 'AUTO_DEBIT'  THEN ABS(t.amount) END), 2) AS avg_amt_auto_debit
    FROM {{ source('silver', 'transactions') }} t
    JOIN {{ source('silver', 'accounts') }} a ON a.account_id = t.account_id
    {% if is_incremental() %}
    -- incremental: hanya baca transaksi bulan yang sedang dibangun ulang
    WHERE t.txn_date >= (SELECT MAX(report_month) FROM {{ this }}) - INTERVAL '1 month'
    {% endif %}
    GROUP BY a.customer_id, date_trunc('month', t.txn_date)
),
latest_score AS (
    -- CTE 4: skor kredit terbaru yang sudah ada di akhir setiap bulan.
    -- Join semua skor sampai akhir bulan, beri nomor dari yang terbaru dengan ROW_NUMBER, lalu ambil rn = 1.
    -- Dengan begitu baris Januari tidak pernah memakai skor yang baru dibuat di Februari.
    -- Beberapa nasabah punya dua skor di hari yang sama, jadi score_id dipakai sebagai penentu.
    SELECT b.customer_id,
           b.report_month,
           s.probability_of_default,
           s.credit_score,
           s.score_date,
           ROW_NUMBER() OVER (PARTITION BY b.customer_id, b.report_month
                              ORDER BY s.score_date DESC, s.score_id DESC) AS rn  -- score_id menentukan kalau tanggalnya sama
    FROM monthly_balance b
    JOIN {{ source('silver', 'credit_scores') }} s
      ON s.customer_id = b.customer_id
     AND s.score_date < b.report_month + INTERVAL '1 month'
),
scorecard AS (
    -- CTE 5: gabungkan semuanya, satu baris per nasabah per bulan.
    -- LEFT JOIN supaya nasabah tanpa transaksi atau tanpa skor di bulan itu tetap dapat baris.
    -- COALESCE mengubah hitungan kosong jadi 0; NULLIF mencegah pembagian dengan nol.
    SELECT b.customer_id,
           b.report_month,
           b.num_accounts,
           b.total_balance,
           b.prev_month_balance,
           b.total_balance - b.prev_month_balance                                                     AS balance_change_mom,
           ROUND((b.total_balance - b.prev_month_balance) / NULLIF(ABS(b.prev_month_balance), 0) * 100, 2) AS balance_change_mom_pct,
           COALESCE(t.txn_count_total, 0)              AS txn_count_total,
           COALESCE(t.txn_count_debit, 0)              AS txn_count_debit,
           COALESCE(t.txn_count_credit, 0)             AS txn_count_credit,
           COALESCE(t.txn_count_transfer_in, 0)        AS txn_count_transfer_in,
           COALESCE(t.txn_count_transfer_out, 0)       AS txn_count_transfer_out,
           COALESCE(t.txn_count_payment, 0)            AS txn_count_payment,
           COALESCE(t.txn_count_fee, 0)                AS txn_count_fee,
           COALESCE(t.txn_count_failed_or_reversed, 0) AS txn_count_failed_or_reversed,
           t.avg_amt_mobile_app,
           t.avg_amt_web,
           t.avg_amt_qris,
           t.avg_amt_atm_network,
           t.avg_amt_auto_debit,
           ROUND(b.cc_balance / NULLIF(b.cc_credit_limit, 0), 4) AS credit_utilization_ratio,  -- NULL kalau tidak punya kartu kredit
           s.probability_of_default,
           s.credit_score,
           s.score_date AS credit_score_date
    FROM balance_with_prev b
    LEFT JOIN monthly_txn t
           ON t.customer_id = b.customer_id
          AND t.report_month = b.report_month
    LEFT JOIN latest_score s
           ON s.customer_id = b.customer_id
          AND s.report_month = b.report_month
          AND s.rn = 1
)
-- Select akhir: tambah risk_flag. TRUE kalau salah satu dari tiga aturan di soal terpenuhi.
-- Aturan saldo turun hanya berlaku kalau saldo bulan lalu positif, kalau tidak persentasenya
-- tidak bermakna.
SELECT *,
       CASE WHEN credit_utilization_ratio > 0.80
              OR probability_of_default > 0.30
              OR (balance_change_mom_pct < -30 AND prev_month_balance > 0)
            THEN TRUE ELSE FALSE
       END AS risk_flag
FROM scorecard
{% if is_incremental() %}
-- incremental: simpan hanya bulan yang dibangun ulang (bulan lalu, bulan terakhir, dan yang lebih baru)
WHERE report_month >= (SELECT MAX(report_month) FROM {{ this }}) - INTERVAL '1 month'
{% endif %}
