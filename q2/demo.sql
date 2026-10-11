
\echo '== (d) Scorecard: satu nasabah dari bulan ke bulan'
SELECT customer_id, report_month, total_balance, balance_change_mom_pct, txn_count_total, txn_count_debit,
       credit_utilization_ratio, probability_of_default, risk_flag
FROM gold.v_customer_health_scorecard WHERE customer_id = 'CUST0000002' ORDER BY report_month;

\echo '== (d) Jumlah baris nasabah-bulan yang kena tiap aturan'
SELECT SUM(CASE WHEN risk_flag THEN 1 ELSE 0 END)                                            AS flagged,
       SUM(CASE WHEN credit_utilization_ratio > 0.80 THEN 1 ELSE 0 END)                      AS utilisation_gt_80,
       SUM(CASE WHEN probability_of_default > 0.30 THEN 1 ELSE 0 END)                        AS pd_gt_0_3,
       SUM(CASE WHEN balance_change_mom_pct < -30 AND prev_month_balance > 0 THEN 1 ELSE 0 END) AS balance_drop_gt_30
FROM gold.v_customer_health_scorecard;

\echo '== (e) Alert fraud per jenis'
SELECT alert_type, COUNT(*) AS alerts, COUNT(DISTINCT customer_id) AS customers
FROM gold.v_fraud_alerts GROUP BY alert_type ORDER BY alerts;

\echo '== (e) Contoh alert beserta buktinya'
SELECT customer_id, alert_type, alert_date, jsonb_pretty(details_json)
FROM gold.v_fraud_alerts WHERE alert_type = 'VELOCITY_5_IN_1H' ORDER BY alert_date, customer_id LIMIT 1;

\echo '== (f) Model incremental hasil dbt (jalankan: docker compose run --rm dbt build)'
SELECT report_month, COUNT(*) FROM dbt_gold.fct_customer_health_monthly GROUP BY report_month ORDER BY report_month;
