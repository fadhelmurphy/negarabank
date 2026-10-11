-- Model dbt harus menghasilkan baris yang sama dengan view di 01_customer_health_scorecard.sql.
-- Mengembalikan baris yang berbeda dari dua arah; test lolos kalau tidak ada baris yang kembali.
{% set cols = 'customer_id, report_month, total_balance, balance_change_mom_pct, txn_count_total, credit_utilization_ratio, probability_of_default, risk_flag' %}
with dbt_side as (select {{ cols }} from {{ ref('fct_customer_health_monthly') }}),
     view_side as (select {{ cols }} from gold.v_customer_health_scorecard)
(select 'only_in_dbt' as side, * from (select * from dbt_side except select * from view_side) a)
union all
(select 'only_in_view', * from (select * from view_side except select * from dbt_side) b)
