-- gagal (mengembalikan baris) kalau ada lebih dari satu baris per nasabah per bulan
select customer_id, report_month, count(*) as n
from {{ ref('fct_customer_health_monthly') }}
group by 1, 2
having count(*) > 1
