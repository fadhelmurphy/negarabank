# Q2: SQL untuk laporan OJK (PostgreSQL 16)
```
scripts/load_postgres.sql          CSV -> bronze.*
00_setup.sql                       index, saldo akhir bulan, lokasi merchant
01_customer_health_scorecard.sql   (d) view gold.v_customer_health_scorecard
02_fraud_alerts.sql                (e) view gold.v_fraud_alerts
dbt/                               (f) scorecard sebagai model dbt incremental
```

```bash
cd q2 && cp .env.example .env
docker compose up -d
docker compose run --rm dbt build
docker compose exec postgres psql -U postgres -d negarabank -f /sql/demo.sql   # Jika menggunakan Git Bash, tambahkan MSYS_NO_PATHCONV=1 di depan
```

**Asumsi.**
- `amount` sudah bertanda (uang keluar negatif), jadi mutasi bersih `SUM(amount)` dan rata-rata memakai `ABS(amount)`.
- Hanya COMPLETED yang dihitung sebagai aktivitas, kecuali aturan velocity yang menghitung semua percobaan.
- Sample hanya punya saldo saat ini, jadi saldo akhir bulan dibangun mundur di `00_setup.sql` (di produksi diambil dari snapshot Q1). Sebagian hasilnya negatif karena sample tidak cocok dengan transaksinya.
- Skema tidak punya lokasi merchant, jadi `merchant_locations` menggunakan data dummy
- Beberapa nasabah punya dua skor kredit di hari yang sama. Saya ambil yang `score_id`-nya lebih besar, supaya hasilnya sama di setiap run.

## (d) Scorecard

Satu baris per nasabah per bulan, dibangun dari beberapa CTE:

| CTE | Isinya | Kenapa |
|---|---|---|
| `monthly_balance` | `SUM` saldo per nasabah dan bulan; saldo dan limit kartu dengan `SUM(CASE WHEN account_type = 'CREDIT_CARD' ...)` | Nasabah bisa punya beberapa akun |
| `balance_with_prev` | `LAG(total_balance) OVER (PARTITION BY customer_id ORDER BY report_month)` | Saldo bulan lalu ada di baris yang sama, untuk menghitung perubahan bulan ke bulan |
| `monthly_txn` | `SUM(CASE WHEN txn_type = 'DEBIT' THEN 1 ELSE 0 END)` per jenis, `AVG(CASE WHEN channel = 'QRIS' THEN ABS(amount) END)` per channel | Conditional aggregation: transaksi dibaca sekali, bukan satu query per jenis |
| `latest_score` | `ROW_NUMBER()` per nasabah dan bulan, skor terbaru di urutan pertama, ambil `rn = 1` | Memakai skor terakhir yang sudah ada saat akhir bulan, bukan skor yang muncul belakangan |
| `scorecard` | Menggabungkan CTE di atas jadi satu baris per nasabah per bulan, dengan `LEFT JOIN` dan `COALESCE` | Nasabah tanpa transaksi atau skor di bulan itu tetap dapat baris; transaksi sudah diringkas dulu, jadi join hanya di ~28 ribu baris |
| select akhir | Menambahkan `risk_flag` dengan `CASE` | Tiga aturan risiko ada di satu tempat yang mudah dibaca |

`risk_flag` bernilai TRUE kalau utilisasi > 80%, PD > 0,3, atau saldo turun lebih dari 30% dari saldo bulan lalu yang positif.

Hasil: 28.000 baris, 13.539 ter-flag (PD > 0,3: 8.358; saldo turun > 30%: 5.988; utilisasi > 80%: 1.681). Sekitar 1,5 detik untuk seluruh riwayat.

## (e) Alert fraud

Output: `customer_id, alert_type, alert_date, details_json`, satu baris per nasabah, aturan, dan hari.

Query-nya dibangun dari CTE berikut:

| CTE | Isinya |
|---|---|
| `txn` | Menambahkan `customer_id` ke setiap transaksi (transaksi hanya punya `account_id`) |
| `txn_last_hour` | Aturan 1: untuk setiap transaksi, berapa transaksi nasabah itu dalam satu jam sebelumnya |
| `velocity_alerts` | Aturan 1: ambil yang jumlahnya 5 atau lebih, satu alert per nasabah per hari |
| `multi_city_alerts` | Aturan 2: jumlah kota merchant berbeda per nasabah per hari, alert kalau 3 atau lebih |
| `outgoing_with_avg` | Aturan 3: rata-rata nominal uang keluar 30 hari sebelumnya, tanpa transaksi itu sendiri |
| `amount_alerts` | Aturan 3: transaksi di atas 3× rata-rata itu, satu alert per nasabah per hari |
| select akhir | `UNION ALL` dari tiga CTE alert |

| Aturan | Cara | Kenapa |
|---|---|---|
| 5+ dalam 1 jam | `COUNT(*) OVER (PARTITION BY customer_id ORDER BY txn_date RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW)`, lalu `GROUP BY` nasabah dan hari | `RANGE` memakai waktu sungguhan. `ROWS` akan menghitung 5 baris terakhir walaupun jaraknya berhari-hari. Semua status dihitung, karena card testing muncul sebagai percobaan gagal |
| 3+ kota dalam sehari | `COUNT(DISTINCT merchant_city) >= 3` per nasabah dan hari, hanya QRIS dan ATM | Hanya itu yang butuh nasabah hadir secara fisik |
| > 3× rata-rata 30 hari | `AVG(...) OVER (... RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)`, hanya uang keluar, minimal 5 transaksi sebelumnya | Transaksi itu sendiri tidak ikut di rata-ratanya, kalau ikut ia akan menaikkan rata-rata dan menyembunyikan lonjakannya |

`details_json` menyimpan buktinya, misalnya `{"max_txn_in_1_hour": 5, "first_flagged_at": "...", "threshold": 5}`, atau ID transaksi dan rasionya untuk aturan nominal.

Hasil: 38 velocity, 12.706 multi-kota (kota dibuat-buat), 280 alert nominal.

## (f) Refresh malam yang lebih cepat

45 menit itu berasal dari `REFRESH MATERIALIZED VIEW`, yang menghitung ulang semua bulan setiap malam, termasuk bulan yang tidak akan berubah lagi.

| Opsi | Penilaian |
|---|---|
| Materialized view | Selalu full refresh |
| Snowflake dynamic table | Incremental, tapi platform saya Databricks, dan window function seperti `LAG` sering memaksa full refresh |
| **dbt incremental** (dipilih) | Hanya bulan terbaru yang dibangun ulang, jalan tepat setelah snapshot Q1, dan dbt menambahkan test dan lineage yang membantu audit OJK |

Modelnya (`dbt/models/gold/fct_customer_health_monthly.sql`) berisi query yang sama dengan view, ditambah filter incremental standar dbt:

```sql
{{ config(materialized='incremental', incremental_strategy='delete+insert',
          unique_key=['customer_id', 'report_month']) }}
...
{% if is_incremental() %}
WHERE report_month >= (SELECT MAX(report_month) FROM {{ this }}) - INTERVAL '1 month'
{% endif %}
```

- **Run pertama:** membangun semua bulan.
- **Setiap run setelahnya:** membangun ulang bulan terakhir di tabel dan bulan sebelumnya (transaksi telat dari akhir bulan lalu tetap tertangkap), plus bulan baru kalau ada. Untuk `LAG`, CTE saldo membaca satu bulan tambahan ke belakang.
- **`delete+insert`:** dbt menaruh baris baru di tabel sementara, menghapus baris `(customer_id, report_month)` yang sama, lalu memasukkan yang baru dalam satu transaksi. Dijalankan ulang hasilnya sama.

Test: satu baris per nasabah dan bulan, kunci tidak null, dan `assert_matches_view` yang memastikan model menghasilkan baris yang persis sama dengan view. Semua lolos.

Waktu di sample: build penuh ~7 detik, incremental ~1 detik (8.000 baris, bukan 28.000). Dengan 60 juta lebih transaksi selisihnya seharusnya lebih besar karena run malam tetap hanya membaca dua bulan, tapi saya tidak bisa mengujinya di ukuran itu.

## Contoh output

```
$ MSYS_NO_PATHCONV=1 docker compose exec postgres psql -U postgres -d negarabank -f /sql/demo.sql
== (d) Scorecard: satu nasabah dari bulan ke bulan
 customer_id | report_month | total_balance | balance_change_mom_pct | txn_count_total | txn_count_debit | credit_utilization_ratio | probability_of_default | risk_flag 
-------------+--------------+---------------+------------------------+-----------------+-----------------+--------------------------+------------------------+-----------
 CUST0000002 | 2025-08-01   |  -61419257.65 |                        |              46 |               3 |                  -2.6506 |                 0.3255 | t
 CUST0000002 | 2025-09-01   |  -54423368.08 |                  11.39 |              52 |              10 |                  -2.3487 |                 0.3255 | t
 CUST0000002 | 2025-10-01   |  -48794345.87 |                  10.34 |              46 |              11 |                  -2.1058 |                 0.3255 | t
 CUST0000002 | 2025-11-01   |  -25142843.39 |                  48.47 |              49 |               8 |                  -1.0851 |                 0.3255 | t
 CUST0000002 | 2025-12-01   |  -24064813.79 |                   4.29 |              41 |               7 |                  -1.0385 |                 0.3255 | t
 CUST0000002 | 2026-01-01   |   -8069217.00 |                  66.47 |              49 |               6 |                  -0.3482 |                 0.3255 | t
 CUST0000002 | 2026-02-01   |          0.00 |                 100.00 |              27 |               5 |                   0.0000 |                 0.3255 | t
(7 rows)

== (d) Jumlah baris nasabah-bulan yang kena tiap aturan
 flagged | utilisation_gt_80 | pd_gt_0_3 | balance_drop_gt_30 
---------+-------------------+-----------+--------------------
   13539 |              1681 |      8358 |               5988
(1 row)

== (e) Alert fraud per jenis
      alert_type      | alerts | customers 
----------------------+--------+-----------
 VELOCITY_5_IN_1H     |     38 |        37
 AMOUNT_GT_3X_30D_AVG |    280 |       235
 MULTI_CITY_SAME_DAY  |  12706 |      2701
(3 rows)

== (e) Contoh alert beserta buktinya
 customer_id |    alert_type    | alert_date |                  jsonb_pretty                  
-------------+------------------+------------+------------------------------------------------
 CUST0003050 | VELOCITY_5_IN_1H | 2025-08-01 | {                                             +
             |                  |            |     "threshold": 5,                           +
             |                  |            |     "last_flagged_at": "2025-08-01T21:07:56", +
             |                  |            |     "first_flagged_at": "2025-08-01T21:07:56",+
             |                  |            |     "max_txn_in_1_hour": 5                    +
             |                  |            | }
(1 row)

== (f) Model incremental hasil dbt (jalankan: docker compose run --rm dbt build)
 report_month | count 
--------------+-------
 2025-08-01   |  4000
 2025-09-01   |  4000
 2025-10-01   |  4000
 2025-11-01   |  4000
 2025-12-01   |  4000
 2026-01-01   |  4000
 2026-02-01   |  4000
(7 rows)
```

Cara membacanya:
- **(d)** CUST0000002 ter-flag setiap bulan karena PD-nya 0,3255 (di atas 0,3). Saldonya negatif karena data sample hanya punya saldo saat ini, jadi saldo bulan-bulan sebelumnya dihitung mundur: saldo sekarang dikurangi transaksi bulan-bulan sesudahnya. Saldo sekarang nasabah ini 0, padahal ia punya banyak transaksi uang masuk, sehingga hasil hitung mundurnya negatif (sampai -61 juta di Agustus). Ini masalah data sample, bukan query.
- **(d)** 13.539 dari 28.000 baris nasabah-bulan ter-flag. Satu baris bisa kena lebih dari satu aturan, jadi jumlah tiga kolom aturan lebih dari 13.539.
- **(e)** Satu alert per nasabah, aturan, dan hari. Contoh alert menunjukkan 5 transaksi dalam satu jam pada 1 Agustus.
- **(f)** Tabel dbt berisi 7 bulan, masing-masing 4.000 nasabah, baris yang sama dengan view.
