# Q1: Snapshot akun harian

- `account_snapshot.py`: job PySpark hasil rewrite
- `tests/test_account_snapshot.py`: unit test di Spark lokal (6 lulus)
- `dags/account_snapshot_dag.py`: DAG Airflow produksi (job Databricks, alert Slack)

## (a) Masalah

| # | Masalah | Dampak | Perbaikan |
|---|---|---|---|
| 1 | Password ditulis di kode | Siapa pun yang punya akses repo bisa membacanya; ganti password, job rusak | Ambil dari secret manager saat job jalan |
| 2 | Pembacaan JDBC kedua tanpa username/password | Pembacaan bisa gagal, kemungkinan salah satu penyebab gagal "kadang-kadang" | Satu reader JDBC bersama dengan credential |
| 3 | Seluruh tabel dibaca lewat satu koneksi, fetch size bawaan Oracle 10 baris | Sangat lambat (kemungkinan besar sumber 4 jam), dan membebani DB core banking | Filter di SQL, `fetchsize` 10.000, baca paralel, baca dari replica |
| 4 | `txn_date >= "2026-01-01"` hard-coded | Data yang dibaca bertambah tiap malam; "hari ini" sebenarnya setahun berjalan | Tanggal bisnis jadi parameter, baca hari itu saja |
| 5 | FAILED, PENDING, REVERSED ikut dijumlahkan | Sekitar 8% transaksi dihitung sebagai uang yang tidak pernah bergerak | Hanya COMPLETED yang mengubah saldo |
| 6 | Join dulu baru group by (dengan `balance` di group by) | Shuffle besar; akun tanpa transaksi dapat NULL, bukan 0 | Agregasi transaksi dulu, lalu join, NULL diisi 0 |
| 7 | Tabel dibaca saat sistem inti masih menulis | Saldo dan transaksi tidak cocok, hasil berubah tiap run | Jalan setelah batch akhir hari selesai, baca dari replica |
| 8 | `overwrite` seluruh folder | Riwayat terhapus tiap malam (buruk untuk audit OJK); gagal di tengah, tabel kosong | Partisi per `snapshot_date` dan hanya ganti hari itu |
| 9 | `agg` gaya dict | Nama kolomnya jadi `sum(amount)` dan `count(txn_id)` | Alias eksplisit dan schema output yang tetap |
| 10 | Tanpa pengecekan data, log, atau penanganan error | Data salah masuk dashboard tanpa ada yang sadar | Pengecekan sebelum menulis, log, kode keluar yang jelas |
| 11 | Tanpa dedup dan kolom audit | Baris duplikat terhitung dua kali; tidak tahu run mana yang menulis | `dropDuplicates(txn_id)`, tambah `_run_id` dan `_processed_at` |

## (b) Rewrite

Yang dilakukan job dan alasannya:

- **Hanya membaca satu hari.** Saldo awal diambil dari snapshot kemarin, jadi job tidak perlu seluruh riwayat. Waktu jalan tetap sama setiap malam. Karena hari ini bergantung pada kemarin, DAG jalan satu hari per waktu (`max_active_runs=1`).
- **Agregasi transaksi dulu, baru join ke akun.** Join dilakukan di satu baris per akun, bukan di setiap transaksi, jadi jauh lebih ringan.
- **Hanya transaksi COMPLETED yang mengubah saldo.** Yang gagal tetap dihitung di kolom terpisah, karena lonjakan kegagalan berguna untuk dilihat.
- **Menulis Delta, dipartisi per `snapshot_date`, dengan `replaceWhere`.** Hanya partisi hari itu yang diganti. Kalau job jalan dua kali untuk tanggal yang sama, hasilnya sama (idempotent), dan hari-hari lama tetap ada untuk time travel. Di lokal saya menulis Parquet dengan partisi yang sama karena Delta tidak terpasang.
- **Credential dari secret manager** (Databricks secrets atau AWS Secrets Manager). Environment variable hanya untuk run lokal.
- **Pengecekan data sebelum menulis.** Kalau satu gagal, tidak ada yang ditulis:
  - satu baris per akun
  - jumlah baris dan jumlah transaksi sama dengan sumber
  - jumlah baris tidak berubah lebih dari 5% dari kemarin
  - saldo awal + transaksi hari ini = saldo akhir, untuk minimal 99,9% akun

  Ambangnya nilai awal; saya akan menyesuaikannya setelah beberapa minggu berjalan dengan data nyata.
- **Dua kode keluar.** 1 berarti error teknis (retry mungkin membantu). 2 berarti data gagal pengecekan (retry tidak membantu).

Yang dites: hanya transaksi completed yang dijumlahkan, rekonsiliasi lolos dan gagal dengan benar, akun yang hilang terdeteksi, run ulang tanggal yang sama memberi hasil sama tanpa menyentuh hari lain, dan pengecekan yang gagal tidak menulis apa pun.

Di data sample, 2026-02-13 terpublish dan 2026-02-14 diblokir karena hanya 35,7% akun yang cocok. Itu masalah di sample: `accounts.balance` tidak pernah berubah, bahkan untuk akun yang punya transaksi. Di dunia nyata saya akan melaporkannya ke tim sumber, bukan mempublishnya. Belum dites di sini: penulisan Delta, koneksi Oracle, dan pengambilan secret.

## (c) Orkestrasi

```
00:30 WIB  tunggu akhir hari Oracle -> jalankan job snapshot -> cek partisi dan publish
                                                                   |  Dataset diperbarui
                                                                   v
                                                          DAG credit scoring
```

- **Credit scoring dipicu oleh Airflow Dataset.** Saat task snapshot selesai, dataset ditandai sudah diperbarui dan DAG credit scoring langsung mulai, jauh di bawah 30 menit. Saya lebih memilih ini daripada jadwal tetap, karena jam tetap bisa menunggu terlalu lama atau malah mulai sebelum data siap.
- **3 percobaan, lalu alert.** `retries=2` dengan jeda 5 menit yang makin lama. Alert Slack baru dikirim setelah percobaan terakhir, jadi tim tidak dihubungi untuk error yang sembuh sendiri saat retry.
- **Kegagalan pengecekan data tidak di-retry**, karena menjalankan data buruk yang sama lagi memberi hasil yang sama.

Versi lokal di `../airflow/` menjalankan alur yang sama dengan docker-compose.

## Kode: sebelum dan sesudah

```python
# credential
.option("user", "etl_user").option("password", "etl_pass123")                      # sebelum
dbutils.secrets.get(scope, "oracle-etl-user"), dbutils.secrets.get(scope, "oracle-etl-password")  # sesudah
```

```python
# membaca: sebelum, seluruh tabel, satu koneksi, difilter di Spark
txns = spark.read.format("jdbc").option("dbtable", "TRANSACTIONS").load()
today_txns = txns.filter(txns.txn_date >= "2026-01-01")

# sesudah: satu hari difilter di Oracle, 16 bacaan paralel, 10.000 baris per fetch
query = f"""SELECT txn_id, account_id, txn_date, txn_type, amount, channel, status,
                   MOD(ORA_HASH(txn_id), 16) AS part_key
            FROM TRANSACTIONS
            WHERE txn_date >= TO_TIMESTAMP('{d} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
              AND txn_date <  TO_TIMESTAMP('{d} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') + INTERVAL '1' DAY"""
txns = (spark.read.format("jdbc").option("url", replica_url).option("dbtable", f"({query}) q")
        .option("fetchsize", 10000).option("partitionColumn", "part_key")
        .option("lowerBound", 0).option("upperBound", 16).option("numPartitions", 16).load())
```

`part_key` memberi setiap baris nomor 0 sampai 15, supaya Spark bisa memecah pembacaan jadi 16 query paralel.

```python
# agregasi: sebelum, join dulu, transaksi gagal ikut
snapshot = accounts.join(today_txns, "account_id", "left") \
    .groupBy("account_id", "customer_id", "account_type", "balance") \
    .agg({"amount": "sum", "txn_id": "count"})

# sesudah: agregasi dulu, hanya COMPLETED, lalu join
completed = F.col("status") == "COMPLETED"
txn_agg = (txns.dropDuplicates(["txn_id"]).groupBy("account_id").agg(
    F.count("*").alias("txn_count"),
    F.sum((F.col("status") == "FAILED").cast("long")).alias("txn_count_failed"),
    F.sum(F.when(completed, F.col("amount")).otherwise(0)).alias("net_amount")))
snap = (accounts.join(txn_agg, "account_id", "left")
        .withColumn("net_amount", F.coalesce("net_amount", F.lit(0))))
```

```python
# write: sebelum, menimpa semuanya
snapshot.write.mode("overwrite").parquet("s3://negarabank-silver/account_snapshots/")

# sesudah: hanya mengganti hari itu
(df.write.format("delta").mode("overwrite")
   .option("replaceWhere", f"snapshot_date = '{d}'")
   .partitionBy("snapshot_date").save("s3://negarabank-silver/account_snapshots/"))
```
