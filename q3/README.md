# Q3: Arsitektur platform

Saya menjaga desainnya tetap sederhana: satu lakehouse (S3 + Delta Lake + Databricks) untuk batch, streaming, BI, dan ML, dengan Unity Catalog untuk akses dan lineage. Lebih sedikit alat berarti lebih sedikit tempat data nasabah disalin dan lebih sedikit tempat yang perlu dicek saat audit OJK. Ini juga masuk dalam budget.

## (g) Arsitektur

![architecture](architecture.png)

### Alur data di platform

Baca diagram dari kiri ke kanan. Ada empat jalur.

**1. Core banking (batch, tiap malam).**
1. AWS DMS membaca perubahan dari log Oracle sepanjang hari dan menulisnya ke bronze di S3 (`accounts`, `transactions`).
2. Jam 00:30 WIB Airflow menunggu batch akhir hari Oracle selesai, lalu menjalankan job Spark Q1.
3. Job Q1 membangun `silver.account_snapshots` untuk hari itu, menjalankan pengecekan data, dan hanya menulis kalau lolos.
4. Airflow menandai snapshot sudah siap. Itu memicu job credit scoring dan model dbt dari Q2, yang membangun tabel gold (scorecard, alert fraud).
5. Dashboard BI dan laporan OJK membaca tabel gold lewat Databricks SQL.

**2. Clickstream aplikasi mobile (streaming, sepanjang hari).**
1. Aplikasi mengirim event ke Kafka (MSK).
2. Spark Structured Streaming membacanya terus-menerus dan menulis event mentah ke bronze.
3. Stream kedua membersihkannya jadi `silver.app_events` (di-parse, dedup, tanggal WIB, data pribadi di-masking).
4. Stream ketiga membangun agregat per menit dan per jam di gold untuk dashboard real-time.
5. Untuk analisis, event di-join dengan snapshot Q1 berdasarkan `customer_id` dan tanggal.

**3. Zendesk, Google Ads, Braze (batch, harian).**
Airbyte (Zendesk, Google Ads) dan ekspor S3 bawaan Braze memasukkan data ke bronze. dbt membersihkannya ke silver dan gold, sama seperti data core banking.

**4. Credit scoring (ML).**
Job credit scoring membaca fitur yang dibangun dari silver dan gold, menulis skornya kembali sebagai tabel, dan skor itu dipakai scorecard Q2 (`probability_of_default`).

**Di semua jalur:** Unity Catalog mengatur siapa boleh membaca tabel mana dan melakukan masking NIK, telepon, dan email. Unity Catalog juga mencatat kolom mana yang dibaca dan ditulis setiap job dan query (lineage). Pengecekan data jalan sebelum data dipublish (pengecekan Q1, test dbt). Secret disimpan di Secrets Manager, dan setiap job jalan dengan IAM role-nya sendiri.

| Lapisan | Pilihan | Kenapa |
|---|---|---|
| Ingestion Oracle | AWS DMS dengan CDC, ke S3 (bronze) | Dikelola AWS, jadi tidak ada server tambahan yang harus dijalankan. CDC membaca perubahan dari log database, jauh lebih ringan untuk DB core banking dibanding baca penuh tiap malam seperti di Q1 |
| Clickstream | Amazon MSK (Kafka), dibaca Spark Structured Streaming | Aplikasi sudah mengirim event ke Kafka. MSK adalah Kafka yang dikelola AWS, dan Structured Streaming jalan di platform Databricks yang sama |
| Zendesk, Google Ads | Airbyte | Konektornya sudah tersedia; volumenya kecil |
| Braze | Ekspor Braze Currents ke S3 | Braze bisa langsung ekspor ke S3, tidak perlu konektor |
| Penyimpanan | Delta Lake di S3: bronze (mentah), silver (bersih), gold (tabel bisnis) | Delta mendukung overwrite per partisi (dipakai di Q1) dan time travel, jadi kita bisa menunjukkan ke OJK isi laporan pada tanggal tertentu |
| Pemrosesan | Job Databricks (Q1), Structured Streaming (clickstream), dbt di Databricks SQL (Q2) | Credit scoring sudah jalan di Databricks, jadi semuanya tetap di satu platform |
| Orkestrasi | Amazon MWAA (Airflow terkelola) | Mengatur job di dalam dan di luar Databricks (cek Oracle, Airbyte, dbt) |
| Serving | Databricks SQL untuk BI dan laporan OJK; feature table dan MLflow untuk credit scoring | Alat BI tersambung dengan koneksi SQL biasa; SQL serverless tidak memakan biaya saat tidak dipakai |
| Governance | Unity Catalog | Satu tempat untuk izin akses, masking NIK/telepon/email, dan lineage per kolom |
| Keamanan | Secrets Manager, IAM role per job, enkripsi KMS, koneksi privat ke Oracle | Praktik standar AWS untuk data perbankan |

Perkiraan budget per bulan (estimasi saya, belum dicek ke harga terbaru):

| Item | USD |
|---|---|
| Databricks (job, streaming, SQL) dan EC2-nya | 22.000 |
| MSK (Kafka) | 5.000 |
| DMS, Airbyte, MWAA | 4.000 |
| Penyimpanan S3 | 4.000 |
| Jaringan, monitoring, lisensi BI | 6.000 |
| Total | 41.000 |

Sisa sekitar 9.000 untuk pertumbuhan. Supaya biaya tetap rendah: instance spot untuk job batch, cluster job yang mati setelah setiap run, dan aturan lifecycle S3 untuk data lama.

## (h) Clickstream 50 ribu event per detik

**Dari Kafka sampai siap dianalisis:**
- **Topik Kafka** `app.events` dengan sekitar 48 partisi dan retensi 72 jam. Dengan ~1 KB per event, puncaknya ~50 MB/detik, dan sekitar 1 MB/detik per partisi masih nyaman, jadi ~48 partisi memberi ruang. Angka ini akan saya pastikan dengan load test. Key-nya `session_id`, supaya event dari satu sesi tetap berurutan.
- **Bronze:** Structured Streaming menulis event mentah ke Delta kira-kira setiap 30 detik. Tidak ada yang diubah di sini, jadi data selalu bisa diproses ulang.
- **Silver:** parse JSON, buang duplikat berdasarkan `event_id` (aplikasi mobile suka mengirim ulang event), ubah timestamp ke WIB dan tambah `event_date`, masking data pribadi.
- **Gold:** agregat per menit dan per jam, misalnya pengguna aktif dan funnel transfer.

**Join dengan data batch Q1:** kedua tabel memakai `customer_id` dan tanggal WIB yang sama. Untuk analisis saya join dengan `snapshot_date = event_date - 1`, karena snapshot suatu hari baru selesai setelah tengah malam. Memakai hari yang sama berarti memakai data yang belum ada saat event terjadi.

**Hot, warm, cold:**

| Tier | Umur | Lokasi | Dipakai untuk |
|---|---|---|---|
| Hot | 0 sampai 72 jam | Kafka dan tabel gold per menit | Dashboard real-time |
| Warm | sampai 90 hari | `silver.app_events` di S3 Standard | Analisis dan fitur ML |
| Cold | lebih lama | S3 Glacier lewat aturan lifecycle | Audit dan proses ulang; hanya agregat gold harian dan bulanan yang tetap warm |

## (i) Lineage per kolom

Tujuannya: untuk angka mana pun di dashboard OJK, bisa ditunjukkan kolom sumbernya dan job yang membuatnya.

- **Di dalam Databricks (bronze ke silver ke gold ke Databricks SQL):** Unity Catalog mencatat lineage per kolom secara otomatis untuk job dan query SQL yang membaca dan menulis tabel Unity Catalog. Tidak perlu memasang apa pun lagi. Ini mencakup job Q1 dan model dbt di Q2.
- **Oracle ke bronze:** DMS menyalin tabel Oracle satu ke satu, jadi pemetaannya sederhana. Saya catat di komentar tabel dan kolom bronze (misalnya "source: CORE.ACCOUNTS.BALANCE").
- **Dashboard BI:** dashboard yang query lewat Databricks SQL muncul sebagai turunan dari tabel gold. Untuk alat BI yang tidak tercatat otomatis, saya dokumentasikan dashboard mana memakai tabel gold mana.

**Hubungannya dengan pipeline Q1:**
- Jalankan job di cluster Unity Catalog dan baca/tulis tabel Unity Catalog, supaya lineage tercatat.
- Kodenya sudah membuat setiap kolom output dengan nama eksplisit (tanpa `agg({...})`), jadi lineage jelas menunjukkan, misalnya, `closing_balance` berasal dari `ACCOUNTS.BALANCE` dan `net_amount` dari `TRANSACTIONS.AMOUNT`.
- Setiap baris punya `_run_id` dan `_processed_at`, jadi satu baris bisa ditelusuri ke run Airflow yang menulisnya.
- Karena tabelnya Delta, kita bisa query versi persis yang dipakai untuk sebuah laporan (`VERSION AS OF`).
