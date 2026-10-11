# Airflow, menjalankan alur Q1 di lokal


```bash
cd airflow
cp .env.example .env     # isi password, Fernet key, dan DATASET_DIR (folder CSV hasil unzip)
docker compose up airflow-init
docker compose up -d --build 
```

## Mencoba alurnya

Aktifkan (unpause) kedua DAG di UI: `silver_account_snapshot_daily_local` dan `credit_scoring_daily_local`.

DAG snapshot punya parameter `snapshot_date` dengan default `2026-02-13`, karena data sample hanya berisi 2025-08-01 sampai 2026-02-14. Jadi tombol **Trigger DAG** biasa langsung memproses 2026-02-13. Untuk tanggal lain, pilih **Trigger DAG w/ config** dan ubah `snapshot_date`.

Lewat terminal:

```bash
docker compose exec airflow-scheduler airflow dags trigger silver_account_snapshot_daily_local -c '{"snapshot_date": "2026-02-13"}'
docker compose exec airflow-scheduler airflow dags trigger silver_account_snapshot_daily_local -c '{"snapshot_date": "2026-02-14"}'
```

(Di Git Bash Windows, tambahkan `MSYS_NO_PATHCONV=1` di depan kalau ada path yang berubah.)

Yang terjadi:
- **2026-02-13:** snapshot sukses, dataset diperbarui, dan `credit_scoring_daily_local` mulai sendiri.
- **2026-02-14:** dihentikan pengecekan saldo (data sample tidak cocok), gagal tanpa retry, menulis satu baris ke `logs/alerts.log`, dan credit scoring tidak mulai.

Jalankan 2026-02-13 dulu, karena 2026-02-14 butuh snapshot hari sebelumnya untuk saldo awal.

Melihat output Parquet (disimpan di Docker volume `silver_data`, bukan di folder laptop):

```bash
docker compose exec airflow-scheduler ls /opt/airflow/silver/account_snapshots
```

## Catatan

DAG di `dags/` adalah versi lokal dari yang ada di `q1/dags/`. Job Databricks diganti pemanggilan script PySpark, dan Slack diganti `logs/alerts.log`. Sensor Oracle dihilangkan karena sample bersifat statis.

Output memakai named volume, bukan folder bind mount, karena di Docker Desktop Windows user Airflow sering tidak bisa menulis ke folder Windows (Spark gagal dengan `Mkdirs failed to create file:/opt/airflow/silver/...`).

**Kalau muncul `PermissionError` atau `Mkdirs failed`:** ulangi dari awal supaya `airflow-init` mengatur ulang pemilik folder:

```bash
docker compose down -v      # -v juga menghapus volume output dan database Airflow
rm -rf logs
docker compose up airflow-init
docker compose up -d
```
