#!/bin/bash
set -euo pipefail
PSQL="psql -v ON_ERROR_STOP=1 -q -U postgres -d negarabank"
echo ">> ngeload CSV ke bronze.*"
sed 's#DATA#/data#g' /scripts/load_postgres.sql | $PSQL
for f in 00_setup 01_customer_health_scorecard 02_fraud_alerts; do
  echo ">> $f.sql"; $PSQL -f /sql/$f.sql
done
echo ">> selesai (jalankan 'docker compose run --rm dbt build' untuk model incremental)"
