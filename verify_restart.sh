#!/usr/bin/env bash
# 기존 ~/lakehouse 환경에서 실행. named volume은 삭제하지 않는다.
set -euo pipefail
cd "${LAKEHOUSE_DIR:-$HOME/lakehouse}"
ls catalog scripts
query='SELECT count(*) AS row_count, sum(amount) AS total_amount FROM iceberg.lakehouse.orders'
before=$(docker compose exec -T trino trino --execute "$query")
printf 'before=%s\n' "$before"
start=$SECONDS
docker compose down
docker compose up -d
deadline=$((SECONDS + 300))
until docker compose exec -T trino trino --execute 'SELECT 1' >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
        echo 'Trino readiness timeout (300s)' >&2
        docker compose ps
        exit 1
    fi
    sleep 2
done
printf 'Trino ready; down/up elapsed=%ss\n' "$((SECONDS - start))"
after=$(docker compose exec -T trino trino --execute "$query")
printf 'after=%s\n' "$after"
test "$before" = "$after"
echo 'PASS: count and sum preserved'
