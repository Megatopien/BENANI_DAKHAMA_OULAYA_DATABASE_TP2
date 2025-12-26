#!/bin/bash
set -e

echo "=== 🧠 Checking FastAPI health ==="
curl -s http://127.0.0.1:8000/health

echo ""
echo "=== 🐘 Checking Postgres queries ==="
docker compose exec -T postgres psql -U app -d shop -c "SELECT * FROM orders LIMIT 5;"
docker compose exec -T postgres psql -U app -d shop -c "SELECT now();"

echo ""
echo "=== ⚙️ Running ETL script ==="
docker compose exec -T app python /app/elt.py || { echo "ETL failed!"; exit 1; }

echo ""
echo "=== ✅ All tests passed successfully! ==="
