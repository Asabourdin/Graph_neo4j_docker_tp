#!/bin/bash

set -e

# Determine if running inside container or on host
MODE=${1:-host}

if [ "$MODE" == "container" ]; then
    HOST="app"
    PG_HOST="postgres"
else
    HOST="127.0.0.1"
    PG_HOST="localhost"
fi

echo "Running checks in $MODE mode..."
echo ""

# 1. Check FastAPI health
echo "› FastAPI health check"
HTTP_CODE=$(curl -s -o /tmp/health.json -w "%{http_code}" http://${HOST}:8000/health)

if [ "$HTTP_CODE" == "200" ]; then
    echo "✔ FastAPI health OK"
    cat /tmp/health.json
    echo ""
else
    echo "FastAPI health check failed (HTTP $HTTP_CODE)"
    exit 1
fi

# 2. Check Postgres - orders query
echo ""
echo "› Postgres: SELECT * FROM orders LIMIT 5;"
if [ "$MODE" == "container" ]; then
    PGPASSWORD=$POSTGRES_PASSWORD psql -h $PG_HOST -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT * FROM orders LIMIT 5;"
else
    docker compose exec -T postgres psql -U app -d shop -c "SELECT * FROM orders LIMIT 5;"
fi

if [ $? -eq 0 ]; then
    echo "Orders query OK"
else
    echo "Orders query failed"
    exit 1
fi

# 3. Check Postgres - now() query
echo ""
echo "› Postgres: SELECT now();"
if [ "$MODE" == "container" ]; then
    PGPASSWORD=$POSTGRES_PASSWORD psql -h $PG_HOST -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT now();"
else
    docker compose exec -T postgres psql -U app -d shop -c "SELECT now();"
fi

if [ $? -eq 0 ]; then
    echo "now() query OK"
else
    echo "now() query failed"
    exit 1
fi

# 4. Run ETL
echo ""
echo "› ETL: python /work/app/etl.py"
if [ "$MODE" == "container" ]; then
    if cd /work/app && python etl.py; then
        echo " ETL output OK"
    else
        echo " ETL failed"
        exit 1
    fi
else
    if docker compose exec -T app python /work/app/etl.py; then
        echo " ETL output OK"
    else
        echo " ETL failed"
        exit 1
    fi
fi

echo ""
echo "All checks passed !"