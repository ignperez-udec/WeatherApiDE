#!/bin/bash

set -e

echo "Waiting for databases to be ready..."

until pg_isready -h "$SILVER_DB_HOST" -p 5432 -U "$SILVER_DB_USER"; do
  echo "${SILVER_DB_NAME} is not ready yet. Waiting..."
  sleep 5
done

until pg_isready -h "$GOLD_DB_HOST" -p 5432 -U "$GOLD_DB_USER"; do
  echo "${GOLD_DB_NAME} is not ready yet. Waiting..."
  sleep 5
done

echo "Databases are ready."

echo "Cleaning init flag"
echo "" > src/logs/init_done

echo "Running init.py script..."
python -u src/init.py

tail -f /dev/null