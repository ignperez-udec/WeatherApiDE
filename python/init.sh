#!/bin/bash

set -e

echo "Waiting for database to be ready..."

until pg_isready -h "$DWH_DB_HOST" -p 5432 -U "$DWH_DB_USER"; do
  echo "${DWH_DB_NAME} is not ready yet. Waiting..."
  sleep 5
done

echo "Database is ready."

echo "Cleaning init flag"
echo "" > src/logs/init_done

echo "Running init.py script..."
python -u src/init.py

tail -f /dev/null