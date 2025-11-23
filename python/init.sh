#!/bin/bash

set -e

echo "Waiting for weather_db to be ready..."

until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER"; do
  echo "weather_db is not ready yet. Waiting..."
  sleep 5
done

echo "weather_db is ready."

echo "Running init.py script..."
python -u src/init.py

tail -f /dev/null