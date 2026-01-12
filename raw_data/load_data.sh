#!/bin/bash

# Wait until PostgreSQL is ready (with timeout)
MAX_RETRIES=30
RETRY_COUNT=0
until pg_isready -h db -U admin; do
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "ERROR: Database connection timeout after $MAX_RETRIES attempts"
    exit 1
  fi
  echo "Waiting for database connection... (attempt $RETRY_COUNT/$MAX_RETRIES)"
  sleep 2
done

# Load each CSV file into its corresponding table
for file in /raw_data/*.csv; do
  table_name=$(basename "$file" .csv)
  echo "Loading data from $file into table $table_name..."
  psql -h db -U admin -d postgres -c "\COPY $table_name FROM '$file' DELIMITER ',' CSV HEADER;"
done

echo "Data loading completed."
