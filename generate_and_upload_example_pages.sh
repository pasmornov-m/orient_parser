#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: ./generate_and_upload.sh <num_pages>"
  exit 1
fi

NUM_PAGES=$1

echo "Генерация $NUM_PAGES страниц..."
bash ./create_example_pages/generate_results.sh "$NUM_PAGES"

echo "Загрузка страниц в MinIO..."
python3 ./create_example_pages/upload_results_to_minio.py

# выполнить
# chmod +x generate_and_upload.sh
