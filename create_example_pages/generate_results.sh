#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <number_of_pages>"
  exit 1
fi

NUM_PAGES=$1

if ! [[ "$NUM_PAGES" =~ ^[0-9]+$ ]] || [ "$NUM_PAGES" -le 0 ]; then
  echo "Error: argument must be a positive integer"
  exit 1
fi

START_DATE="2010-04-01"
END_DATE="2010-12-01"

START_TS=$(date -d "$START_DATE" +%s)
END_TS=$(date -d "$END_DATE" +%s)
TOTAL_DAYS=$(( (END_TS - START_TS) / 86400 ))
STEP=$(( TOTAL_DAYS / NUM_PAGES ))

mkdir -p "/create_example_pages/results_pages/"

for (( i=0; i<NUM_PAGES; i++ ))
do
  PAGE_DATE=$(date -d "$START_DATE +$((i * STEP)) days" +%Y-%m-%d)
  echo "Generating page for $PAGE_DATE..."
  python3 ./create_example_pages/generate_single_page.py "$PAGE_DATE"
done

echo "Генерация завершена."\

# Дать права на выолнение
# chmod +x generate_results.sh