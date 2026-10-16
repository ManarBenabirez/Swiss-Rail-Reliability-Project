#!/usr/bin/env bash
# Runs the whole Part 1 pipeline. Stops at the first error.
# If there is no raw data yet, it is first downloaded from our Kaggle copy.
set -euo pipefail
cd "$(dirname "$0")/src"

if ! ls ../data/raw/istdaten/*.csv >/dev/null 2>&1; then
  echo "=== 0/6 No raw data yet: download it from Kaggle ==="
  python ../scripts/download_kaggle.py
fi

echo "=== 1/6 Ingestion ===";                python 01_ingest.py
echo "=== 2/6 Check the ingestion ===";      python 01b_check_ingestion.py
echo "=== 3/6 Profiling ===";                python 02a_profile.py
echo "=== 4/6 Cleaning ===";                 python 02_clean.py
echo "=== 5/6 Check stations + join ===";    python 02b_check_stations_join.py
echo "=== 6/6 Analysis ===";                 python 03_analyze.py
echo "Done. Results are in output/"