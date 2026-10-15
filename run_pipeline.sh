#!/usr/bin/env bash
# Runs the whole Part 1 pipeline. Stops at the first error.
# Download the data first: python scripts/download_data.py
set -euo pipefail
cd "$(dirname "$0")/src"

echo "=== 1/6 Ingestion ===";                python 01_ingest.py
echo "=== 2/6 Check the ingestion ===";      python 01b_check_ingestion.py
echo "=== 3/6 Profiling ===";                python 02a_profile.py
echo "=== 4/6 Cleaning ===";                 python 02_clean.py
echo "=== 5/6 Check stations + join ===";    python 02b_check_stations_join.py
echo "=== 6/6 Analysis ===";                 python 03_analyze.py
echo "Done. Results are in output/"