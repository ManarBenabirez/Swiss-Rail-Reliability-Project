#!/usr/bin/env bash
# Runs the whole Part 1 pipeline. Stops at the first error.
# Download the data first: python scripts/download_data.py
set -euo pipefail
cd "$(dirname "$0")/src"

echo "=== 1/3 Ingestion ===";  python 01_ingest.py
echo "=== 2/3 Cleaning ===";   python 02_clean.py
echo "=== 3/3 Analysis ===";   python 03_analyze.py
echo "Done. Results are in output/"
