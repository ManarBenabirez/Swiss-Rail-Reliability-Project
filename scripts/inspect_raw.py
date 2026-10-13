"""
First look at the raw CSV files, before any Spark code.
For each file: size, column names and the first 3 lines.

  python scripts/inspect_raw.py
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = Path(os.environ.get("DATA_DIR", ROOT / "data")) / "raw"

files = sorted(RAW.rglob("*.csv"))
if not files:
    raise SystemExit(f"No CSV in {RAW}. Run scripts/download_data.py first.")

seen_headers = set()
for f in files:
    with open(f, encoding="utf-8-sig", errors="replace") as fh:
        header = fh.readline().rstrip("\n")
        lines = [fh.readline().rstrip("\n") for _ in range(3)]
    print(f"\n=== {f.relative_to(RAW)}  ({f.stat().st_size/1e6:,.0f} MB)")
    if header in seen_headers:          # daily files all have the same columns
        print("  (same header as a previous file)")
        continue
    seen_headers.add(header)
    print("  Columns:")
    for i, c in enumerate(header.split(";")):
        print(f"    {i:2d}. {c}")
    print("  First lines:")
    for line in lines:
        print("    " + line[:250])