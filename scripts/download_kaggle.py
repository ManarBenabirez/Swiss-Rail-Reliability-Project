"""
Download our copy of the raw data from Kaggle and put it in the project structure:

  data/raw/istdaten/2026-09-28_IstDaten.csv      (one file per day)
  data/raw/service_points/service_points.csv

Files are recognised by their columns, not by their names, so the layout of the
Kaggle dataset does not matter (folders and zipped files are fine).

  python scripts/download_kaggle.py                     # download from Kaggle
  python scripts/download_kaggle.py --zip archive.zip   # use a zip you downloaded yourself

The dataset is public, so no Kaggle account is needed. If Kaggle asks for one,
set KAGGLE_USERNAME and KAGGLE_KEY (from kaggle.com > Settings > API).
"""
import argparse
import io
import os
import re
import shutil
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import requests

KAGGLE_DATASET = "meflahyousra/swiss-rail-ist-daten-v2-28-sep-to-11-oct-2026"
URL = f"https://www.kaggle.com/api/v1/datasets/download/{KAGGLE_DATASET}"

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("DATA_DIR", ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
ISTDATEN_DIR = RAW_DIR / "istdaten"
STATIONS_FILE = RAW_DIR / "service_points" / "service_points.csv"
ZIP_FILE = RAW_DIR / "kaggle_download.zip"


def download(target):
    auth = None
    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        auth = (os.environ["KAGGLE_USERNAME"], os.environ["KAGGLE_KEY"])
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".part")
    print(f"Downloading {URL}")
    with requests.get(URL, auth=auth, stream=True, timeout=120) as r:
        if r.status_code in (401, 403):
            sys.exit("Kaggle refused the download. Check that the dataset is public, or set "
                     "KAGGLE_USERNAME and KAGGLE_KEY.")
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        done = 0
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
                done += len(chunk)
                if total:
                    print(f"\r  {done/1e9:.2f} / {total/1e9:.2f} GB", end="", flush=True)
                else:
                    print(f"\r  {done/1e9:.2f} GB", end="", flush=True)
    print()
    if total and done != total:
        sys.exit(f"Incomplete download: {done} of {total} bytes. Run the script again.")
    tmp.rename(target)


def kind_of(first_line):
    """'istdaten', 'stations' or None, from the column names."""
    cols = [c.strip().strip('"') for c in re.split(r"[;,]", first_line.lstrip("﻿"))]
    if "BETRIEBSTAG" in cols and "BPUIC" in cols:
        return "istdaten"
    if "number" in cols and "designationOfficial" in cols:
        return "stations"
    return None


def day_of(name, open_file):
    """Operating day from the file name, or else from the first data row."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", name)
    if m:
        return m.group(1)
    with open_file() as f:
        lines = io.TextIOWrapper(f, encoding="utf-8-sig", errors="replace")
        lines.readline()
        first = lines.readline()
    day = first.split(";")[0].strip().strip('"')
    return datetime.strptime(day, "%d.%m.%Y").strftime("%Y-%m-%d")


def extract(zip_source, found):
    """Copy every Ist-Daten / station CSV from a zip (also zips inside the zip)."""
    with zipfile.ZipFile(zip_source) as z:
        for info in z.infolist():
            name = info.filename
            if info.is_dir():
                continue
            if name.lower().endswith(".zip"):
                with z.open(info) as inner:
                    extract(io.BytesIO(inner.read()), found)
                continue
            if not name.lower().endswith(".csv"):
                continue
            with z.open(info) as f:
                first_line = f.readline().decode("utf-8-sig", errors="replace")
            kind = kind_of(first_line)
            if kind == "istdaten":
                target = ISTDATEN_DIR / f"{day_of(name, lambda: z.open(info))}_IstDaten.csv"
            elif kind == "stations":
                target = STATIONS_FILE
            else:
                print(f"  - skipped (unknown columns): {name}")
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with z.open(info) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst, 16 * 1024 * 1024)
            found[kind].append(target)
            print(f"  + {name}  ->  {target.relative_to(DATA_DIR)}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--zip", type=Path, help="use this zip instead of downloading it")
    p.add_argument("--keep-zip", action="store_true", help="keep the downloaded zip")
    args = p.parse_args()

    zip_path = args.zip
    if zip_path is None:
        zip_path = ZIP_FILE
        if not zip_path.exists():
            download(zip_path)
    if not zip_path.exists():
        sys.exit(f"Zip not found: {zip_path}")

    print(f"Extracting {zip_path.name}")
    found = {"istdaten": [], "stations": []}
    extract(zip_path, found)

    if args.zip is None and not args.keep_zip:
        zip_path.unlink()

    days = sorted(f.name[:10] for f in found["istdaten"])
    print(f"\nIst-Daten: {len(days)} day(s)" + (f", from {days[0]} to {days[-1]}" if days else ""))
    print(f"Station list: {'found' if found['stations'] else 'NOT FOUND'}")
    if not days or not found["stations"]:
        sys.exit("Some data is missing: check the Kaggle dataset.")
    total = sum(f.stat().st_size for f in RAW_DIR.rglob("*.csv"))
    print(f"Raw data on disk: {total/1e9:.2f} GB in {RAW_DIR}")


if __name__ == "__main__":
    main()