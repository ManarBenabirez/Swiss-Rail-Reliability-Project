"""
Download the raw data for Part 1.

  1. Ist-Daten v2: one CSV per day with the planned and real times of every stop.
  2. Service Points v2: the official list of stations (ID, name, canton, coordinates).

Source: opentransportdata.swiss (open use, the source must be cited).

By default we download a fixed period, so everyone gets the same data.

  python scripts/download_data.py                  # our project period
  python scripts/download_data.py --list           # see which days are available
  python scripts/download_data.py --days 1         # quick test with the last day
  python scripts/download_data.py --start 2026-10-01 --end 2026-10-03
"""
import argparse
import csv
import os
import re
import shutil
import sys
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path

import requests

# Our period: 15 days, so every weekday appears at least twice.
PROJECT_START = date(2026, 9, 28)
PROJECT_END = date(2026, 10, 12)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("DATA_DIR", ROOT / "data"))
ISTDATEN_DIR = DATA_DIR / "raw" / "istdaten"
STATIONS_DIR = DATA_DIR / "raw" / "service_points"
STATIONS_FILE = STATIONS_DIR / "service_points.csv"

# Two copies of the same catalogue: if the first one fails, we try the second.
CKAN_APIS = [
    "https://data.opentransportdata.swiss/api/3/action/package_show",
    "https://ckan.opendata.swiss/api/3/action/package_show",
]
ISTDATEN_DATASET = "ist-daten-v2"
STATIONS_DATASET = "service-point-v2"

HEADERS = {"User-Agent": "DISS-DPA student project (Swiss rail reliability)"}
DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})_istdaten", re.IGNORECASE)
MAX_MB_PER_DAY = 650


def list_resources(dataset_id, api):
    r = requests.get(api, params={"id": dataset_id}, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()["result"]["resources"]


def istdaten_days():
    """Return {day: download link} for every daily file in the catalogue."""
    for api in CKAN_APIS:
        try:
            days = {}
            for res in list_resources(ISTDATEN_DATASET, api):
                m = DATE_IN_NAME.search(f"{res.get('name', '')} {res.get('url', '')}")
                if m and res.get("url"):
                    days[date.fromisoformat(m.group(1))] = res["url"]
            if days:
                return dict(sorted(days.items()))
            print(f"  ! catalogue {api} answered but listed no daily files")
        except Exception as e:
            print(f"  ! catalogue {api} failed: {e}")
    sys.exit("No daily Ist-Daten files found (check the network or use --list).")


def station_resources():
    for api in CKAN_APIS:
        try:
            res = [r for r in list_resources(STATIONS_DATASET, api)
                if "service-point" in f"{r.get('name', '')} {r.get('url', '')}".lower()
                and r.get("url", "").lower().endswith((".csv", ".zip"))]
            if res:
                return res
            print(f"  ! catalogue {api} listed no station file")
        except Exception as e:
            print(f"  ! catalogue {api} failed: {e}")
    sys.exit("No station file found in the catalogue.")


def download(url, target: Path, retries=3):
    """Download a file. Skip it if we already have it, and try again if it fails."""
    if target.exists() and target.stat().st_size > 0:
        print(f"  = already downloaded: {target.name}")
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")   # renamed only when complete
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, headers=HEADERS, stream=True, timeout=120) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", 0))
                done = 0
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
                        done += len(chunk)
                        if total:
                            print(f"\r  ↓ {target.name}: {done/1e6:,.0f} / {total/1e6:,.0f} MB",
                                end="", flush=True)
            print()
            if total and done != total:
                raise IOError(f"incomplete: {done} of {total} bytes")
            tmp.rename(target)
            return target
        except Exception as e:
            print(f"\n  ! attempt {attempt}/{retries} failed for {target.name}: {e}")
            time.sleep(5 * attempt)
    sys.exit(f"Giving up on {url}")


def unzip_if_needed(path: Path):
    """Some files are zipped: if so, keep only the CSV inside."""
    with open(path, "rb") as f:
        if f.read(2) != b"PK":
            return
    print(f"  {path.name} is a zip: extracting the CSV")
    with zipfile.ZipFile(path) as z:
        csvs = [i for i in z.infolist() if i.filename.lower().endswith(".csv")]
        if not csvs:
            sys.exit(f"No CSV inside {path}")
        biggest = max(csvs, key=lambda i: i.file_size)
        data = z.read(biggest)
    path.write_bytes(data)


def check_days(chosen):
    """Warn if a day is missing or if some weekdays are not covered."""
    expected = {chosen[0] + timedelta(n) for n in range((chosen[-1] - chosen[0]).days + 1)}
    missing = sorted(expected - set(chosen))
    if missing:
        print("  ! missing days in the catalogue:", ", ".join(map(str, missing)))
    weekdays = sorted({d.weekday() for d in chosen})
    names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    print("  weekdays covered:", ", ".join(names[w] for w in weekdays))
    if len(weekdays) < 7:
        print("  ! not every weekday is covered")


def check_disk(n_days):
    need_gb = n_days * MAX_MB_PER_DAY * 2 / 1000          # raw CSV + Parquet
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    free_gb = shutil.disk_usage(DATA_DIR).free / 1e9
    print(f"  disk: ~{need_gb:.1f} GB needed, {free_gb:.1f} GB free")
    if free_gb < need_gb:
        sys.exit("Not enough free disk space.")


def check_stations():
    """A station can have several rows (old versions). We will fix that in the cleaning."""
    with open(STATIONS_FILE, encoding="utf-8-sig", errors="replace") as f:
        sample = f.readline()
        sep = ";" if sample.count(";") > sample.count(",") else ","
        f.seek(0)
        reader = csv.DictReader(f, delimiter=sep)
        id_col = next((c for c in ("number", "NUMBER", "Number") if c in (reader.fieldnames or [])), None)
        if id_col is None:
            print(f"  ! no 'number' column in the station file. Columns: {reader.fieldnames}")
            return
        rows, ids = 0, set()
        for row in reader:
            rows += 1
            ids.add(row[id_col])
    print(f"  stations: {rows:,} rows, {len(ids):,} distinct IDs")
    if rows != len(ids):
        print("  ! several rows per station: the cleaning keeps only one.")


def download_stations():
    res = station_resources()
    # Take the "full" list first (it also has old stations), then the newest file.
    res.sort(key=lambda r: ("full" in r["url"].lower(), r.get("last_modified") or ""), reverse=True)
    url = res[0]["url"]
    print(f"Station list: {url}")
    download(url, STATIONS_FILE)
    unzip_if_needed(STATIONS_FILE)
    check_stations()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--list", action="store_true", help="only list the available days")
    p.add_argument("--days", type=int, help="quick test: the N most recent days")
    p.add_argument("--start", type=date.fromisoformat, help="first day, YYYY-MM-DD")
    p.add_argument("--end", type=date.fromisoformat, help="last day, YYYY-MM-DD")
    p.add_argument("--skip-stations", action="store_true")
    args = p.parse_args()

    days = istdaten_days()
    print(f"Catalogue: {len(days)} daily files, from {min(days)} to {max(days)}")
    if args.list:
        for d in days:
            print(" ", d, d.strftime("%a"))
        return

    if args.days:
        chosen = list(days)[-args.days:]
    else:
        start = args.start or PROJECT_START
        end = args.end or PROJECT_END
        chosen = [d for d in days if start <= d <= end]
        if not chosen:
            sys.exit(f"No file between {start} and {end} in the catalogue (use --list).")

    print(f"Downloading {len(chosen)} day(s): {chosen[0]} -> {chosen[-1]}")
    check_days(chosen)
    check_disk(len(chosen))

    for d in chosen:
        target = ISTDATEN_DIR / f"{d.isoformat()}_IstDaten.csv"
        download(days[d], target)
        unzip_if_needed(target)

    if not args.skip_stations:
        download_stations()

    total = sum(f.stat().st_size for f in (DATA_DIR / "raw").rglob("*.csv"))
    print(f"\nDone. Raw data on disk: {total/1e9:.2f} GB in {DATA_DIR / 'raw'}")


if __name__ == "__main__":
    main()