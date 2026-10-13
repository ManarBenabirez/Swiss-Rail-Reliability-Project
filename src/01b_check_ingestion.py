"""
Step 1b - Check the ingestion: is the Parquet table a faithful copy of the raw CSV?

Each test prints PASS or FAIL, and the script stops with an error if one fails.
  1. No rows lost (per day)
  2. All days are there
  3. Every column has the right type
  4. No values that could not be converted
  5. Key columns are never empty
  6. Values look plausible (times on the right day, 7-digit station IDs)
  7. A few raw lines compared one by one with Parquet
  8. Station table: same number of rows as the raw file

Run:  docker compose run --rm pipeline python src/01b_check_ingestion.py
"""
import csv
import sys

from pyspark.sql import functions as F

from spark_session import OUTPUT_DIR, PARQUET_DIR, RAW_DIR, get_spark

SEP = ";"
EXPECTED_TYPES = {
    "operating_day": "date", "trip_id": "string", "operator_id": "string",
    "operator_abbr": "string", "operator_name": "string", "product": "string",
    "line_id": "string", "line_text": "string", "circulation_id": "string",
    "vehicle_type": "string", "is_extra_trip": "boolean", "is_cancelled": "boolean",
    "station_id": "int", "station_name": "string", "planned_arrival": "timestamp",
    "actual_arrival": "timestamp", "arrival_status": "string",
    "planned_departure": "timestamp", "actual_departure": "timestamp",
    "departure_status": "string", "is_pass_through": "boolean", "sloid": "string",
}
# Columns compared in test 7: raw name -> (Parquet name, kind)
SPOT_COLUMNS = {
    "FAHRT_BEZEICHNER": ("trip_id", "text"), "BPUIC": ("station_id", "int"),
    "HALTESTELLEN_NAME": ("station_name", "text"), "PRODUKT_ID": ("product", "text"),
    "ANKUNFTSZEIT": ("planned_arrival", "ts"), "AN_PROGNOSE": ("actual_arrival", "ts"),
    "AN_PROGNOSE_STATUS": ("arrival_status", "text"),
    "ABFAHRTSZEIT": ("planned_departure", "ts"), "AB_PROGNOSE": ("actual_departure", "ts"),
}
SPOT_LINES_PER_FILE = 3

results = []


def check(name, ok, detail=""):
    results.append((name, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))


def raw_line_count(path):
    """Number of data lines in a CSV (no header, no empty lines)."""
    n = 0
    with open(path, "rb") as f:
        next(f, None)
        for line in f:
            if line.strip():
                n += 1
    return n


def day_of_file(path):
    return path.name[:10]           # 2026-09-28_IstDaten.csv -> 2026-09-28


def main():
    spark = get_spark("01b_check_ingestion")
    stops = spark.read.parquet(str(PARQUET_DIR / "stops_typed"))
    files = sorted((RAW_DIR / "istdaten").glob("*.csv"))

    # 1. No rows lost
    print("\n1. Rows: raw CSV vs Parquet, per day (counting raw lines takes a moment)")
    parquet_per_day = {str(r["operating_day"]): r["count"]
                       for r in stops.groupBy("operating_day").count().collect()}
    total_raw = 0
    for f in files:
        n_raw = raw_line_count(f)
        total_raw += n_raw
        n_pq = parquet_per_day.get(day_of_file(f), 0)
        check(f"{f.name}: {n_raw:,} raw lines = {n_pq:,} Parquet rows", n_raw == n_pq)
    n_total = stops.count()
    check(f"TOTAL: {total_raw:,} raw lines = {n_total:,} Parquet rows", total_raw == n_total)

    # 2. All days
    print("\n2. Days")
    file_days = {day_of_file(f) for f in files}
    pq_days = set(parquet_per_day)
    check(f"{len(file_days)} files -> {len(pq_days)} days in Parquet, same set", file_days == pq_days,
          f"only in files: {sorted(file_days - pq_days)}, only in Parquet: {sorted(pq_days - file_days)}"
          if file_days != pq_days else "")

    # 3. Types
    print("\n3. Schema")
    actual = {f.name: f.dataType.simpleString() for f in stops.schema.fields}
    for col, typ in EXPECTED_TYPES.items():
        check(f"column {col} is {typ}", actual.get(col) == typ,
              f"found {actual.get(col)}" if actual.get(col) != typ else "")
    extra = set(actual) - set(EXPECTED_TYPES)
    check("no unexpected columns", not extra, f"extra: {sorted(extra)}" if extra else "")

    # 4. Conversion failures (counted by 01_ingest.py)
    print("\n4. Conversion failures (from the ingestion report)")
    report = OUTPUT_DIR / "ingestion_report.csv"
    if report.exists():
        with open(report, encoding="utf-8") as fh:
            rows = [r for r in csv.DictReader(fh) if r["check"].startswith("not convertible")]
        bad = [(r["table"], r["column"], r["count"]) for r in rows if int(r["count"]) > 0]
        check(f"{len(rows)} typed columns checked, values that could not be converted: "
              f"{sum(int(b[2]) for b in bad)}", not bad, f"{bad}" if bad else "")
    else:
        check("ingestion report exists", False, "run src/01_ingest.py first")

    # 5. Key columns
    print("\n5. Key columns")
    nulls = stops.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c)
                        for c in ["operating_day", "trip_id", "station_id", "product"]]).first()
    for c in ["operating_day", "trip_id", "station_id"]:
        check(f"{c}: {nulls[c] or 0:,} empty values", (nulls[c] or 0) == 0)
    # Empty product values are already in the raw file (not our mistake): only shown as INFO.
    print(f"  [INFO] product: {nulls['product'] or 0:,} empty values in the source "
          f"(not a train, removed in cleaning)")

    # 6. Plausible values
    print("\n6. Plausible values")
    def off_day(c):
        d = F.to_date(c)
        return F.col(c).isNotNull() & ((d < F.col("operating_day")) | (d > F.date_add("operating_day", 1)))
    stats = stops.agg(
        F.min("operating_day").alias("first_day"), F.max("operating_day").alias("last_day"),
        F.sum(off_day("planned_arrival").cast("int")).alias("arr_off"),
        F.sum(off_day("planned_departure").cast("int")).alias("dep_off"),
        F.sum((F.col("station_id") < 1000000).cast("int")).alias("short_ids"),
    ).first()
    check(f"days from {stats['first_day']} to {stats['last_day']}", True)
    off = (stats["arr_off"] or 0) + (stats["dep_off"] or 0)
    share = 100 * off / n_total if n_total else 0
    check(f"planned times on the operating day or the day after: {off:,} exceptions ({share:.3f}%)",
          share < 0.1, "handled in cleaning" if off else "")
    check(f"station IDs with fewer than 7 digits: {stats['short_ids'] or 0:,}", (stats["short_ids"] or 0) == 0)

    # 7. Compare a few raw lines with Parquet, column by column
    print(f"\n7. Spot check: first {SPOT_LINES_PER_FILE} raw lines of each file vs Parquet")
    fmt = "dd.MM.yyyy HH:mm:ss"
    n_ok = n_checked = 0
    for f in files:
        with open(f, encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh, delimiter=SEP)
            samples = [next(reader) for _ in range(SPOT_LINES_PER_FILE)]
        for raw in samples:
            if not raw["BPUIC"].strip().isdigit():
                continue
            cond = ((F.col("operating_day") == F.lit(day_of_file(f)).cast("date"))
                    & (F.col("trip_id") == raw["FAHRT_BEZEICHNER"].strip())
                    & (F.col("station_id") == int(raw["BPUIC"])))
            sel = [F.date_format(new, fmt).alias(new) if kind == "ts" else F.col(new).cast("string").alias(new)
                   for new, kind in SPOT_COLUMNS.values()]
            found = stops.filter(cond).select(*sel).collect()
            n_checked += 1
            if not found:
                check(f"{f.name}: row {raw['FAHRT_BEZEICHNER']} / {raw['BPUIC']} found in Parquet", False)
                continue
            mismatches = []
            for rawcol, (new, kind) in SPOT_COLUMNS.items():
                rv = raw[rawcol].strip()
                pv = found[0][new]
                if kind == "ts" and rv:
                    rv = rv if rv.count(":") == 2 else rv + ":00"     # planned times have no seconds
                if (rv or None) != pv:
                    mismatches.append(f"{rawcol}: raw '{rv}' vs parquet '{pv}'")
            if mismatches:
                check(f"{f.name}: row {raw['FAHRT_BEZEICHNER']} identical", False, "; ".join(mismatches))
            else:
                n_ok += 1
    check(f"{n_ok} of {n_checked} sampled rows identical in raw CSV and Parquet", n_ok == n_checked)

    # 8. Station table
    print("\n8. Station table")
    stations = spark.read.parquet(str(PARQUET_DIR / "stations_typed"))
    st_file = next((RAW_DIR / "service_points").glob("*.csv"))
    n_st_raw = raw_line_count(st_file)
    n_st = stations.count()
    check(f"{n_st_raw:,} raw lines = {n_st:,} Parquet rows", n_st_raw == n_st)
    n_st_null = stations.filter(F.col("station_id").isNull()).count()
    check(f"station_id: {n_st_null:,} empty values", n_st_null == 0)

    # Summary
    failed = [n for n, ok in results if not ok]
    print(f"\n=== {len(results) - len(failed)} PASS, {len(failed)} FAIL ===")
    for n in failed:
        print(f"  FAIL: {n}")
    spark.stop()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
