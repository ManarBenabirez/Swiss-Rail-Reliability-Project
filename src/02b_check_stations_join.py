"""
Step 2b - Check the second dataset (station list) and the join

Input: stations_typed, stations_clean and stops_clean (made by 01_ingest.py and 02_clean.py).

  A. Station list before cleaning: how many versions per station, missing values, countries.
  B. Station list after cleaning: there must be exactly one row per station.
  C. The join: it must not add rows, at least 99% of train stops must get a canton,
     and we list the stations without a canton and the number of stops per canton.

Output: PASS / FAIL / INFO on screen, tables in output/profile/.
The script stops with an error if a check fails.
"""
import csv
import sys

from pyspark.sql import functions as F

from spark_session import OUTPUT_DIR, PARQUET_DIR, get_spark

PROFILE_DIR = OUTPUT_DIR / "profile"
SWISS_MIN, SWISS_MAX = 8500000, 8599999   # Swiss station IDs start with 85
results = []


def check(name, ok, detail=""):
    results.append((name, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))


def info(text):
    print(f"  [INFO] {text}")


def save(name, header, rows, show=15):
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    with open(PROFILE_DIR / f"{name}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"\n  --- {name} ---")
    shown = rows[:show]
    widths = [max(len(str(x)) for x in col) for col in zip(header, *shown)] if shown else [len(h) for h in header]
    for r in [header] + list(shown):
        print("    " + "  ".join(str(x).ljust(w) for x, w in zip(r, widths)))
    if len(rows) > show:
        print(f"    ... {len(rows) - show} more rows in {name}.csv")


def main():
    spark = get_spark("02b_check_stations_join")
    st = spark.read.parquet(str(PARQUET_DIR / "stations_typed"))
    st_clean = spark.read.parquet(str(PARQUET_DIR / "stations_clean"))
    stops = spark.read.parquet(str(PARQUET_DIR / "stops_clean"))
    SWISS = F.col("station_id").between(SWISS_MIN, SWISS_MAX)

    # A. Station list before cleaning
    print("\nA. Station list (Service Points) before cleaning")
    n = st.count()
    n_ids = st.select("station_id").distinct().count()
    info(f"{n:,} rows for {n_ids:,} distinct stations")

    versions = (st.groupBy("station_id").count()
                  .groupBy(F.col("count").alias("versions")).count()
                  .orderBy("versions").collect())
    save("stations_01_versions_per_station", ["versions per station", "stations"],
         [(r["versions"], r["count"]) for r in versions])

    cols = [c for c in st.columns]
    nulls = st.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    save("stations_02_missing_values", ["column", "missing", "missing_pct"],
         [(c, nulls[c], f"{100 * nulls[c] / n:.2f}%") for c in cols], show=20)

    countries = st_clean.groupBy("country").count().orderBy(F.desc("count")).collect()
    save("stations_03_countries", ["country", "stations"], [(r["country"], r["count"]) for r in countries])

    swiss_no_canton = st_clean.filter(SWISS & F.col("canton").isNull()).count()
    swiss_total = st_clean.filter(SWISS).count()
    info(f"Swiss station IDs (85xxxxx): {swiss_total:,}, of which {swiss_no_canton:,} without canton")

    no_coord = st_clean.filter(F.col("lon").isNull() | F.col("lat").isNull()).count()
    info(f"stations without coordinates after cleaning: {no_coord:,}")

    # B. One row per station after cleaning
    print("\nB. Station list after cleaning")
    n_clean = st_clean.count()
    n_clean_ids = st_clean.select("station_id").distinct().count()
    check(f"one row per station: {n_clean:,} rows = {n_clean_ids:,} distinct IDs", n_clean == n_clean_ids)
    check(f"same stations as before cleaning: {n_clean_ids:,} = {n_ids:,}", n_clean_ids == n_ids)

    # C. The join between train stops and stations
    print("\nC. Join: train stops x stations")
    n_stops = stops.count()
    key = ["operating_day", "trip_id", "station_id", "planned_arrival", "planned_departure"]
    n_key = stops.select(*key).distinct().count()
    check(f"join did not duplicate rows: {n_stops:,} rows = {n_key:,} distinct stops", n_stops == n_key)

    matched = stops.filter(F.col("canton") != "UNKNOWN").count()
    pct = 100 * matched / n_stops if n_stops else 0
    check(f"stops with a canton: {matched:,} of {n_stops:,} ({pct:.2f}%)", pct >= 99.0,
          "at least 99% expected")

    n_train_stations = stops.select("station_id").distinct().count()
    n_matched_stations = stops.filter(F.col("canton") != "UNKNOWN").select("station_id").distinct().count()
    info(f"train stations: {n_train_stations:,}, with a canton: {n_matched_stations:,}")

    unmatched = (stops.filter(F.col("canton") == "UNKNOWN")
                 .groupBy("station_id", "station_name").count()
                 .orderBy(F.desc("count")).collect())
    save("join_01_stations_without_canton", ["station_id", "station", "train stops"],
         [(r["station_id"], r["station_name"], r["count"]) for r in unmatched])

    in_list = (stops.filter(F.col("canton") == "UNKNOWN").select("station_id").distinct()
               .join(st_clean.select("station_id", "country"), "station_id", "left"))
    reasons = (in_list.withColumn("reason",
                   F.when(F.col("country").isNull(), "not in the station list")
                    .when(F.col("country") != "CH", F.concat(F.lit("in the list, country "), F.col("country")))
                    .otherwise("in the list, Swiss, but no canton"))
               .groupBy("reason").count().collect())
    save("join_02_why_no_canton", ["reason", "stations"], [(r["reason"], r["count"]) for r in reasons])

    per_canton = (stops.groupBy("canton").agg(F.count("*").alias("stops"),
                                              F.countDistinct("station_id").alias("stations"))
                  .orderBy(F.desc("stops")).collect())
    save("join_03_stops_per_canton", ["canton", "train stops", "stations"],
         [(r["canton"], r["stops"], r["stations"]) for r in per_canton], show=27)

    # Summary
    failed = [n for n, ok in results if not ok]
    print(f"\n=== {len(results) - len(failed)} PASS, {len(failed)} FAIL ===")
    for n in failed:
        print(f"  FAIL: {n}")
    print(f"Tables saved in {PROFILE_DIR}")
    spark.stop()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
