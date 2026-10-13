"""
Step 2a - Profiling: look at the data before deciding how to clean it.

Nothing is changed here. We measure the problems (missing values, duplicates,
wrong IDs, extreme delays, times on the wrong day) so that each cleaning rule
in 02_clean.py is based on real numbers.

Output: printed on screen + one CSV per table in output/profile/

Run:  docker compose run --rm pipeline python src/02a_profile.py
"""
import csv

from pyspark import StorageLevel
from pyspark.sql import functions as F

from spark_session import OUTPUT_DIR, PARQUET_DIR, get_spark

PROFILE_DIR = OUTPUT_DIR / "profile"


def save(name, header, rows):
    """Print a small table and save it as CSV."""
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    with open(PROFILE_DIR / f"{name}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"\n--- {name} ---")
    widths = [max(len(str(x)) for x in col) for col in zip(header, *rows)] if rows else [len(h) for h in header]
    for r in [header] + list(rows):
        print("  " + "  ".join(str(x).ljust(w) for x, w in zip(r, widths)))


def counts(df, col, name, limit=30):
    rows = [(r[col], r["count"]) for r in
            df.groupBy(col).count().orderBy(F.desc("count")).limit(limit).collect()]
    save(name, [col, "rows"], rows)


def main():
    spark = get_spark("02a_profile")
    stops = spark.read.parquet(str(PARQUET_DIR / "stops_typed"))
    stations = spark.read.parquet(str(PARQUET_DIR / "stations_typed"))

    # 1. Rows per day
    total = stops.count()
    save("01_rows_per_day", ["operating_day", "rows"],
         [(str(r["operating_day"]), r["count"]) for r in
          stops.groupBy("operating_day").count().orderBy("operating_day").collect()] +
         [("TOTAL", total)])

    # 2. Transport types: how many rows are trains?
    counts(stops, "product", "02_products")

    trains = stops.filter(F.lower("product") == "zug").persist(StorageLevel.MEMORY_AND_DISK)
    n_trains = trains.count()
    print(f"\nTrain rows: {n_trains:,} of {total:,} ({100 * n_trains / total:.1f}%)")

    # 3. Missing values (trains only, like all checks below)
    agg = trains.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in trains.columns]).first()
    save("03_missing_values_trains", ["column", "missing", "missing_pct"],
         [(c, agg[c], f"{100 * agg[c] / n_trains:.2f}%") for c in trains.columns])

    # 4. Is the real time measured (REAL) or only a forecast?
    counts(trains, "arrival_status", "04_arrival_status_trains")
    counts(trains, "departure_status", "04b_departure_status_trains")

    # 5. Cancelled, passing without stopping, extra trips, first/last stops
    flags = trains.agg(
        F.sum(F.col("is_cancelled").cast("int")).alias("cancelled"),
        F.sum(F.col("is_pass_through").cast("int")).alias("pass_through"),
        F.sum(F.col("is_extra_trip").cast("int")).alias("extra_trip"),
        F.sum((F.col("planned_arrival").isNull() & F.col("planned_departure").isNotNull()).cast("int")).alias("first_stop_no_arrival"),
        F.sum((F.col("planned_departure").isNull() & F.col("planned_arrival").isNotNull()).cast("int")).alias("last_stop_no_departure"),
    ).first()
    save("05_flags_trains", ["flag", "rows"], [(k, flags[k]) for k in flags.asDict()])

    # 6. Duplicates: same day, trip, station and planned times
    key = ["operating_day", "trip_id", "station_id", "planned_arrival", "planned_departure"]
    n_distinct_key = trains.select(*key).distinct().count()
    n_distinct_rows = trains.distinct().count()
    save("06_duplicates_trains", ["check", "rows"], [
        ("train rows", n_trains),
        ("distinct on key (day, trip, station, planned times)", n_distinct_key),
        ("duplicates on key", n_trains - n_distinct_key),
        ("fully identical duplicate rows", n_trains - n_distinct_rows),
    ])

    # 7. Delays: how they are spread out, and extreme values
    delay = (trains
             .filter(F.col("planned_arrival").isNotNull() & F.col("actual_arrival").isNotNull())
             .withColumn("delay_min",
                         (F.unix_timestamp("actual_arrival") - F.unix_timestamp("planned_arrival")) / 60.0))
    q = delay.agg(
        F.count("*").alias("n"), F.min("delay_min").alias("min"), F.max("delay_min").alias("max"),
        F.avg("delay_min").alias("mean"),
        F.percentile_approx("delay_min", [0.01, 0.05, 0.5, 0.95, 0.99, 0.999], 10000).alias("p"),
    ).first()
    p = q["p"]
    save("07_delay_distribution_trains", ["stat", "minutes"], [
        ("rows with planned+actual arrival", q["n"]), ("min", round(q["min"], 2)),
        ("p1", p[0]), ("p5", p[1]), ("median", p[2]), ("mean", round(q["mean"], 2)),
        ("p95", p[3]), ("p99", p[4]), ("p99.9", p[5]), ("max", round(q["max"], 2)),
    ])
    buckets = delay.agg(
        F.sum((F.col("delay_min") < -10).cast("int")).alias("early > 10 min"),
        F.sum(((F.col("delay_min") >= -10) & (F.col("delay_min") < 0)).cast("int")).alias("early 0-10 min"),
        F.sum(((F.col("delay_min") >= 0) & (F.col("delay_min") < 3)).cast("int")).alias("on time (< 3 min late)"),
        F.sum(((F.col("delay_min") >= 3) & (F.col("delay_min") < 60)).cast("int")).alias("late 3-60 min"),
        F.sum(((F.col("delay_min") >= 60) & (F.col("delay_min") < 180)).cast("int")).alias("late 1-3 h"),
        F.sum((F.col("delay_min") >= 180).cast("int")).alias("late > 3 h"),
    ).first()
    save("07b_delay_buckets_trains", ["bucket", "rows"], [(k, buckets[k]) for k in buckets.asDict()])

    # 8. Planned times on the wrong day
    arr_day = F.to_date("planned_arrival")
    out_of_range = trains.filter(
        F.col("planned_arrival").isNotNull() &
        ((arr_day < F.col("operating_day")) | (arr_day > F.date_add("operating_day", 1)))).count()
    save("08_timestamps_out_of_range", ["check", "rows"],
         [("planned arrival not on operating day or day+1", out_of_range)])

    # 9. Station list: several rows per station? do train stations match it?
    n_st = stations.count()
    n_st_ids = stations.select("station_id").distinct().count()
    current = stations.filter(F.col("valid_to") >= F.current_date())
    n_current_ids = current.select("station_id").distinct().count()
    n_current_rows = current.count()
    station_ids = stations.select("station_id").distinct()
    train_ids = trains.select("station_id").distinct()
    n_train_stations = train_ids.count()
    unmatched_ids = train_ids.join(station_ids, "station_id", "left_anti")
    n_unmatched_ids = unmatched_ids.count()
    n_unmatched_rows = trains.join(station_ids, "station_id", "left_anti").count()
    country = (trains.select("station_id").join(
        stations.dropDuplicates(["station_id"]).select("station_id", "country"), "station_id", "left")
        .groupBy("country").count().orderBy(F.desc("count")).collect())
    save("09_stations", ["check", "value"], [
        ("station list: rows", n_st),
        ("station list: distinct station_id", n_st_ids),
        ("station list: rows still valid today", n_current_rows),
        ("station list: distinct station_id still valid today", n_current_ids),
        ("train data: distinct stations", n_train_stations),
        ("train stations with NO match in the station list", n_unmatched_ids),
        ("train rows with NO match in the station list", n_unmatched_rows),
    ])
    save("09b_train_rows_by_country", ["country", "rows"], [(r["country"], r["count"]) for r in country])

    # 10. First look at our importance measures (trains per day, lines)
    imp = (trains.filter(~F.coalesce("is_cancelled", F.lit(False)) & ~F.coalesce("is_pass_through", F.lit(False)))
           .groupBy("station_id", "station_name")
           .agg((F.countDistinct("operating_day", "trip_id") / F.countDistinct("operating_day")).alias("trains_per_day"),
                F.countDistinct("line_text").alias("lines"))
           .orderBy(F.desc("trains_per_day")).limit(15).collect())
    save("10_importance_preview_top15", ["station_id", "station", "trains_per_day", "lines"],
         [(r["station_id"], r["station_name"], round(r["trains_per_day"], 1), r["lines"]) for r in imp])

    print(f"\nAll tables saved in {PROFILE_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()
