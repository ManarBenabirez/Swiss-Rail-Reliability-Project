"""
Step 2 - Cleaning

Input:  the typed Parquet tables made by 01_ingest.py (stops_typed, stations_typed).

Stations: keep one row per station (the latest version) and remove impossible coordinates.
Train stops: keep Swiss train stops only, remove duplicates and wrong rows, add the canton
with a join on the station ID, and compute the arrival delay (only when it was measured).
A stop without a measured delay is kept, because it is still a real train at a station.

Output: data/parquet/stations_clean, data/parquet/stops_clean,
        output/cleaning_report.csv (rows before / after each step, and why).
"""
import csv

from pyspark import StorageLevel
from pyspark.sql import Window, functions as F

from spark_session import OUTPUT_DIR, PARQUET_DIR, get_spark

# Cleaning settings
TRAIN_PRODUCT = "zug"                           # "Zug" = train
SWISS_ID_MIN, SWISS_ID_MAX = 8500000, 8599999   # Swiss station IDs start with 85
MIN_DELAY, MAX_DELAY = -10.0, 180.0             # minutes, outside = wrong data
ON_TIME_LIMIT = 3.0                             # on time = less than 3 min late
CH_LON, CH_LAT = (5.9, 10.5), (45.8, 47.9)      # rectangle around Switzerland

report = []


def step(table, name, before, after, reason, modified=0):
    report.append((table, name, before, after, before - after, modified, reason))
    print(f"  {name:<45} {before:>11,} -> {after:>11,}"
          f"  removed {before - after:>9,}  modified {modified:>9,}")


def clean_stations(spark):
    print("\n=== Stations ===")
    st = spark.read.parquet(str(PARQUET_DIR / "stations_typed"))
    n0 = st.count()
    step("stations", "0. after ingestion", n0, n0, "")

    # A station can have several versions: keep the most recent one
    w = Window.partitionBy("station_id").orderBy(F.desc("valid_to"), F.desc("valid_from"))
    st = (st.filter(F.col("station_id").isNotNull())
            .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn"))
    n1 = st.count()
    step("stations", "1. one row per station_id (latest version)", n0, n1,
         "historical versions would duplicate train rows in the join")

    # Swiss station with coordinates outside Switzerland -> empty coordinates
    swiss = F.col("station_id").between(SWISS_ID_MIN, SWISS_ID_MAX)
    bad = swiss & F.col("lon").isNotNull() & ~(
        F.col("lon").between(*CH_LON) & F.col("lat").between(*CH_LAT))
    n_bad = st.filter(bad).count()
    st = (st.withColumn("lon", F.when(bad, None).otherwise(F.col("lon")))
            .withColumn("lat", F.when(bad, None).otherwise(F.col("lat"))))
    step("stations", "2. impossible coordinates -> NULL", n1, n1,
         "Swiss station with coordinates outside Switzerland", modified=n_bad)

    st.write.mode("overwrite").parquet(str(PARQUET_DIR / "stations_clean"))
    return st


def clean_stops(spark, stations):
    print("\n=== Train stops (Ist-Daten) ===")
    df = spark.read.parquet(str(PARQUET_DIR / "stops_typed"))
    n0 = df.count()
    step("stops", "0. after ingestion", n0, n0, "all transport types, 15 days")

    # Trains only ('Bus' / 'BUS' are written differently, so we compare in lower case)
    df = df.filter(F.lower(F.trim("product")) == TRAIN_PRODUCT).persist(StorageLevel.MEMORY_AND_DISK)
    n1 = df.count()
    step("stops", "1. keep trains only", n0, n1,
         "question is about the rail network; buses/trams/boats removed")

    # Identical rows
    df = df.dropDuplicates()
    n2 = df.count()
    step("stops", "2. remove duplicate rows", n1, n2, "fully identical rows")

    # Trains passing without stopping
    df = df.filter(~F.coalesce(F.col("is_pass_through"), F.lit(False)))
    n3 = df.count()
    step("stops", "3. remove pass-through (no stop)", n2, n3, "train did not stop")

    # Planned times must be on the operating day or the day after
    def on_day(c):
        d = F.to_date(c)
        return F.col(c).isNull() | ((d >= F.col("operating_day")) & (d <= F.date_add("operating_day", 1)))
    df = df.filter(on_day("planned_arrival") & on_day("planned_departure"))
    n4 = df.count()
    step("stops", "4. remove timestamps outside the day", n3, n4,
         "planned time not on the operating day or the day after")

    # Some IDs have 9 digits = station (7 digits) + platform (2 digits),
    # e.g. 850626402 -> 8506264. The station list uses 7 digits.
    nine = F.col("station_id") > 9999999
    n_nine = df.filter(nine).count()
    df = (df.withColumn("station_id_raw", F.col("station_id"))
            .withColumn("station_id", F.when(nine, (F.col("station_id") / 100).cast("int"))
                                       .otherwise(F.col("station_id"))))
    step("stops", "5. normalise 9-digit station IDs to 7 digits", n4, n4,
         "9-digit IDs = station number + 2-digit platform code; divided by 100 to match "
         "the station list", modified=n_nine)

    # Swiss stations only
    df = df.filter(F.col("station_id").between(SWISS_ID_MIN, SWISS_ID_MAX))
    n5 = df.count()
    step("stops", "6. keep Swiss stations only", n4, n5,
         "foreign stations of international trains (Hungary, Italy, Germany...)")

    # Join with the station list to get the canton (broadcast: the list is small)
    st = stations.select("station_id", F.col("station_name").alias("station_name_official"),
                         "canton", "canton_name", "lon", "lat")
    df = df.join(F.broadcast(st), "station_id", "left")
    n_unmatched = df.filter(F.col("canton").isNull()).count()
    df = df.withColumn("canton", F.coalesce("canton", F.lit("UNKNOWN")))
    step("stops", "7. join with stations (canton)", n5, n5,
         "rows with no canton after the join -> canton UNKNOWN", modified=n_unmatched)

    # Delay only when the arrival was really measured (REAL) and not cancelled.
    raw_delay = (F.unix_timestamp("actual_arrival") - F.unix_timestamp("planned_arrival")) / 60.0
    measured = (F.col("arrival_status") == "REAL") & F.col("planned_arrival").isNotNull() \
        & F.col("actual_arrival").isNotNull() & ~F.coalesce(F.col("is_cancelled"), F.lit(False))
    df = df.withColumn("arrival_delay_min", F.when(measured, raw_delay))
    n_measured = df.filter(F.col("arrival_delay_min").isNotNull()).count()
    step("stops", "8. compute arrival delay (measured only)", n5, n5,
         "delay only for REAL, non-cancelled arrivals; others NULL (forecasts, unknown, "
         "first stops, cancelled)", modified=n5 - n_measured)

    # Impossible delays -> empty (the stop is kept)
    outlier = (F.col("arrival_delay_min") < MIN_DELAY) | (F.col("arrival_delay_min") > MAX_DELAY)
    n_out = df.filter(outlier).count()
    df = df.withColumn("arrival_delay_min", F.when(outlier, None).otherwise(F.col("arrival_delay_min")))
    step("stops", "9. delay outliers -> NULL", n5, n5,
         f"delay < {MIN_DELAY} or > {MAX_DELAY} min (min/max were about +/-24 h: day-shift errors)",
         modified=n_out)

    # Extra columns for the analysis
    event_time = F.coalesce("planned_arrival", "planned_departure")
    df = (df.withColumn("hour", F.hour(event_time))
            .withColumn("weekday", F.dayofweek("operating_day"))          # 1 = Sunday
            .withColumn("weekday_name", F.date_format("operating_day", "EEEE"))
            .withColumn("is_cancelled", F.coalesce("is_cancelled", F.lit(False)))
            .withColumn("is_punctual",
                        F.when(F.col("arrival_delay_min").isNotNull(),
                               F.col("arrival_delay_min") < ON_TIME_LIMIT)))

    df.write.mode("overwrite").partitionBy("operating_day").parquet(str(PARQUET_DIR / "stops_clean"))
    print(f"\n  clean table: {n5:,} rows, of which {n_measured - n_out:,} with a usable delay")
    print(f"  written to {PARQUET_DIR / 'stops_clean'}")


def main():
    spark = get_spark("02_clean")
    stations = clean_stations(spark)
    clean_stops(spark, stations)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_DIR / "cleaning_report.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["table", "step", "rows_before", "rows_after", "removed", "modified", "reason"])
        w.writerows(report)
    print(f"\nReport written to {OUTPUT_DIR / 'cleaning_report.csv'}")
    spark.stop()


if __name__ == "__main__":
    main()
