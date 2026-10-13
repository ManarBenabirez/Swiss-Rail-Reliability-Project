"""
Step 1 - Ingestion: raw CSV files -> typed Parquet tables.

Nothing is removed here (that is the cleaning step). We:
  1. check that every file has the columns we expect,
  2. read the CSVs with our own schema (everything as text first),
  3. convert each column to its real type; a value that cannot be converted
     becomes NULL and we count it,
  4. give the columns English names,
  5. write data/parquet/stops_typed (one folder per day) and data/parquet/stations_typed,
  6. write output/ingestion_report.csv (rows read + values that could not be converted).

Run:  docker compose run --rm pipeline python src/01_ingest.py
"""
import csv
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark_session import OUTPUT_DIR, PARQUET_DIR, RAW_DIR, get_spark

SEP = ";"

# Ist-Daten: the 22 columns in file order -> (new name, type)
ISTDATEN = {
    "BETRIEBSTAG":         ("operating_day",     "date"),
    "FAHRT_BEZEICHNER":    ("trip_id",           "string"),
    "BETREIBER_ID":        ("operator_id",       "string"),
    "BETREIBER_ABK":       ("operator_abbr",     "string"),     # SBB, BLS, ...
    "BETREIBER_NAME":      ("operator_name",     "string"),
    "PRODUKT_ID":          ("product",           "string"),     # Zug = train, Bus, Tram, ...
    "LINIEN_ID":           ("line_id",           "string"),
    "LINIEN_TEXT":         ("line_text",         "string"),     # IC1, S3, ...
    "UMLAUF_ID":           ("circulation_id",    "string"),
    "VERKEHRSMITTEL_TEXT": ("vehicle_type",      "string"),
    "ZUSATZFAHRT_TF":      ("is_extra_trip",     "boolean"),
    "FAELLT_AUS_TF":       ("is_cancelled",      "boolean"),
    "BPUIC":               ("station_id",        "int"),        # used for the join
    "HALTESTELLEN_NAME":   ("station_name",      "string"),
    "ANKUNFTSZEIT":        ("planned_arrival",   "timestamp"),
    "AN_PROGNOSE":         ("actual_arrival",    "timestamp"),
    "AN_PROGNOSE_STATUS":  ("arrival_status",    "string"),     # REAL = really measured
    "ABFAHRTSZEIT":        ("planned_departure", "timestamp"),
    "AB_PROGNOSE":         ("actual_departure",  "timestamp"),
    "AB_PROGNOSE_STATUS":  ("departure_status",  "string"),
    "DURCHFAHRT_TF":       ("is_pass_through",   "boolean"),
    "SLOID":               ("sloid",             "string"),
}

# Service Points: the 56 columns in file order. We read all of them,
# but keep only the ones we need (KEEP_STATIONS).
STATIONS_HEADER = [
    "numberShort", "uicCountryCode", "sloid", "number", "checkDigit", "globalId",
    "validFrom", "validTo", "designationOfficial", "designationLong", "abbreviation",
    "operatingPoint", "operatingPointWithTimetable", "stopPoint", "stopPointType",
    "freightServicePoint", "trafficPoint", "borderPoint", "hasGeolocation",
    "isoCountryCode", "cantonName", "cantonFsoNumber", "cantonAbbreviation",
    "districtName", "districtFsoNumber", "municipalityName", "fsoNumber", "localityName",
    "operatingPointType", "operatingPointTechnicalTimetableType", "meansOfTransport",
    "categories", "operatingPointTrafficPointType", "operatingPointRouteNetwork",
    "operatingPointKilometer", "operatingPointKilometerMasterNumber",
    "sortCodeOfDestinationStation", "businessOrganisation", "businessOrganisationNumber",
    "businessOrganisationAbbreviationDe", "businessOrganisationAbbreviationFr",
    "businessOrganisationAbbreviationIt", "businessOrganisationAbbreviationEn",
    "businessOrganisationDescriptionDe", "businessOrganisationDescriptionFr",
    "businessOrganisationDescriptionIt", "businessOrganisationDescriptionEn",
    "fotComment", "lv95East", "lv95North", "wgs84East", "wgs84North", "height",
    "creationDate", "editionDate", "status",
]
KEEP_STATIONS = {
    "number":              ("station_id",         "int"),       # same as BPUIC
    "sloid":               ("sloid",              "string"),
    "designationOfficial": ("station_name",       "string"),
    "validFrom":           ("valid_from",         "date_iso"),
    "validTo":             ("valid_to",           "date_iso"),  # 9999-12-31 = still valid
    "isoCountryCode":      ("country",            "string"),
    "cantonAbbreviation":  ("canton",             "string"),
    "cantonName":          ("canton_name",        "string"),
    "municipalityName":    ("municipality",       "string"),
    "meansOfTransport":    ("means_of_transport", "string"),
    "stopPoint":           ("is_stop_point",      "boolean"),
    "wgs84East":           ("lon",                "double"),
    "wgs84North":          ("lat",                "double"),
    "status":              ("status",             "string"),
}

# Planned times have no seconds, real times sometimes do: we try both.
TS_FORMATS = ["dd.MM.yyyy HH:mm:ss", "dd.MM.yyyy HH:mm"]


def text(c):
    """Remove spaces; an empty value becomes NULL."""
    return F.nullif(F.trim(F.col(c)), F.lit(""))


def convert(c, kind):
    """Convert text to the real type. If it fails, the value becomes NULL."""
    t = text(c)
    if kind == "string":
        return t
    if kind == "int":
        return t.try_cast("int")
    if kind == "double":
        return t.try_cast("double")
    if kind == "boolean":
        low = F.lower(t)
        return F.when(low == "true", True).when(low == "false", False)
    if kind == "date":        # 28.09.2026
        return F.to_date(F.try_to_timestamp(t, F.lit("dd.MM.yyyy")))
    if kind == "date_iso":    # 2017-07-12
        return F.to_date(F.try_to_timestamp(t, F.lit("yyyy-MM-dd")))
    if kind == "timestamp":   # 28.09.2026 06:47 or 28.09.2026 06:47:30
        return F.coalesce(*[F.try_to_timestamp(t, F.lit(f)) for f in TS_FORMATS])
    raise ValueError(kind)


def check_headers(folder, expected):
    """Stop if a file does not have exactly the expected columns."""
    files = sorted(folder.glob("*.csv"))
    if not files:
        sys.exit(f"No CSV in {folder}. Run scripts/download_data.py first.")
    for f in files:
        with open(f, encoding="utf-8-sig") as fh:
            header = [h.strip().strip('"') for h in fh.readline().rstrip("\r\n").split(SEP)]
        if header != expected:
            sys.exit(f"Unexpected columns in {f.name}:\n  expected {expected}\n  found    {header}")
    print(f"  {len(files)} file(s) in {folder.name}: header OK ({len(expected)} columns)")
    return files


def read_csv(spark, files, columns):
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return (spark.read
            .option("header", True)
            .option("sep", SEP)
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .csv([str(f) for f in files]))


def ingest(spark, label, folder, header, mapping, out, partition=None):
    print(f"\n=== {label} ===")
    files = check_headers(folder, header)
    raw = read_csv(spark, files, header)

    typed = raw.select(*[convert(c, k).alias(new) for c, (new, k) in mapping.items()])
    writer = typed.write.mode("overwrite")
    if partition:
        writer = writer.partitionBy(partition)
    writer.parquet(str(out))
    print(f"  written to {out}")

    # Count the rows, and the values that were not empty but could not be converted.
    checked = [(c, new, k) for c, (new, k) in mapping.items() if k != "string"]
    stats = raw.agg(
        F.count(F.lit(1)).alias("rows"),
        *[F.sum(F.when(text(c).isNotNull() & convert(c, k).isNull(), 1).otherwise(0)).alias(new)
          for c, new, k in checked],
    ).first()
    print(f"  rows read: {stats['rows']:,}")
    rows = [(label, "rows read", "", stats["rows"])]
    for c, new, k in checked:
        n = stats[new] or 0
        rows.append((label, f"not convertible to {k}", f"{c} -> {new}", n))
        if n:
            print(f"  ! {n:,} values in {c} could not be converted to {k} (set to NULL)")
    return rows


def main():
    spark = get_spark("01_ingest")
    report = []
    report += ingest(spark, "istdaten", RAW_DIR / "istdaten", list(ISTDATEN), ISTDATEN,
                     PARQUET_DIR / "stops_typed", partition="operating_day")
    report += ingest(spark, "stations", RAW_DIR / "service_points", STATIONS_HEADER, KEEP_STATIONS,
                     PARQUET_DIR / "stations_typed")

    print("\nSchema of the typed Ist-Daten table:")
    spark.read.parquet(str(PARQUET_DIR / "stops_typed")).printSchema()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_DIR / "ingestion_report.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["table", "check", "column", "count"])
        w.writerows(report)
    print(f"Report written to {OUTPUT_DIR / 'ingestion_report.csv'}")
    spark.stop()


if __name__ == "__main__":
    main()
