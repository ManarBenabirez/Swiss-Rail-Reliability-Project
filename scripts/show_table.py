"""
Show a Parquet table: number of rows and the first lines.

  python scripts/show_table.py TABLE [LINES] [FILTER] [COLUMNS] [schema]

  python scripts/show_table.py stops_typed
  python scripts/show_table.py stops_typed 10 "station_name = 'Bern'"
  python scripts/show_table.py stops_typed 10 "station_name = 'Bern'" "operating_day,line_text,planned_arrival,actual_arrival"
  python scripts/show_table.py stations_typed 10 "" "station_id,station_name,valid_from,valid_to"

  FILTER: a Spark SQL condition, or "" for none.
  COLUMNS: column names separated by commas (all columns if empty).
  Add the word schema at the end to also see the column types.
"""
import sys
from pathlib import Path

# Lets this script use src/spark_session.py
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from spark_session import PARQUET_DIR, get_spark  # noqa: E402

args = sys.argv[1:]
show_schema = "schema" in args
args = [a for a in args if a != "schema"]
if not args:
    sys.exit(__doc__)

name = args[0]
n_lines = int(args[1]) if len(args) > 1 else 10
where = args[2] if len(args) > 2 and args[2].strip() else None
cols = [c.strip() for c in args[3].split(",")] if len(args) > 3 and args[3].strip() else None

spark = get_spark("show_table")
df = spark.read.parquet(str(PARQUET_DIR / name))
if show_schema:
    print(f"\n=== {name}: columns and types ===")
    df.printSchema()
if where:
    df = df.filter(where)
if cols:
    df = df.select(*cols)

print(f"\n=== {name}" + (f"  WHERE {where}" if where else "") + " ===")
print(f"rows: {df.count():,}\n")
df.show(n_lines, truncate=False)
spark.stop()
