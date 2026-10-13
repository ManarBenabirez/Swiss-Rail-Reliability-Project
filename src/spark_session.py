"""Shared Spark session + project paths. Import this in every script."""
import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"          # downloaded CSV files
PARQUET_DIR = DATA_DIR / "parquet"  # typed / cleaned tables
OUTPUT_DIR = ROOT / "output"        # result tables, charts, row-count reports


def get_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # use all CPU cores of the laptop
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.session.timeZone", "Europe/Zurich")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
