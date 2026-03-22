"""Bronze extraction: Citi Bike station status (GBFS feed).

Fetches real-time station status from the GBFS API and appends raw data
to a Delta table. Designed to run every 1 minute on Databricks.
"""

import requests
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    BooleanType,
    TimestampType,
)

STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

SCHEMA = StructType(
    [
        StructField("station_id", StringType(), False),
        StructField("num_bikes_available", IntegerType(), True),
        StructField("num_ebikes_available", IntegerType(), True),
        StructField("num_bikes_disabled", IntegerType(), True),
        StructField("num_docks_available", IntegerType(), True),
        StructField("num_docks_disabled", IntegerType(), True),
        StructField("is_installed", IntegerType(), True),
        StructField("is_renting", IntegerType(), True),
        StructField("is_returning", IntegerType(), True),
        StructField("last_reported", LongType(), True),
        StructField("eightd_has_available_keys", BooleanType(), True),
        StructField("legacy_id", StringType(), True),
        StructField("_extracted_at", TimestampType(), False),
    ]
)


def fetch_station_status() -> tuple[list[dict], datetime]:
    """Fetch station status from GBFS API. Returns (stations, extraction_timestamp)."""
    response = requests.get(STATION_STATUS_URL, timeout=30)
    response.raise_for_status()
    extracted_at = datetime.now(timezone.utc)
    data = response.json()
    return data["data"]["stations"], extracted_at


def extract(catalog: str, schema: str) -> None:
    """Fetch station status and append to bronze Delta table."""
    spark = SparkSession.builder.getOrCreate()

    stations, extracted_at = fetch_station_status()

    for s in stations:
        s["_extracted_at"] = extracted_at

    df = spark.createDataFrame(stations, schema=SCHEMA)

    table_name = f"{catalog}.{schema}.bronze_citibike_station_status"
    df.write.format("delta").mode("append").saveAsTable(table_name)


def main():
    import sys

    catalog = sys.argv[1] if len(sys.argv) > 1 else "default"
    schema = sys.argv[2] if len(sys.argv) > 2 else "dev"
    extract(catalog, schema)


if __name__ == "__main__":
    main()
