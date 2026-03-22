"""Bronze extraction: Citi Bike station information (GBFS feed).

Fetches static station master data from the GBFS API and appends a snapshot
to a Delta table. Designed to run once daily on Databricks.
"""

import requests
from datetime import datetime, timezone

STATION_INFORMATION_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"


def fetch_station_information() -> tuple[list[dict], datetime]:
    """Fetch station information from GBFS API. Returns (stations, extraction_timestamp)."""
    response = requests.get(STATION_INFORMATION_URL, timeout=30)
    response.raise_for_status()
    extracted_at = datetime.now(timezone.utc)
    data = response.json()
    return data["data"]["stations"], extracted_at


def extract(catalog: str, schema: str) -> None:
    """Fetch station information and append to bronze Delta table."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType,
        BooleanType, ArrayType, MapType, TimestampType,
    )

    schema_def = StructType(
        [
            StructField("station_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("short_name", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("capacity", IntegerType(), True),
            StructField("region_id", StringType(), True),
            StructField("station_type", StringType(), True),
            StructField("has_kiosk", BooleanType(), True),
            StructField("electric_bike_surcharge_waiver", BooleanType(), True),
            StructField("eightd_has_key_dispenser", BooleanType(), True),
            StructField("external_id", StringType(), True),
            StructField("rental_methods", ArrayType(StringType()), True),
            StructField("rental_uris", MapType(StringType(), StringType()), True),
            StructField("eightd_station_services", ArrayType(StringType()), True),
            StructField("_extracted_at", TimestampType(), False),
        ]
    )

    spark = SparkSession.builder.getOrCreate()

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    stations, extracted_at = fetch_station_information()

    for s in stations:
        s["_extracted_at"] = extracted_at

    df = spark.createDataFrame(stations, schema=schema_def)

    table_name = f"{catalog}.{schema}.bronze_citibike_station_information"
    df.write.format("delta").mode("append").saveAsTable(table_name)


def main():
    import sys

    catalog = sys.argv[1] if len(sys.argv) > 1 else "default"
    schema = sys.argv[2] if len(sys.argv) > 2 else "dev"
    extract(catalog, schema)


if __name__ == "__main__":
    main()
