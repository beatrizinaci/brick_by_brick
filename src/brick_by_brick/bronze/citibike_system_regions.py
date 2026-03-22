"""Bronze extraction: Citi Bike system regions (GBFS feed).

Fetches the list of system regions from the GBFS API and appends a snapshot
to a Delta table. Designed to run once daily on Databricks.
"""

import requests
from datetime import datetime, timezone
from loguru import logger

SYSTEM_REGIONS_URL = "https://gbfs.citibikenyc.com/gbfs/en/system_regions.json"


def fetch_system_regions() -> tuple[list[dict], datetime]:
    """Fetch system regions from GBFS API. Returns (regions, extraction_timestamp)."""
    logger.info("Fetching system regions from GBFS API")
    response = requests.get(SYSTEM_REGIONS_URL, timeout=30)
    response.raise_for_status()
    extracted_at = datetime.now(timezone.utc)
    data = response.json()
    regions = data["data"]["regions"]
    logger.info(f"Fetched {len(regions)} regions")
    return regions, extracted_at


def extract(catalog: str, schema: str) -> None:
    """Fetch system regions and append to bronze Delta table."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType,
    )

    schema_def = StructType(
        [
            StructField("region_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("_extracted_at", TimestampType(), False),
        ]
    )

    table_name = f"{catalog}.{schema}.bronze_citibike_system_regions"
    logger.info(f"Starting extraction -> {table_name}")

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    regions, extracted_at = fetch_system_regions()

    for r in regions:
        r["_extracted_at"] = extracted_at

    df = spark.createDataFrame(regions, schema=schema_def)
    df.write.format("delta").mode("append").saveAsTable(table_name)
    logger.info(f"Appended {df.count()} rows to {table_name}")


def main():
    import sys

    catalog = sys.argv[1] if len(sys.argv) > 1 else "default"
    schema = sys.argv[2] if len(sys.argv) > 2 else "dev"
    extract(catalog, schema)


if __name__ == "__main__":
    main()
