"""Bronze extraction: Citi Bike system information (GBFS feed).

Fetches static system metadata from the GBFS API and appends a snapshot
to a Delta table. Designed to run once daily on Databricks.
"""

import requests
from datetime import datetime, timezone

SYSTEM_INFORMATION_URL = "https://gbfs.citibikenyc.com/gbfs/en/system_information.json"


def fetch_system_information() -> tuple[dict, datetime]:
    """Fetch system information from GBFS API. Returns (info, extraction_timestamp)."""
    response = requests.get(SYSTEM_INFORMATION_URL, timeout=30)
    response.raise_for_status()
    extracted_at = datetime.now(timezone.utc)
    data = response.json()
    return data["data"], extracted_at


def extract(catalog: str, schema: str) -> None:
    """Fetch system information and append to bronze Delta table."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType,
    )

    schema_def = StructType(
        [
            StructField("system_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("operator", StringType(), True),
            StructField("url", StringType(), True),
            StructField("purchase_url", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("language", StringType(), True),
            StructField("timezone", StringType(), True),
            StructField("_extracted_at", TimestampType(), False),
        ]
    )

    spark = SparkSession.builder.getOrCreate()

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    info, extracted_at = fetch_system_information()
    info["_extracted_at"] = extracted_at

    df = spark.createDataFrame([info], schema=schema_def)

    table_name = f"{catalog}.{schema}.bronze_citibike_system_information"
    df.write.format("delta").mode("append").saveAsTable(table_name)


def main():
    import sys

    catalog = sys.argv[1] if len(sys.argv) > 1 else "default"
    schema = sys.argv[2] if len(sys.argv) > 2 else "dev"
    extract(catalog, schema)


if __name__ == "__main__":
    main()
