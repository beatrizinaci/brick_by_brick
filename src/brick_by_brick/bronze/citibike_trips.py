"""Bronze extraction: Citi Bike trip history (S3 tripdata).

Ingests monthly trip CSV files from the public S3 bucket s3a://tripdata
using Databricks Autoloader (cloudFiles). Only processes files matching
the configured year filter (default: 2026). Uses trigger(availableNow=True)
to behave like a batch job while maintaining exactly-once guarantees via
checkpoint. The required Volume is created automatically if it does not exist.
"""

from loguru import logger

TRIPDATA_SOURCE = "s3a://tripdata/"
DEFAULT_YEAR = "2026"


def extract(catalog: str, schema: str, checkpoint_path: str, year: str = DEFAULT_YEAR) -> None:
    """Ingest Citi Bike trip CSVs from S3 into bronze Delta table via Autoloader."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType,
    )

    schema_def = StructType([
        StructField("ride_id", StringType(), True),
        StructField("rideable_type", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("ended_at", TimestampType(), True),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_id", StringType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_id", StringType(), True),
        StructField("start_lat", DoubleType(), True),
        StructField("start_lng", DoubleType(), True),
        StructField("end_lat", DoubleType(), True),
        StructField("end_lng", DoubleType(), True),
        StructField("member_casual", StringType(), True),
    ])

    table_name = f"{catalog}.{schema}.bronze_citibike_trips"
    path_filter = f"{year}*-citibike-tripdata.csv.zip"
    logger.info(f"Starting Autoloader ingestion -> {table_name} (filter: {path_filter})")

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")

    stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .option("pathGlobFilter", path_filter)
        .option("header", "true")
        .schema(schema_def)
        .load(TRIPDATA_SOURCE)
    )

    query = (
        stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
    )

    query.awaitTermination()
    logger.info(f"Autoloader ingestion complete -> {table_name}")


def main():
    import sys

    catalog = sys.argv[1] if len(sys.argv) > 1 else "default"
    schema = sys.argv[2] if len(sys.argv) > 2 else "dev"
    checkpoint_path = (
        sys.argv[3] if len(sys.argv) > 3
        else f"/Volumes/{catalog}/{schema}/checkpoints/citibike_trips"
    )
    year = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_YEAR
    extract(catalog, schema, checkpoint_path, year)


if __name__ == "__main__":
    main()
