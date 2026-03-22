"""Bronze extraction: OpenWeather current conditions for NYC.

Fetches current weather from the OpenWeather API for New York City
and appends a snapshot to a Delta table. Designed to run every 10 minutes.

Requires the environment variable OPENWEATHER_API_KEY.
"""

import os
import requests
from datetime import datetime, timezone
from loguru import logger

OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
NYC_LAT = 40.7128
NYC_LON = -74.0060


def fetch_nyc_weather(api_key: str) -> tuple[dict, datetime]:
    """Fetch current weather for NYC. Returns (weather_data, extraction_timestamp)."""
    logger.info("Fetching current weather for NYC from OpenWeather API")
    response = requests.get(
        OPENWEATHER_URL,
        params={"lat": NYC_LAT, "lon": NYC_LON, "units": "metric", "appid": api_key},
        timeout=30,
    )
    response.raise_for_status()
    extracted_at = datetime.now(timezone.utc)
    return response.json(), extracted_at


def parse_weather(raw: dict, extracted_at: datetime) -> dict:
    """Extract relevant fields from raw OpenWeather response."""
    return {
        "temp": raw["main"]["temp"],
        "feels_like": raw["main"]["feels_like"],
        "humidity": raw["main"]["humidity"],
        "pressure": raw["main"]["pressure"],
        "wind_speed": raw["wind"]["speed"],
        "wind_deg": raw["wind"].get("deg"),
        "rain_1h": raw.get("rain", {}).get("1h"),
        "visibility": raw.get("visibility"),
        "clouds_pct": raw["clouds"]["all"],
        "weather_main": raw["weather"][0]["main"],
        "weather_description": raw["weather"][0]["description"],
        "dt": raw["dt"],
        "_extracted_at": extracted_at,
    }


def extract(catalog: str, schema: str) -> None:
    """Fetch NYC weather and append to bronze Delta table."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, LongType,
        DoubleType, TimestampType,
    )

    schema_def = StructType(
        [
            StructField("temp", DoubleType(), True),
            StructField("feels_like", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("pressure", IntegerType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_deg", IntegerType(), True),
            StructField("rain_1h", DoubleType(), True),
            StructField("visibility", IntegerType(), True),
            StructField("clouds_pct", IntegerType(), True),
            StructField("weather_main", StringType(), True),
            StructField("weather_description", StringType(), True),
            StructField("dt", LongType(), True),
            StructField("_extracted_at", TimestampType(), False),
        ]
    )

    table_name = f"{catalog}.{schema}.bronze_openweather_nyc"
    logger.info(f"Starting extraction -> {table_name}")

    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        api_key = dbutils.secrets.get(scope="brick_by_brick", key="openweather_api_key")
        logger.info("API key loaded from Databricks secret scope")
    except Exception:
        logger.info("Databricks secrets unavailable, falling back to environment variable")
        api_key = os.environ["OPENWEATHER_API_KEY"]

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    raw, extracted_at = fetch_nyc_weather(api_key)
    record = parse_weather(raw, extracted_at)
    logger.info(f"Weather: {record['weather_main']} | temp={record['temp']}°C | humidity={record['humidity']}%")

    df = spark.createDataFrame([record], schema=schema_def)
    df.write.format("delta").mode("append").saveAsTable(table_name)
    logger.info(f"Appended 1 row to {table_name}")


def main():
    import sys

    catalog = sys.argv[1] if len(sys.argv) > 1 else "default"
    schema = sys.argv[2] if len(sys.argv) > 2 else "dev"
    extract(catalog, schema)


if __name__ == "__main__":
    main()
