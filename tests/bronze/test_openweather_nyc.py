import pytest
import requests
from unittest.mock import patch, Mock
from datetime import datetime, timezone

from brick_by_brick.bronze.openweather_nyc import fetch_nyc_weather, parse_weather


FAKE_RAW = {
    "main": {"temp": 15.2, "feels_like": 14.1, "humidity": 65, "pressure": 1013},
    "wind": {"speed": 3.5, "deg": 180},
    "clouds": {"all": 20},
    "weather": [{"main": "Clouds", "description": "few clouds"}],
    "visibility": 10000,
    "dt": 1700000000,
}


def test_fetch_returns_data_and_timestamp():
    mock_response = Mock()
    mock_response.json.return_value = FAKE_RAW

    with patch("brick_by_brick.bronze.openweather_nyc.requests.get", return_value=mock_response):
        data, extracted_at = fetch_nyc_weather("fake-key")

    assert data == FAKE_RAW
    assert extracted_at.tzinfo is not None


def test_fetch_raises_on_http_error():
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")

    with patch("brick_by_brick.bronze.openweather_nyc.requests.get", return_value=mock_response):
        with pytest.raises(requests.HTTPError):
            fetch_nyc_weather("bad-key")


def test_fetch_raises_on_timeout():
    with patch(
        "brick_by_brick.bronze.openweather_nyc.requests.get",
        side_effect=requests.exceptions.Timeout,
    ):
        with pytest.raises(requests.exceptions.Timeout):
            fetch_nyc_weather("fake-key")


def test_parse_weather_maps_fields():
    extracted_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    record = parse_weather(FAKE_RAW, extracted_at)

    assert record["temp"] == 15.2
    assert record["humidity"] == 65
    assert record["wind_speed"] == 3.5
    assert record["wind_deg"] == 180
    assert record["rain_1h"] is None
    assert record["visibility"] == 10000
    assert record["weather_main"] == "Clouds"
    assert record["_extracted_at"] == extracted_at


def test_parse_weather_with_rain():
    raw_with_rain = {**FAKE_RAW, "rain": {"1h": 2.5}}
    extracted_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    record = parse_weather(raw_with_rain, extracted_at)

    assert record["rain_1h"] == 2.5
