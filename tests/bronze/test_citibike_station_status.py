import pytest
import requests
from unittest.mock import patch, Mock
from datetime import timezone

from brick_by_brick.bronze.citibike_station_status import fetch_station_status


def test_fetch_returns_stations_and_timestamp():
    fake_stations = [{"station_id": "1", "num_bikes_available": 3}]
    mock_response = Mock()
    mock_response.json.return_value = {"data": {"stations": fake_stations}}

    with patch("brick_by_brick.bronze.citibike_station_status.requests.get", return_value=mock_response):
        stations, extracted_at = fetch_station_status()

    assert stations == fake_stations
    assert extracted_at.tzinfo is not None


def test_fetch_raises_on_http_error():
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

    with patch("brick_by_brick.bronze.citibike_station_status.requests.get", return_value=mock_response):
        with pytest.raises(requests.HTTPError):
            fetch_station_status()


def test_fetch_raises_on_timeout():
    with patch(
        "brick_by_brick.bronze.citibike_station_status.requests.get",
        side_effect=requests.exceptions.Timeout,
    ):
        with pytest.raises(requests.exceptions.Timeout):
            fetch_station_status()
