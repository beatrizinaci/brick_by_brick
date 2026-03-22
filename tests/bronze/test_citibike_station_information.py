import pytest
import requests
from unittest.mock import patch, Mock

from brick_by_brick.bronze.citibike_station_information import fetch_station_information


def test_fetch_returns_stations_and_timestamp():
    fake_stations = [
        {
            "station_id": "abc123",
            "name": "36 St & 3 Ave",
            "short_name": "3460.02",
            "lat": 40.655716,
            "lon": -74.006664,
            "capacity": 30,
            "region_id": "71",
            "station_type": "classic",
            "has_kiosk": True,
            "electric_bike_surcharge_waiver": False,
            "eightd_has_key_dispenser": False,
            "external_id": "abc123",
            "rental_methods": ["KEY", "CREDITCARD"],
            "rental_uris": {"android": "https://bkn.lft.to/lastmile_qr_scan", "ios": "https://bkn.lft.to/lastmile_qr_scan"},
            "eightd_station_services": [],
        }
    ]
    mock_response = Mock()
    mock_response.json.return_value = {"data": {"stations": fake_stations}}

    with patch("brick_by_brick.bronze.citibike_station_information.requests.get", return_value=mock_response):
        stations, extracted_at = fetch_station_information()

    assert stations == fake_stations
    assert extracted_at.tzinfo is not None


def test_fetch_raises_on_http_error():
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

    with patch("brick_by_brick.bronze.citibike_station_information.requests.get", return_value=mock_response):
        with pytest.raises(requests.HTTPError):
            fetch_station_information()


def test_fetch_raises_on_timeout():
    with patch(
        "brick_by_brick.bronze.citibike_station_information.requests.get",
        side_effect=requests.exceptions.Timeout,
    ):
        with pytest.raises(requests.exceptions.Timeout):
            fetch_station_information()
