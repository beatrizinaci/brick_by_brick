import pytest
import requests
from unittest.mock import patch, Mock
from datetime import timezone

from brick_by_brick.bronze.citibike_system_regions import fetch_system_regions


def test_fetch_returns_regions_and_timestamp():
    fake_regions = [
        {"region_id": "70", "name": "JC District"},
        {"region_id": "71", "name": "NYC District"},
    ]
    mock_response = Mock()
    mock_response.json.return_value = {"data": {"regions": fake_regions}}

    with patch("brick_by_brick.bronze.citibike_system_regions.requests.get", return_value=mock_response):
        regions, extracted_at = fetch_system_regions()

    assert regions == fake_regions
    assert extracted_at.tzinfo is not None


def test_fetch_raises_on_http_error():
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

    with patch("brick_by_brick.bronze.citibike_system_regions.requests.get", return_value=mock_response):
        with pytest.raises(requests.HTTPError):
            fetch_system_regions()


def test_fetch_raises_on_timeout():
    with patch(
        "brick_by_brick.bronze.citibike_system_regions.requests.get",
        side_effect=requests.exceptions.Timeout,
    ):
        with pytest.raises(requests.exceptions.Timeout):
            fetch_system_regions()
