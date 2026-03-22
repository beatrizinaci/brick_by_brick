import pytest
import requests
from unittest.mock import patch, Mock
from datetime import timezone

from brick_by_brick.bronze.citibike_system_information import fetch_system_information


def test_fetch_returns_info_and_timestamp():
    fake_info = {
        "system_id": "lyft_nyc",
        "name": "Citi Bike",
        "operator": "Lyft",
        "url": "http://www.citibikenyc.com",
        "purchase_url": "https://account.citibikenyc.com/access-plans",
        "start_date": "2013-05-01",
        "language": "en",
        "timezone": "America/New_York",
    }
    mock_response = Mock()
    mock_response.json.return_value = {"data": fake_info}

    with patch("brick_by_brick.bronze.citibike_system_information.requests.get", return_value=mock_response):
        info, extracted_at = fetch_system_information()

    assert info == fake_info
    assert extracted_at.tzinfo is not None


def test_fetch_raises_on_http_error():
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

    with patch("brick_by_brick.bronze.citibike_system_information.requests.get", return_value=mock_response):
        with pytest.raises(requests.HTTPError):
            fetch_system_information()


def test_fetch_raises_on_timeout():
    with patch(
        "brick_by_brick.bronze.citibike_system_information.requests.get",
        side_effect=requests.exceptions.Timeout,
    ):
        with pytest.raises(requests.exceptions.Timeout):
            fetch_system_information()
