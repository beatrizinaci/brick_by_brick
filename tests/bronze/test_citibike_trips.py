import sys
from unittest.mock import patch, MagicMock

from brick_by_brick.bronze.citibike_trips import DEFAULT_YEAR, TRIPDATA_SOURCE, main


def test_default_year_is_2026():
    assert DEFAULT_YEAR == "2026"


def test_tripdata_source_points_to_public_bucket():
    assert TRIPDATA_SOURCE == "s3a://tripdata/"


def test_main_passes_args_to_extract():
    with patch("brick_by_brick.bronze.citibike_trips.extract") as mock_extract:
        with patch.object(sys, "argv", ["prog", "my_catalog", "my_schema", "/Volumes/my_catalog/my_schema/checkpoints/citibike_trips", "2025"]):
            main()

    mock_extract.assert_called_once_with(
        "my_catalog",
        "my_schema",
        "/Volumes/my_catalog/my_schema/checkpoints/citibike_trips",
        "2025",
    )


def test_main_default_checkpoint_path_uses_catalog_and_schema():
    with patch("brick_by_brick.bronze.citibike_trips.extract") as mock_extract:
        with patch.object(sys, "argv", ["prog", "brick_by_brick", "prod"]):
            main()

    _, _, checkpoint_path, _ = mock_extract.call_args[0]
    assert "brick_by_brick" in checkpoint_path
    assert "prod" in checkpoint_path
    assert "citibike_trips" in checkpoint_path


def test_main_default_year_is_2026():
    with patch("brick_by_brick.bronze.citibike_trips.extract") as mock_extract:
        with patch.object(sys, "argv", ["prog", "brick_by_brick", "prod", "/some/checkpoint"]):
            main()

    _, _, _, year = mock_extract.call_args[0]
    assert year == "2026"


def test_main_defaults_without_args():
    with patch("brick_by_brick.bronze.citibike_trips.extract") as mock_extract:
        with patch.object(sys, "argv", ["prog"]):
            main()

    catalog, schema, checkpoint_path, year = mock_extract.call_args[0]
    assert catalog == "default"
    assert schema == "dev"
    assert year == "2026"
