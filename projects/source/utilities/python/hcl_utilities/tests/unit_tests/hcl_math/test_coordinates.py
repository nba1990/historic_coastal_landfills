# Unit tests for hcl_math.coordinates package

import pytest

import hcl_math.coordinates


class TestConvertCoordinates:
    @pytest.mark.parametrize(
        "input_coordinates, expected_coordinates",
        [
            ((534600, 342000), (52.958489029831, 0.002334965131743245)),
            ((534400, 341500), (52.954047248782295, -0.0008476602151455863)),
        ],
    )
    def test_convert_easting_northing_to_lat_long(
        self, input_coordinates, expected_coordinates
    ):
        """Easting and northing values are successfully converted to latitude and longitude coordinates."""
        output = hcl_math.coordinates.convert_easting_northing_to_lat_long(
            input_coordinates[0], input_coordinates[1]
        )
        assert output == expected_coordinates
