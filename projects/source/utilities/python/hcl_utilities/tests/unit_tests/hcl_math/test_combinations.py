# Unit tests for hcl_math.combinations package

import pytest

import read_io.excel_io


class TestCombinations:
    @pytest.mark.parametrize(
        "testdata, expected",
        [
            ("A", 1),
            ("B", 2),
            ("C", 3),
            ("AA", 27),
            ("AB", 28),
            ("BB", 54),
            ("BI", 61),
        ],
    )
    def test_convert_excel_column_letters_to_column_index(self, testdata, expected):
        """Returns expected Excel-style column indices for each of the column letters."""
        output = read_io.excel_io.convert_excel_column_letters_to_column_index(testdata)
        assert output == expected

    @pytest.mark.parametrize(
        "testdata, expected",
        [
            (1, "A"),
            (2, "B"),
            (3, "C"),
            (27, "AA"),
            (28, "AB"),
            (54, "BB"),
            (61, "BI"),
        ],
    )
    def test_convert_excel_column_index_to_column_letters(self, testdata, expected):
        """Returns expected Excel-style column letters for each of the column indices."""
        output = read_io.excel_io.convert_excel_column_index_to_column_letters(testdata)
        assert output == expected
