"""
Tests for bronze/extract.py — fixed_width_to_csv

Pure Python tests (no Spark required).
Validates the parser that converts raw KNMI fixed-width files into CSVs.
"""
import os
import tempfile

from bronze.extract import fixed_width_to_csv


SAMPLE_CONTENT = """# Source: KNMI
# DTG         LOCATION   TX_DRYB_10 TN_DRYB_10
200308011200  260_T_a    285        180
200308021200  260_T_a    312        201
200308011200  999_X_x    270        150
"""


def out_path_fixture():
    f = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    f.close()
    return f.name


def read_csv_lines(path):
    with open(path, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def test_header_row_matches_column_names():
    path = out_path_fixture()
    try:
        fixed_width_to_csv(SAMPLE_CONTENT, path)
        headers = read_csv_lines(path)[0].split(",")
        assert "DTG" in headers
        assert "LOCATION" in headers
        assert "TX_DRYB_10" in headers
        assert "TN_DRYB_10" in headers
    finally:
        os.unlink(path)


def test_comment_lines_not_written_to_csv():
    path = out_path_fixture()
    try:
        fixed_width_to_csv(SAMPLE_CONTENT, path)
        for line in read_csv_lines(path):
            assert not line.startswith("#")
    finally:
        os.unlink(path)
