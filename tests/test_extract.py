"""
Tests for bronze/extract.py — fixed_width_to_csv

These are pure Python tests (no Spark required).
They validate the parser that converts raw KNMI fixed-width files into CSVs.
"""
import os
import tempfile

import pytest

from bronze.extract import fixed_width_to_csv


# Minimal fixed-width content that mirrors the real KNMI file format
SAMPLE_CONTENT = """# Source: KNMI
# DTG         LOCATION   TX_DRYB_10 TN_DRYB_10
200308011200  260_T_a    285        180
200308021200  260_T_a    312        201
200308011200  999_X_x    270        150
"""

# A file where the header has different column ordering
REORDERED_CONTENT = """# DTG         TX_DRYB_10 LOCATION   TN_DRYB_10
200308011200  285        260_T_a    180
"""


@pytest.fixture
def out_path():
    """Temporary file path; cleaned up after each test."""
    f = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    f.close()
    yield f.name
    os.unlink(f.name)


def read_csv_lines(path):
    with open(path, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


# ---------------------------------------------------------------------------
# Header / structure tests
# ---------------------------------------------------------------------------

def test_header_row_matches_column_names(out_path):
    fixed_width_to_csv(SAMPLE_CONTENT, out_path)
    lines = read_csv_lines(out_path)
    headers = lines[0].split(",")
    assert "DTG" in headers
    assert "LOCATION" in headers
    assert "TX_DRYB_10" in headers
    assert "TN_DRYB_10" in headers


def test_correct_number_of_data_rows(out_path):
    fixed_width_to_csv(SAMPLE_CONTENT, out_path)
    lines = read_csv_lines(out_path)
    # 1 header + 3 data rows
    assert len(lines) == 4


def test_comment_lines_not_written_to_csv(out_path):
    fixed_width_to_csv(SAMPLE_CONTENT, out_path)
    lines = read_csv_lines(out_path)
    for line in lines:
        assert not line.startswith("#")


def test_empty_content_produces_no_output(out_path):
    fixed_width_to_csv("", out_path)
    lines = read_csv_lines(out_path)
    assert lines == []


# ---------------------------------------------------------------------------
# Data value tests
# ---------------------------------------------------------------------------

def test_data_values_are_extracted_correctly(out_path):
    fixed_width_to_csv(SAMPLE_CONTENT, out_path)
    lines = read_csv_lines(out_path)
    headers = lines[0].split(",")
    tx_col = headers.index("TX_DRYB_10")
    # First data row TX value should be 285
    first_row = lines[1].split(",")
    assert first_row[tx_col] == "285"


def test_parser_handles_reordered_columns(out_path):
    """Column order in the header must not affect which value lands in which column."""
    fixed_width_to_csv(REORDERED_CONTENT, out_path)
    lines = read_csv_lines(out_path)
    headers = lines[0].split(",")
    loc_col = headers.index("LOCATION")
    first_row = lines[1].split(",")
    assert first_row[loc_col] == "260_T_a"


def test_all_stations_included_in_output(out_path):
    """The bronze layer does not filter by station — that is silver's job."""
    fixed_width_to_csv(SAMPLE_CONTENT, out_path)
    lines = read_csv_lines(out_path)
    raw = "\n".join(lines)
    assert "260_T_a" in raw
    assert "999_X_x" in raw
