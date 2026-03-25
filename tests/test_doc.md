# Test Documentation

## Overview

51 tests across 5 files. Each file maps to one layer of the Medallion Architecture.
No Spark is required for `test_extract.py` — all other files share a single SparkSession defined in `conftest.py`.

---

## conftest.py

| Item | Description |
|---|---|
| `spark` fixture | Creates one SparkSession for the entire test run (`scope="session"`). Shared across all Spark tests to avoid the ~10s startup cost per test. Shut down automatically after all tests complete. |

---

## test_extract.py — Bronze Layer (Pure Python)

Tests the `fixed_width_to_csv` function in `bronze/extract.py`. No Spark needed — these run fast.

| Test | What it checks |
|---|---|
| `test_header_row_matches_column_names` | The first row of the output CSV contains the column names from the `# DTG` header line (DTG, LOCATION, TX_DRYB_10, TN_DRYB_10) |
| `test_correct_number_of_data_rows` | 3 data lines in the input produce exactly 3 rows in the CSV (plus 1 header = 4 lines total) |
| `test_comment_lines_not_written_to_csv` | Lines starting with `#` are stripped and never appear in the output |
| `test_empty_content_produces_no_output` | An empty string input writes nothing to the CSV |
| `test_data_values_are_extracted_correctly` | The TX value `285` from the first data row lands in the correct column |
| `test_parser_handles_reordered_columns` | If column order in the header changes (e.g. TX before LOCATION), values still map to the right columns — validates the dynamic position detection |
| `test_all_stations_included_in_output` | Both `260_T_a` and `999_X_x` appear in the output — confirms bronze does not filter by station (that is silver's responsibility) |

---

## test_filter.py — Silver Layer

Tests the `filter_station` function in `silver/filter.py`.

### Station Filtering

| Test | What it checks |
|---|---|
| `test_keeps_correct_station` | A row for `260_T_a` passes through the filter |
| `test_excludes_other_stations` | A row for `999_X_x` is removed |
| `test_trims_whitespace_from_location` | `"  260_T_a  "` (with spaces) is still matched — guards against whitespace left over from fixed-width parsing |
| `test_mixed_stations_only_keeps_target` | 3 rows (2 for De Bilt, 1 for another station) — only 2 come out |

### Date Parsing

| Test | What it checks |
|---|---|
| `test_parses_full_timestamp_format` | `"2003-08-01 12:00:00"` parses to `datetime.date(2003, 8, 1)` |
| `test_parses_compact_timestamp_format` | `"20030801120000"` parses to the same date — validates the fallback format |
| `test_drops_rows_with_unparseable_date` | `"not-a-date"` results in zero rows — unrecoverable rows are removed |
| `test_drops_rows_with_empty_date` | An empty DTG string produces zero rows |

### Temperature Casting

| Test | What it checks |
|---|---|
| `test_tx_cast_to_double` | `"285"` (string) becomes `285.0` (float) in the `tx` column |
| `test_tn_cast_to_double` | `"180"` (string) becomes `180.0` (float) in the `tn` column |
| `test_empty_tx_becomes_null` | An empty `TX_DRYB_10` string becomes `None` rather than causing a cast error |
| `test_empty_tn_becomes_null` | Same as above for `TN_DRYB_10` |

### Output Schema

| Test | What it checks |
|---|---|
| `test_output_only_has_date_tx_tn_columns` | The output DataFrame has exactly three columns: `date`, `tx`, `tn` — all other raw columns are dropped |

---

## test_aggregate.py — Gold Layer

Tests the `daily_aggregate` function in `gold/aggregate.py`.

| Test | What it checks |
|---|---|
| `test_multiple_readings_collapse_to_one_row` | 3 readings on the same date produce exactly 1 output row |
| `test_daily_max_is_highest_tx` | When two readings exist for one day (250 and 310), `daily_max` is 310 |
| `test_daily_min_is_lowest_tn` | When two readings exist (180 and 120), `daily_min` is 120 |
| `test_separate_days_produce_separate_rows` | Readings on two different dates produce two output rows |
| `test_output_ordered_by_date` | Three days inserted out of order come out sorted chronologically |
| `test_single_reading_passes_through` | A single reading produces one row with the correct max and min |
| `test_output_columns_are_date_daily_max_daily_min` | Output schema is exactly `{date, daily_max, daily_min}` |

---

## test_detect.py — Gold Layer (Core Business Logic)

Tests `detect_heatwaves` and `detect_coldwaves` in `gold/detect.py`. These tests directly encode the official KNMI meteorological definitions.

> Temperature values are in tenths of °C (KNMI convention): `310` = 31.0°C, `-120` = -12.0°C

### Heatwave Detection — Happy Path

| Test | What it checks |
|---|---|
| `test_minimum_valid_heatwave_detected` | Exactly 5 days >= 25, exactly 3 tropical (>= 30) — the minimum qualifying case returns 1 result |
| `test_long_heatwave_detected` | 10 consecutive tropical days — confirms there is no upper limit |
| `test_heatwave_duration_is_correct` | A 5-day wave reports `duration = 5` |
| `test_heatwave_start_and_end_dates_are_correct` | Wave from Aug 1–5 reports `from_date = 2003-08-01` and `to_date = 2003-08-05` |
| `test_tropical_day_count_is_correct` | 3 tropical days in a 5-day wave reports `special_days = 3` |
| `test_max_temperature_reported_correctly` | The highest daily_max across the wave (335) is reported as `extreme_temp` |

### Heatwave Detection — Boundary (Wave NOT Detected)

| Test | What it checks |
|---|---|
| `test_only_4_days_not_a_heatwave` | 4 consecutive hot days — one short of the 5-day minimum — returns 0 results |
| `test_only_2_tropical_days_not_a_heatwave` | 5 hot days but only 2 tropical — one short of the 3-day minimum — returns 0 results |
| `test_days_below_25_are_not_counted` | A cool day in the middle of a hot run breaks the consecutive streak — the two halves do not combine |
| `test_empty_dataframe_returns_no_heatwaves` | An empty input DataFrame returns 0 results without errors |

### Heatwave Detection — Gaps-and-Islands Algorithm

| Test | What it checks |
|---|---|
| `test_gap_splits_consecutive_days_into_two_waves` | A missing day between two qualifying runs produces 2 separate waves, not 1 merged wave |
| `test_two_waves_have_correct_individual_dates` | Each of the two waves reports its own correct start date independently |

### Coldwave Detection — Happy Path

| Test | What it checks |
|---|---|
| `test_minimum_valid_coldwave_detected` | Exactly 5 days max < 0, exactly 3 high-frost (min < -10) — returns 1 result |
| `test_coldwave_duration_is_correct` | A 5-day wave reports `duration = 5` |
| `test_coldwave_start_and_end_dates_are_correct` | Wave from Feb 1–5 reports correct start and end dates |
| `test_high_frost_day_count_is_correct` | 3 high-frost days in a 5-day wave reports `special_days = 3` |
| `test_min_temperature_reported_correctly` | The lowest daily_min across the wave (-150) is reported as `extreme_temp` |

### Coldwave Detection — Boundary (Wave NOT Detected)

| Test | What it checks |
|---|---|
| `test_only_4_days_not_a_coldwave` | 4 consecutive freezing days — one short of the minimum — returns 0 results |
| `test_only_2_high_frost_days_not_a_coldwave` | 5 freezing days but only 2 high-frost — returns 0 results |
| `test_day_with_max_above_zero_breaks_coldwave` | A single day with max >= 0 interrupts the streak and prevents detection |
| `test_empty_dataframe_returns_no_coldwaves` | An empty input DataFrame returns 0 results without errors |

### Coldwave Detection — Gaps-and-Islands Algorithm

| Test | What it checks |
|---|---|
| `test_gap_splits_consecutive_days_into_two_coldwaves` | A warm gap day between two cold runs produces 2 separate waves, not 1 |

---

## test_format.py — Gold Layer

Tests the `format_output` function in `gold/format.py`.

### Column Labels

| Test | What it checks |
|---|---|
| `test_heatwave_uses_tropical_days_label` | `wave_type="heatwave"` produces a column named `"Number of tropical days"` |
| `test_coldwave_uses_high_frost_days_label` | `wave_type="coldwave"` produces a column named `"Number of high frost days"` |
| `test_heatwave_uses_max_temperature_label` | `wave_type="heatwave"` produces a column named `"Max temperature"` |
| `test_coldwave_uses_min_temperature_label` | `wave_type="coldwave"` produces a column named `"Min temperature"` |

### Date Formatting

| Test | What it checks |
|---|---|
| `test_from_date_formatted_as_readable_string` | `2003-08-01` is displayed as `"1 Aug 2003"` not as an ISO date string |
| `test_to_date_formatted_as_readable_string` | `2003-08-10` is displayed as `"10 Aug 2003"` |

### Temperature

| Test | What it checks |
|---|---|
| `test_temperature_rounded_to_one_decimal` | The temperature value in the output is rounded to 1 decimal place |

### Ordering

| Test | What it checks |
|---|---|
| `test_results_ordered_by_start_date` | Two waves inserted in reverse order come out sorted chronologically by start date |

### Output Schema

| Test | What it checks |
|---|---|
| `test_output_has_exactly_five_columns` | The final output has exactly 5 columns: From date, To date, Duration, special label, temperature label |

---

## Running the Tests

```bash
# All tests
pytest tests/

# Verbose output (see each test name)
pytest tests/ -v

# With coverage report
pytest tests/ --cov=. --cov-report=term-missing

# Single file
pytest tests/test_detect.py

# Single test
pytest tests/test_detect.py::TestDetectHeatwaves::test_minimum_valid_heatwave_detected
```
