# Test Documentation

15 tests covering the most critical paths across all pipeline layers.

---

## test_extract.py — Bronze layer (2 tests)

| Test | What it checks |
|---|---|
| `test_header_row_matches_column_names` | The CSV header contains all four expected column names (DTG, LOCATION, TX_DRYB_10, TN_DRYB_10). Confirms the parser reads column positions correctly from the fixed-width header. |
| `test_comment_lines_not_written_to_csv` | Lines starting with `#` in the raw KNMI file are not carried over to the CSV. Confirms comment stripping works. |

---

## test_filter.py — Silver layer (4 tests)

| Test | What it checks |
|---|---|
| `test_keeps_correct_station` | A row for De Bilt (`260_T_a`) survives the filter. Core station logic. |
| `test_trims_whitespace_from_location` | A location value with leading/trailing spaces (`"  260_T_a  "`) still matches. The fixed-width parser often leaves padding. |
| `test_parses_full_timestamp_format` | The timestamp `"2003-08-01 12:00:00"` is correctly parsed to `date(2003, 8, 1)`. |
| `test_empty_tx_becomes_null` | An empty string in the TX column becomes `null` rather than crashing. Confirms safe casting. |

---

## test_aggregate.py — Gold layer, aggregation (2 tests)

| Test | What it checks |
|---|---|
| `test_daily_max_is_highest_tx` | Of two readings for the same day, the higher TX value becomes `daily_max`. |
| `test_daily_min_is_lowest_tn` | Of two readings for the same day, the lower TN value becomes `daily_min`. |

---

## test_detect.py — Gold layer, detection (5 tests)

All values are actual °C matching the thresholds in `detect.py`.

| Test | What it checks |
|---|---|
| `test_minimum_valid_heatwave_detected` | Exactly 5 days >= 25°C with exactly 3 tropical days (>= 30°C) — the bare minimum — is detected as one heatwave. |
| `test_only_4_days_not_a_heatwave` | 4 consecutive hot days is one short of the 5-day minimum — no wave returned. |
| `test_only_2_tropical_days_not_a_heatwave` | 5 days >= 25°C but only 2 tropical days — fails the >= 3 tropical day condition. |
| `test_gap_splits_into_two_heatwaves` | Two valid 5-day runs separated by a cool day produce two distinct waves, not one. Validates the gaps-and-islands grouping. |
| `test_minimum_valid_coldwave_detected` | Exactly 5 days max < 0°C with exactly 3 high-frost days (min < -10°C) is detected as one coldwave. |

---

## test_format.py — Gold layer, output formatting (2 tests)

| Test | What it checks |
|---|---|
| `test_heatwave_uses_tropical_days_label` | The column is labelled "Number of tropical days" for heatwave output. |
| `test_from_date_formatted_as_readable_string` | A date like `2003-08-01` is rendered as `"1 Aug 2003"` in the final output. |
