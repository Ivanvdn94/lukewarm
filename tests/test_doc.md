# Test Documentation

5 tests — 4 unit tests and 1 integration test.

---

## test_filter.py — Unit (1 test)

| Test | What it checks |
|---|---|
| `test_empty_tx_becomes_null` | An empty string in the TX column becomes `null` rather than crashing or producing 0. The most common silent data bug in typed pipelines. |

---

## test_detect.py — Unit (3 tests)

| Test | What it checks |
|---|---|
| `test_minimum_valid_heatwave_detected` | Exactly 5 days >= 25°C with exactly 3 tropical days (>= 30°C) — the bare minimum — is detected as one heatwave. Confirms the happy path of the core algorithm. |
| `test_gap_splits_into_two_heatwaves` | Two valid 5-day runs separated by a cool day produce two distinct waves, not one. Validates the gaps-and-islands grouping — the most complex logic in the codebase. |
| `test_minimum_valid_coldwave_detected` | Exactly 5 days max < 0°C with exactly 3 high-frost days (min < -10°C) is detected as one coldwave. Confirms the cold path of the algorithm runs independently. |

---

## test_integration.py — Integration (1 test)

| Test | What it checks |
|---|---|
| `test_silver_to_gold_detects_heatwave` | Feeds raw string rows (as they arrive from Bronze CSVs) through `filter_station` → `daily_aggregate` → `detect_heatwaves` and asserts one heatwave is found. Also confirms a row for a different station is filtered out. No network or files — fully in-memory. |
