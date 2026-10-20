# Data dictionary

All sensor names, bounds and thresholds are **synthetic demonstration settings** (`config/sensors.json`, `config/settings.json`), not engineering limits.

## Event fields (schema_version 1.0)
| Field | Meaning |
|---|---|
| event_id | Unique id of the measurement. If blank, derived deterministically (`derived-<hash>`) from run, sensor, timestamp, sequence. |
| campaign_id | Campaign grouping (default `UNASSIGNED`). |
| test_run_id | **Required.** Run identifier. |
| sensor_id | **Required.** Must exist and be enabled in sensor config. |
| event_timestamp | **Required.** ISO-8601; naive timestamps are interpreted as UTC. Never modified. |
| ingestion_timestamp | When the source says it was ingested; used for the lateness check. Replaced by processing time if absent/invalid. |
| sequence_number | Optional integer per (run, sensor); enables exact missing-sample counts. |
| sensor_value | Numeric. **Empty = missing measurement (stored as NULL, never 0).** NaN/inf are invalid. |
| sensor_unit | Must equal the configured unit (blank = configured unit). No conversions are attempted. |
| test_phase | PREPARATION, STARTUP, TEST_WINDOW, SHUTDOWN, POST_TEST (unknown labels stored as UNKNOWN). Software categories only. |
| source_type | e.g. SYNTHETIC. All demo data is SYNTHETIC. |
| schema_version | Only `1.0` is supported; others are quarantined. |
| quality_status | Input: UNCHECKED. Stored: VALID (value present) or MISSING (value absent). Invalid records are not stored as events; they are quarantined. |

New sensor types need only a new entry in `sensors.json` (or the Settings page); no code change.

## Severity vocabulary
INFO, WARNING, ERROR, CRITICAL describe **data-quality/analytics findings only, not engine safety.** Currently emitted: INFO, WARNING, ERROR. CRITICAL is defined but no rule emits it yet.

## Event-level data-quality rules (at ingestion)
| Rule | Outcome | Condition |
|---|---|---|
| DQ_NOT_AN_OBJECT / DQ_UNDECODABLE_PAYLOAD | quarantine | record/message is not a JSON object / not decodable |
| DQ_BLANK_IDENTIFIER / DQ_MISSING_FIELD | quarantine | blank run or sensor id / missing timestamp |
| DQ_UNSUPPORTED_SCHEMA | quarantine | schema_version not supported |
| DQ_INVALID_TIMESTAMP | quarantine | timestamp unparseable |
| DQ_UNKNOWN_SENSOR / DQ_SENSOR_DISABLED | quarantine | sensor not configured / disabled (replayable after fixing config) |
| DQ_NON_NUMERIC_VALUE | quarantine | value not a finite number |
| DQ_UNEXPECTED_UNIT | quarantine | unit differs from configured |
| DQ_INVALID_SEQUENCE | quarantine | sequence not an integer |
| DQ_OUT_OF_BOUNDS | quarantine | value outside configured **validation** bounds (treated as invalid data) |
| DQ_MALFORMED_ROW | quarantine | CSV row field count differs from header |
| DQ_DUPLICATE_EVENT_ID_CONFLICT | quarantine | existing event_id, different content |
| DQ_DUPLICATE_DELIVERY | INFO issue | existing event_id, identical content (ignored) |
| DQ_MISSING_VALUE | WARNING issue | record stored with NULL value |
| DQ_LATE_EVENT | WARNING issue | ingestion - event time > `quality.late_event_seconds` |
| DQ_UNKNOWN_PHASE, DQ_INVALID_INGESTION_TS | INFO/WARNING | label/timestamp normalised |

## Run-level data-quality rules (recomputed by `analyze_run`)
| Rule | Condition |
|---|---|
| DQ_OUT_OF_ORDER | an event arrived with an older event time than the previous arrival of the same sensor |
| DQ_MISSING_SEQUENCE | gaps in sequence numbers (exact count) |
| DQ_TIME_GAP | interval > `gap_factor` x expected interval |
| DQ_IMPLAUSIBLE_INTERVAL | median interval differs from configured by > tolerance (one sensor, or inconsistent across sensors) |
| DQ_RUN_SAMPLE_RATE_DIFFERS | all sensors deviate by a similar factor (INFO) |
| DQ_FLATLINE | >= `flatline_min_samples` identical consecutive values |

## Anomaly rules (findings)
AN_RANGE_EXCEEDED (operating range; valid but unusual), AN_SPIKE, AN_DROP (robust z-score **and** minimum relative change, must revert), AN_FLATLINE, AN_DRIFT, AN_MISSING_BURST (time gap or run of NULL values), AN_INTERVAL_IRREGULAR, AN_CROSS_SENSOR_TIME_MISMATCH, AN_PHASE_TRANSITION (against `expected_phase_order`), AN_INSUFFICIENT_DATA (missing evidence). Review states: OPEN, REVIEWED, FALSE_POSITIVE (with reviewer, note, timestamp).

## Metrics (per run and sensor)
record_count, valid_count (non-null values), missing_value_count, invalid_count (quarantined for that sensor), min, max, mean, median, std (population), first/last timestamp, duration_s, observed_interval_median_s, expected_interval_s (configured), effective_interval_s (configured, or observed if it deviates), **estimated_missing_sample_rate** and **completeness_pct** (estimates from the effective interval; `completeness_basis` says which). Exact counts come only from sequence numbers (DQ_MISSING_SEQUENCE).

## Comparison
Differences are `other - baseline` (first selected run); percent only when baseline != 0. Sensors with differing units are NOT_COMPARABLE (no diffs); differing intervals give CAUTION plus a warning. Elapsed alignment (`align=elapsed`) is display-only.
