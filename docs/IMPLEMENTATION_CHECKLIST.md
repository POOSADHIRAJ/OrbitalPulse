# Implementation checklist (baseline inspection)

Inspection of the uploaded `OrbitalPulse.zip` before any change.

## 1. What already existed and worked
- `orbitalpulse.py` (+ `anomaly_detector.py`): a **NASA C-MAPSS turbofan** pipeline (Kafka producer/consumer, validation, MAD/freeze anomaly detection, JSONL outputs). It is not the rocket-telemetry platform described in the brief, and it depended on a dataset path (`Bhramastra\data`) not included in the zip.
- 8 pytest tests for the anomaly detector (pass) + 3 script-style checks (validator, source audit, quality summary; pass).
- Kafka 4.1.2 distribution unpacked in the repo (valid KRaft broker, Java 21 works).
- Sample outputs in `output/`, a README for the turbofan pipeline.

## 2. Existing but incomplete or broken
- Kafka config logged to `/tmp/kraft-combined-logs` (does not suit Windows); shell scripts lacked exec bits.
- Producer, consumer and analytics lived in one file; no API, UI, storage, config, schema versioning, quarantine store, run comparison, reports.
- No `.gitignore` entries for runtime data; git repo had no commits.

## 3. What was missing
Everything in the brief: synthetic rocket-test generator, canonical schema, CSV ingestion, SQLite store + raw archive, quality engine and quarantine, explainable rule engine, run comparison, timeline, reports, REST API, dashboard, tests, docs, Windows scripts.

## 4. Plan executed (smallest path to a working end-to-end app)
1. Preserve legacy work in `legacy_cmapss/` (tests re-verified there).
2. Vertical slice: simulator -> validation -> SQLite + raw archive -> API -> dashboard.
3. Quality rules + anomaly rules; run comparison, timeline, reports.
4. Kafka producer/consumer with honest fallback; verified against a real local broker.
5. Tests, lint, type-check, docs, scripts.
