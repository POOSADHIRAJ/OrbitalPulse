# Runbook (Windows PowerShell, from the project root)

## Start / stop
| Task | Command |
|---|---|
| One-time setup | `.\scripts\setup.ps1` |
| Demo mode (import + dashboard) | `.\scripts\run_demo.ps1` then open http://127.0.0.1:8000 |
| Stop | `Ctrl+C` in that window |
| Reset all data + re-import demo | `.\scripts\reset_demo.ps1` |
| Only import demo | `.\.venv\Scripts\python scripts\op.py import-demo` |
| Re-run analysis on stored runs | `.\.venv\Scripts\python scripts\op.py analyze` |
| Tests + lint | `.\scripts\test.ps1` |

Data lives in `data/` (SQLite `orbitalpulse.db`, `raw/`, `demo/`, `kafka/`, `config/` overrides). Set `ORBITALPULSE_DATA_DIR` to relocate it. **Retention:** nothing is deleted automatically; `reset` removes DB rows, raw archive and demo CSVs (config overrides stay; use Settings > Reset to restore shipped config).

## Kafka mode
1. Terminal 1: `.\scripts\kafka_start.ps1` (needs Java; uses `$env:KAFKA_HOME`, else `.\kafka_2.13-4.1.2`, else `C:\k\kafka`). First run formats KRaft storage under `data\kafka`.
2. Terminal 2: `.\scripts\run_demo.ps1` (dashboard).
3. Terminal 3: `.\scripts\kafka_demo.ps1` (starts the consumer, publishes RUN-001 and RUN-002 with 2% duplicate redelivery). Or use the Ingestion page buttons.
4. Dashboard header changes to "Live Kafka mode" only while a broker answers **and** the consumer heartbeat is fresh (<15 s).

Check connectivity: `Test-NetConnection localhost -Port 9092`, or `GET /api/status`.

## Troubleshooting
| Symptom | Cause / fix |
|---|---|
| "Kafka broker not reachable" | Broker not running or wrong `kafka.bootstrap_servers` in `config/settings.json`. Batch/demo mode is unaffected. |
| `pip install` fails on Python 3.14 | A dependency may lack a 3.14 wheel. Use 3.12/3.13: `py -3.12 -m venv .venv`. |
| Import returns 409 | That exact file was already imported (by design). Use **Reset** to start over. |
| Findings missing after editing thresholds | Saving in Settings re-analyses all runs; or `scripts\op.py analyze`. |
| Kafka consumer shows no lag | Lag appears only while the consumer is running and assigned partitions. |
| Port 8000 busy | `python scripts\op.py serve --port 8001`. |
| `database is locked` | Close other processes writing the DB (WAL + 30 s busy timeout are set). |
| Kafka storage error after moving folders | Delete `data\kafka` and start again (this erases broker state, not the SQLite data). |
| A test fails | `python -m pytest -x -rs` for the first failure; Kafka tests skip (with reason) when no broker is up. |

Replay quarantined data: fix the cause (e.g. add the sensor in Settings), then `POST /api/quarantine/replay?rule_id=DQ_UNKNOWN_SENSOR` (see `/docs`). Re-delivered/replayed Kafka messages are safe: storage is idempotent.
