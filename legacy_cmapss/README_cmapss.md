# OrbitalPulse

A Python and Apache Kafka streaming pipeline for NASA C-MAPSS simulated turbofan-engine telemetry.

OrbitalPulse demonstrates a small end-to-end data engineering workflow: ingest structured sensor records, stream them through Kafka, validate consumed messages, audit rejected source rows, and report data-quality metrics.

## Architecture

```text
NASA C-MAPSS source file
          |
          v
   Python ingestion
          |
          +----> Source rejection audit (JSONL)
          |
          v
   Kafka producer
          |
          v
   engine-telemetry
          |
          v
   Kafka consumer
          |
          v
   Record validation
          |
          +----> Consumed telemetry (JSONL)
          |
          +----> Data-quality summary (JSON)
```

## Features

- Reads up to 100 non-empty source rows from `test_FD001.txt`.
- Parses numeric telemetry records while tracking source line numbers.
- Records malformed source rows and rejection reasons in a separate audit file.
- Publishes JSON messages to the Kafka topic `engine-telemetry`.
- Consumes messages from Kafka and validates their structure and values.
- Rejects invalid field counts, non-numeric values, booleans used as numbers, NaN, and infinity.
- Writes consumed telemetry and source rejection audits as JSON Lines.
- Produces a machine-readable quality summary with source, streaming, and validation count checks.
- Records the pipeline status and error information in the quality summary.

## Technology

- Python 3
- Apache Kafka 4.1.2 in KRaft mode
- `kafka-python` 3.0.11
- JSON and JSON Lines (JSONL)
- Java 21

## Prerequisites

- Windows and PowerShell
- Java 21
- Apache Kafka 4.1.2 configured for local use
- Python 3 and a virtual environment
- The NASA C-MAPSS dataset, specifically `test_FD001.txt`

The source dataset is external to this project and is not included in the repository.

The current local configuration expects the dataset at:

`C:\Users\Admin\Desktop\Bhramastra\data\test_FD001.txt`

Update the `DATASET` path in `orbitalpulse.py` if your dataset is stored elsewhere.

## Local setup

### 1. Create the Python environment

From the project directory:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r .\requirements.txt
```

### 2. Start Kafka

Open a separate PowerShell terminal and start the local Kafka broker:

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"

Set-Location C:\k\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Keep the broker terminal open while running OrbitalPulse. The local broker configuration and the `engine-telemetry` topic must be available.

### 3. Run the pipeline

In the project terminal:

```powershell
Set-Location C:\Users\Admin\Desktop\PRATICE\OrbitalPulse
python .\orbitalpulse.py
```

A successful test run with the current 100-row limit should report:

- 100 source rows parsed
- 100 messages published
- 100 messages consumed
- 100 valid records
- 0 invalid records
- `Pipeline status: SUCCESS`
- `Count reconciliation: True`

Exact results depend on the source data and the current run.

## Test suite

Run the tests from the project directory:

```powershell
python .\test_validator.py
python .\test_source_audit.py
python .\test_quality_summary.py
```

### What the tests cover

**`test_validator.py` — 8 cases**

Tests valid numeric records and invalid field counts, data types, non-finite values, and record structure.

**`test_source_audit.py` — 6 checks**

Tests synthetic valid rows, blank-line handling, malformed-row rejection, and preservation of source line numbers, raw contents, and rejection reasons.

**`test_quality_summary.py` — 3 checks**

Tests successful count reconciliation, preservation of failure status when counts match, and detection of a count mismatch.

These tests use synthetic test cases where appropriate. The summary tests do not simulate a full Kafka broker failure or recovery.

## Generated output files

The pipeline writes these files to `output/`:

| File | Purpose |
|---|---|
| `consumed_telemetry.jsonl` | Consumed records and validation results |
| `rejected_source_rows.jsonl` | Source rows rejected during parsing |
| `data_quality_summary.json` | Run status, record counts, errors, and reconciliation checks |

The rejection audit can be empty when all examined source rows parse successfully.

### Verify the saved output

After a successful run, execute:

```powershell
python -c "import json; from pathlib import Path; p=Path('output/consumed_telemetry.jsonl'); rows=[json.loads(x) for x in p.read_text(encoding='utf-8').splitlines() if x.strip()]; print('Records:', len(rows)); print('All valid:', all(r['valid'] for r in rows)); print('Distinct source lines:', len({r['source_line'] for r in rows}))"
```

For the current successful 100-record run, the expected output is:

```text
Records: 100
All valid: True
Distinct source lines: 100
```

## Data-quality checks

The quality summary checks three count relationships:

1. Examined non-empty source rows = parsed rows + rejected source rows.
2. Published messages = consumed messages.
3. Consumed messages = valid records + invalid records.

These checks help identify discrepancies between pipeline stages. Count reconciliation alone does not establish that a run succeeded; inspect the explicit pipeline status as well.

## Current limitations

- The pipeline currently processes a maximum of 100 non-empty source rows per run.
- It uses a local Kafka broker and is intended as a proof of concept.
- The validation rules check record structure and numeric validity, not physical sensor plausibility.
- The project does not yet implement production deployment, distributed fault recovery, persistent monitoring, or automated retries across pipeline stages.
- A successful run on simulated data does not establish that an engine is healthy or safe to operate.

## Project status

- [x] Read C-MAPSS telemetry records
- [x] Audit malformed source rows
- [x] Publish records to Kafka
- [x] Consume Kafka messages
- [x] Validate consumed records
- [x] Write JSONL output and rejection audit
- [x] Generate a machine-readable data-quality summary
- [x] Reconcile source, streaming, and validation counts
- [x] Test validator, source audit, and summary logic
- [ ] Add a full Kafka failure-and-recovery integration test
- [ ] Add dependency pinning and a reproducible project configuration
- [ ] Document a clean setup on another machine

## Scope

OrbitalPulse is a local data engineering proof of concept using simulated turbofan-engine telemetry. It is not a live aircraft telemetry system, a predictive maintenance model, or a production deployment.

