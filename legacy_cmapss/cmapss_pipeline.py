
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer

from anomaly_detector import detect_anomalies


# ==================================================
# 1. CONFIGURATION
# ==================================================

BOOTSTRAP = "localhost:9092"
TOPIC = "engine-telemetry"

# Original C-MAPSS dataset: read-only.
DATASET = Path(
    r"C:\Users\Admin\Desktop\Bhramastra\data\test_FD001.txt"
)

OUTPUT = Path("output")
LIMIT = 100

# C-MAPSS layout:
# engine ID, cycle, 3 operating settings, 21 sensors.
SENSOR_START_INDEX = 5

KAFKA_ASSIGNMENT_TIMEOUT = 15
KAFKA_CONSUME_TIMEOUT = 30


# ==================================================
# 2. OUTPUT HELPERS
# ==================================================

def write_jsonl(path, records):
    """Write records as one JSON object per line."""
    with path.open("w", encoding="utf-8", newline="\n") as file:
        for record in records:
            file.write(json.dumps(record, allow_nan=False) + "\n")


def write_json(path, value):
    """Write a formatted JSON document."""
    with path.open("w", encoding="utf-8") as file:
        json.dump(value, file, indent=2, allow_nan=False)
        file.write("\n")


# ==================================================
# 3. SOURCE INGESTION AND AUDITING
# ==================================================

def read_rows():
    """
    Read up to LIMIT non-empty source lines.

    Returns:
        rows: successfully parsed numeric records
        rejected_rows: source lines that could not be parsed
        blank_lines: number of blank lines skipped
        examined_rows: number of non-empty lines examined
    """
    rows = []
    rejected_rows = []
    blank_lines = 0
    examined_rows = 0

    with DATASET.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            parts = line.split()

            # Blank lines are skipped and do not count toward LIMIT.
            if not parts:
                blank_lines += 1
                continue

            # Stop after examining LIMIT non-empty lines.
            if examined_rows >= LIMIT:
                break

            examined_rows += 1

            try:
                values = [float(part) for part in parts]

                if not all(math.isfinite(value) for value in values):
                    raise ValueError("Non-finite numeric value found")

            except ValueError as error:
                rejected_rows.append({
                    "source": DATASET.name,
                    "source_line": line_number,
                    "reason": (
                        "Source line contains a value that cannot be parsed "
                        "as a finite number"
                    ),
                    "error_detail": str(error),
                    "raw_line": line.rstrip("\r\n"),
                })
                continue

            rows.append({
                "source": DATASET.name,
                "source_line": line_number,
                "values": values,
            })

    if examined_rows == 0:
        raise RuntimeError(
            f"No non-empty source rows found in {DATASET}"
        )

    return rows, rejected_rows, blank_lines, examined_rows


# ==================================================
# 4. RECORD VALIDATION
# ==================================================

def validate_record(record, expected_field_count):
    """Validate the structure and numeric values of a record."""
    errors = []

    if not isinstance(record, dict):
        return False, ["Record must be a dictionary"]

    values = record.get("values")

    if not isinstance(values, list):
        return False, ["'values' must be a list"]

    if len(values) != expected_field_count:
        errors.append(
            f"Expected {expected_field_count} fields, got {len(values)}"
        )

    for index, value in enumerate(values):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            errors.append(f"Field {index} must be numeric")
        elif not math.isfinite(value):
            errors.append(f"Field {index} must be finite")

    return len(errors) == 0, errors


# ==================================================
# 5. DATA-QUALITY SUMMARY
# ==================================================

def build_quality_summary(
    *,
    source_file,
    examined_rows,
    blank_lines,
    parsed_rows,
    source_rejected_rows,
    published_rows,
    consumed_rows,
    valid_rows,
    invalid_rows,
    status,
    error,
    anomaly_count=0,
):
    """Build an auditable summary with reconciled record counts."""

    source_reconciles = (
        examined_rows == parsed_rows + source_rejected_rows
    )

    streaming_reconciles = published_rows == consumed_rows

    validation_reconciles = (
        consumed_rows == valid_rows + invalid_rows
    )

    all_reconcile = (
        source_reconciles
        and streaming_reconciles
        and validation_reconciles
    )

    return {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": source_file,
        "topic": TOPIC,
        "configured_row_limit": LIMIT,
        "counts": {
            "non_empty_source_rows_examined": examined_rows,
            "blank_lines_skipped": blank_lines,
            "rows_parsed_successfully": parsed_rows,
            "source_rows_rejected": source_rejected_rows,
            "records_published": published_rows,
            "records_consumed": consumed_rows,
            "valid_records": valid_rows,
            "invalid_records": invalid_rows,
            "anomaly_alerts": anomaly_count,
        },
        "checks": {
            "source_counts_reconcile": source_reconciles,
            "published_equals_consumed": streaming_reconciles,
            "consumed_equals_valid_plus_invalid": validation_reconciles,
            "all_counts_reconcile": all_reconcile,
        },
        "status": status,
        "error": error,
    }


# ==================================================
# 6. KAFKA CONSUMER AND PRODUCER
# ==================================================

def stream_records(rows, expected_field_count):
    """
    Publish records to Kafka, consume them, and validate them.

    Returns:
        received: consumed and validated records
        published_rows: confirmed published record count
        valid_rows: number of valid records
        invalid_rows: number of invalid records
    """
    producer = None
    consumer = None

    received = []
    published_rows = 0

    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda obj: json.dumps(
                obj,
                allow_nan=False,
            ).encode("utf-8"),
            acks="all",
            retries=3,
        )

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            value_deserializer=lambda data: json.loads(
                data.decode("utf-8")
            ),
            group_id=f"orbitalpulse-{int(time.time())}",
        )

        # Wait until the consumer is assigned a topic partition.
        deadline = time.time() + KAFKA_ASSIGNMENT_TIMEOUT

        while not consumer.assignment() and time.time() < deadline:
            consumer.poll(timeout_ms=500)

        if not consumer.assignment():
            raise RuntimeError(
                "Kafka consumer received no partition assignment "
                f"within {KAFKA_ASSIGNMENT_TIMEOUT} seconds."
            )

        print(f"Kafka broker: {BOOTSTRAP}")
        print(f"Kafka topic: {TOPIC}")
        print(f"Assigned partitions: {consumer.assignment()}")

        # Publish every parsed source record.
        futures = [producer.send(TOPIC, row) for row in rows]
        producer.flush(timeout=20)

        for future in futures:
            future.get(timeout=10)
            published_rows += 1

        print(f"Published records: {published_rows}")

        # Consume records until all published messages arrive or timeout.
        deadline = time.time() + KAFKA_CONSUME_TIMEOUT

        while (
            len(received) < published_rows
            and time.time() < deadline
        ):
            batches = consumer.poll(timeout_ms=1000)

            for messages in batches.values():
                for message in messages:
                    record = message.value

                    valid, errors = validate_record(
                        record,
                        expected_field_count,
                    )

                    values = (
                        record.get("values")
                        if isinstance(record, dict)
                        else None
                    )

                    received.append({
                        "source": (
                            record.get("source")
                            if isinstance(record, dict)
                            else None
                        ),
                        "source_line": (
                            record.get("source_line")
                            if isinstance(record, dict)
                            else None
                        ),
                        "field_count": (
                            len(values)
                            if isinstance(values, list)
                            else 0
                        ),
                        "valid": valid,
                        "validation_errors": errors,
                        "values": values,
                    })

                    if len(received) >= published_rows:
                        break

                if len(received) >= published_rows:
                    break

        valid_rows = sum(1 for record in received if record["valid"])
        invalid_rows = len(received) - valid_rows

        print(f"Consumed records: {len(received)}/{published_rows}")
        print(f"Valid records: {valid_rows}")
        print(f"Invalid records: {invalid_rows}")

        if len(received) != published_rows:
            raise RuntimeError(
                "Record-count mismatch: "
                f"published {published_rows}, consumed {len(received)}."
            )

        if invalid_rows:
            raise RuntimeError(
                f"{invalid_rows} record(s) failed validation."
            )

        return received, published_rows, valid_rows, invalid_rows

    finally:
        if consumer is not None:
            consumer.close()

        if producer is not None:
            producer.close()


# ==================================================
# 7. MAIN PIPELINE
# ==================================================

def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)

    result_path = OUTPUT / "consumed_telemetry.jsonl"
    source_reject_path = OUTPUT / "rejected_source_rows.jsonl"
    anomaly_path = OUTPUT / "anomaly_report.jsonl"
    summary_path = OUTPUT / "data_quality_summary.json"

    rows = []
    rejected_source_rows = []
    received = []
    anomaly_alerts = []

    examined_rows = 0
    blank_lines = 0
    parsed_rows = 0
    published_rows = 0
    valid_rows = 0
    invalid_rows = 0

    status = "FAILED"
    error_message = None

    try:
        # Step 1: Verify and read the original dataset.
        if not DATASET.is_file():
            raise FileNotFoundError(f"Dataset not found: {DATASET}")

        (
            rows,
            rejected_source_rows,
            blank_lines,
            examined_rows,
        ) = read_rows()

        parsed_rows = len(rows)
        source_rejected_count = len(rejected_source_rows)

        write_jsonl(source_reject_path, rejected_source_rows)

        print("\n=== SOURCE AUDIT ===")
        print(f"Dataset: {DATASET}")
        print(f"Non-empty rows examined: {examined_rows}")
        print(f"Blank lines skipped: {blank_lines}")
        print(f"Successfully parsed: {parsed_rows}")
        print(f"Source rows rejected: {source_rejected_count}")
        print(f"Source audit file: {source_reject_path.resolve()}")

        if not rows:
            raise RuntimeError(
                "No valid numeric records are available to publish."
            )

        expected_field_count = len(rows[0]["values"])

        if any(
            len(row["values"]) != expected_field_count
            for row in rows
        ):
            raise RuntimeError(
                "Source rows have inconsistent field counts. "
                "Inspect the source data before streaming."
            )

        print(f"Fields per record: {expected_field_count}")

        # Step 2: Publish, consume, and validate through Kafka.
        print("\n=== KAFKA STREAMING ===")

        (
            received,
            published_rows,
            valid_rows,
            invalid_rows,
        ) = stream_records(rows, expected_field_count)

        write_jsonl(result_path, received)
        print(f"Consumed telemetry saved: {result_path.resolve()}")

        # Step 3: Run explainable screening rules on valid records.
        print("\n=== ANOMALY SCREENING ===")

        anomaly_alerts = detect_anomalies(
            received,
            sensor_start_index=SENSOR_START_INDEX,
        )

        write_jsonl(anomaly_path, anomaly_alerts)

        print(f"Anomaly alerts: {len(anomaly_alerts)}")
        print(f"Anomaly report saved: {anomaly_path.resolve()}")

        # Step 4: Mark the run successful only after pipeline checks pass.
        status = "SUCCESS"
        print("\nPASS: End-to-end Kafka pipeline succeeded.")

    except Exception as error:
        error_message = f"{type(error).__name__}: {error}"
        status = "FAILED"
        raise

    finally:
        # Keep audit artifacts available even if a later step fails.
        write_jsonl(source_reject_path, rejected_source_rows)
        write_jsonl(result_path, received)
        write_jsonl(anomaly_path, anomaly_alerts)

        consumed_rows = len(received)
        valid_rows = sum(1 for record in received if record["valid"])
        invalid_rows = consumed_rows - valid_rows

        summary = build_quality_summary(
            source_file=DATASET.name,
            examined_rows=examined_rows,
            blank_lines=blank_lines,
            parsed_rows=parsed_rows,
            source_rejected_rows=len(rejected_source_rows),
            published_rows=published_rows,
            consumed_rows=consumed_rows,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            status=status,
            error=error_message,
            anomaly_count=len(anomaly_alerts),
        )

        write_json(summary_path, summary)

        print("\n=== FINAL RUN SUMMARY ===")
        print(f"Status: {summary['status']}")
        print(
            "All count checks reconcile: "
            f"{summary['checks']['all_counts_reconcile']}"
        )
        print(f"Summary file: {summary_path.resolve()}")


if __name__ == "__main__":
    main()
