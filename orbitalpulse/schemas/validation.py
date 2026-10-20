"""Event-level validation. Never raises on bad input: returns issues instead.

A rejected record is returned with rule_id/explanation so the caller can
quarantine it with the original payload. Rule IDs are documented in
docs/DATA_DICTIONARY.md.
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from orbitalpulse.config import SensorSpec
from orbitalpulse.schemas.events import (
    REQUIRED_FIELDS,
    SUPPORTED_SCHEMA_VERSIONS,
    Phase,
    QualityStatus,
    Severity,
    TelemetryEvent,
)


@dataclass
class Rejection:
    rule_id: str
    severity: str
    observed: str
    expected: str
    explanation: str


@dataclass
class ValidationResult:
    event: TelemetryEvent | None = None
    rejection: Rejection | None = None
    warnings: list[Rejection] = field(default_factory=list)  # accepted but noteworthy


def _blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def parse_timestamp(value: Any) -> datetime | None:
    """ISO-8601 only. Naive timestamps are interpreted as UTC (documented)."""
    if _blank(value) or isinstance(value, bool):
        return None
    try:
        ts = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def parse_number(value: Any) -> tuple[float | None, bool]:
    """Returns (number, ok). (None, True) = missing measurement; (None, False) = invalid."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None, True
    if isinstance(value, bool):
        return None, False
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None, False
    if not math.isfinite(num):
        return None, False
    return num, True


def derive_event_id(run_id: str, sensor_id: str, ts_iso: str, seq: Any) -> str:
    raw = f"{run_id}|{sensor_id}|{ts_iso}|{seq}"
    return "derived-" + hashlib.sha1(raw.encode()).hexdigest()[:20]


def payload_hash(event: TelemetryEvent) -> str:
    """Hash of the measurement identity, used to tell duplicates from conflicts."""
    raw = "|".join(str(x) for x in (
        event.test_run_id, event.sensor_id, event.event_timestamp.isoformat(),
        event.sequence_number, event.sensor_value, event.sensor_unit, event.test_phase))
    return hashlib.sha1(raw.encode()).hexdigest()


def validate_event(
    raw: Any,
    sensors: dict[str, SensorSpec],
    *,
    default_campaign: str = "UNASSIGNED",
    late_event_seconds: float | None = None,
    ingestion_time: datetime | None = None,
) -> ValidationResult:
    def reject(rule, sev, observed, expected, why) -> ValidationResult:
        return ValidationResult(rejection=Rejection(rule, sev, str(observed)[:300], expected, why))

    if isinstance(raw, dict) and "_undecodable" in raw:
        return reject("DQ_UNDECODABLE_PAYLOAD", Severity.ERROR.value, str(raw["_undecodable"])[:120], "valid JSON object",
                      "Message could not be decoded as a JSON object.")
    if not isinstance(raw, dict):
        return reject("DQ_NOT_AN_OBJECT", Severity.ERROR.value, type(raw).__name__,
                      "JSON object / CSV row", "Record is not a key/value structure.")

    for name in REQUIRED_FIELDS:
        if _blank(raw.get(name)):
            rule = "DQ_BLANK_IDENTIFIER" if name != "event_timestamp" else "DQ_MISSING_FIELD"
            return reject(rule, Severity.ERROR.value, repr(raw.get(name)), f"non-blank {name}",
                          f"Required field '{name}' is missing or blank.")

    version = str(raw.get("schema_version") or "1.0").strip()
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        return reject("DQ_UNSUPPORTED_SCHEMA", Severity.ERROR.value, version,
                      f"one of {SUPPORTED_SCHEMA_VERSIONS}", "Unsupported schema_version.")

    ts = parse_timestamp(raw.get("event_timestamp"))
    if ts is None:
        return reject("DQ_INVALID_TIMESTAMP", Severity.ERROR.value, raw.get("event_timestamp"),
                      "ISO-8601 timestamp", "event_timestamp could not be parsed.")

    run_id = str(raw["test_run_id"]).strip()
    sensor_id = str(raw["sensor_id"]).strip()
    spec = sensors.get(sensor_id)
    if spec is None:
        return reject("DQ_UNKNOWN_SENSOR", Severity.ERROR.value, sensor_id, "sensor listed in sensor config",
                      "Sensor is not defined in the sensor configuration. Add its metadata, then replay.")
    if not spec.enabled:
        return reject("DQ_SENSOR_DISABLED", Severity.WARNING.value, sensor_id, "enabled sensor",
                      "Sensor is disabled in configuration; record preserved in quarantine.")

    value, ok = parse_number(raw.get("sensor_value"))
    if not ok:
        return reject("DQ_NON_NUMERIC_VALUE", Severity.ERROR.value, raw.get("sensor_value"),
                      "finite number or empty (missing)", "sensor_value is not a finite number.")

    unit = str(raw.get("sensor_unit") or "").strip() or spec.unit
    if unit != spec.unit:
        return reject("DQ_UNEXPECTED_UNIT", Severity.ERROR.value, unit, spec.unit,
                      f"Unit '{unit}' differs from configured unit '{spec.unit}'. No conversion is attempted.")

    seq_raw = raw.get("sequence_number")
    seq: int | None = None
    if not _blank(seq_raw):
        try:
            seq_f = float(str(seq_raw))
            if not seq_f.is_integer() or isinstance(seq_raw, bool):
                raise ValueError
            seq = int(seq_f)
        except (TypeError, ValueError):
            return reject("DQ_INVALID_SEQUENCE", Severity.ERROR.value, seq_raw, "integer",
                          "sequence_number is not an integer.")

    if value is not None:
        lo, hi = spec.validation_min, spec.validation_max
        if (lo is not None and value < lo) or (hi is not None and value > hi):
            return reject("DQ_OUT_OF_BOUNDS", Severity.ERROR.value, value, f"[{lo}, {hi}] {spec.unit}",
                          "Value is outside the configured validation bounds (synthetic demo settings) "
                          "and is treated as invalid data, not as a measurement.")

    ing_ts = parse_timestamp(raw.get("ingestion_timestamp"))
    warnings: list[Rejection] = []
    if _blank(raw.get("ingestion_timestamp")) is False and ing_ts is None:
        warnings.append(Rejection("DQ_INVALID_INGESTION_TS", Severity.WARNING.value,
                                  str(raw.get("ingestion_timestamp")), "ISO-8601",
                                  "ingestion_timestamp unparseable; replaced by processing time."))
    ing_ts = ing_ts or ingestion_time or datetime.now(UTC)
    if late_event_seconds is not None and raw.get("ingestion_timestamp") and ing_ts:
        delay = (ing_ts - ts).total_seconds()
        if delay > late_event_seconds:
            warnings.append(Rejection("DQ_LATE_EVENT", Severity.WARNING.value, f"{delay:.1f}s delay",
                                      f"<= {late_event_seconds}s",
                                      "Event arrived later than the configured lateness allowance."))

    phase = str(raw.get("test_phase") or Phase.UNKNOWN.value).strip().upper()
    if phase not in Phase.__members__:
        warnings.append(Rejection("DQ_UNKNOWN_PHASE", Severity.INFO.value, phase, "known phase label",
                                  "Unrecognised test_phase label; stored as UNKNOWN."))
        phase = Phase.UNKNOWN.value

    event_id = str(raw.get("event_id") or "").strip() or derive_event_id(run_id, sensor_id, ts.isoformat(), seq)
    if value is None:
        warnings.append(Rejection("DQ_MISSING_VALUE", Severity.WARNING.value, "empty", "numeric value",
                                  "Record present but measurement is missing (stored as NULL, not zero)."))

    event = TelemetryEvent(
        event_id=event_id,
        campaign_id=str(raw.get("campaign_id") or "").strip() or default_campaign,
        test_run_id=run_id, sensor_id=sensor_id, event_timestamp=ts, ingestion_timestamp=ing_ts,
        sequence_number=seq, sensor_value=value, sensor_unit=unit, test_phase=phase,
        source_type=str(raw.get("source_type") or "UNKNOWN").strip().upper(),
        schema_version=version,
        quality_status=(QualityStatus.MISSING if value is None else QualityStatus.VALID).value,
    )
    return ValidationResult(event=event, warnings=warnings)
