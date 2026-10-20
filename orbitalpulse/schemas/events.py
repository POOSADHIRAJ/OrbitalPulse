"""Canonical, versioned telemetry event schema (schema_version 1.0)."""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class Phase(str, Enum):
    PREPARATION = "PREPARATION"
    STARTUP = "STARTUP"
    TEST_WINDOW = "TEST_WINDOW"
    SHUTDOWN = "SHUTDOWN"
    POST_TEST = "POST_TEST"
    UNKNOWN = "UNKNOWN"


class QualityStatus(str, Enum):
    UNCHECKED = "UNCHECKED"  # as produced by a source
    VALID = "VALID"          # passed event-level validation, value present
    MISSING = "MISSING"      # valid record, but no measurement (NULL, never 0)


class Severity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


SEVERITY_ORDER = {s.value: i for i, s in enumerate(Severity)}

REQUIRED_FIELDS = ("test_run_id", "sensor_id", "event_timestamp")
ALL_FIELDS = (
    "event_id", "campaign_id", "test_run_id", "sensor_id", "event_timestamp",
    "ingestion_timestamp", "sequence_number", "sensor_value", "sensor_unit",
    "test_phase", "source_type", "schema_version", "quality_status",
)
SUPPORTED_SCHEMA_VERSIONS = ("1.0",)


class TelemetryEvent(BaseModel):
    event_id: str
    campaign_id: str
    test_run_id: str
    sensor_id: str
    event_timestamp: datetime
    ingestion_timestamp: datetime | None = None
    sequence_number: int | None = None
    sensor_value: float | None = Field(default=None, description="None = missing measurement (not zero)")
    sensor_unit: str
    test_phase: str = Phase.UNKNOWN.value
    source_type: str = "SYNTHETIC"
    schema_version: str = "1.0"
    quality_status: str = QualityStatus.UNCHECKED.value
