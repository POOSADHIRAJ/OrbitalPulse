import os
import tempfile

import pytest

# Isolated data dir for the whole test session (set before any orbitalpulse import touches config).
_TMP = tempfile.mkdtemp(prefix="orbitalpulse_tests_")
os.environ["ORBITALPULSE_DATA_DIR"] = _TMP


@pytest.fixture()
def conn(tmp_path, monkeypatch):
    """Fresh database + data dir per test."""
    monkeypatch.setenv("ORBITALPULSE_DATA_DIR", str(tmp_path))
    from orbitalpulse.storage.db import session
    with session() as c:
        yield c


@pytest.fixture()
def sensors():
    from orbitalpulse.config import load_sensor_config
    return load_sensor_config()


@pytest.fixture()
def settings():
    from orbitalpulse.config import load_settings
    return load_settings()


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("ORBITALPULSE_DATA_DIR", str(tmp_path))
    from fastapi.testclient import TestClient

    from orbitalpulse.api.main import app
    return TestClient(app)


def rec(**over):
    """A valid raw record (CSV-like strings)."""
    base = {"event_id": "E1", "campaign_id": "C1", "test_run_id": "R1", "sensor_id": "chamber_pressure",
            "sequence_number": "1", "event_timestamp": "2026-01-01T00:00:00Z", "ingestion_timestamp": "2026-01-01T00:00:00.050Z",
            "test_phase": "TEST_WINDOW", "sensor_value": "10.5", "sensor_unit": "bar", "source_type": "SYNTHETIC", "schema_version": "1.0"}
    base.update(over)
    return base
