import pytest

from orbitalpulse.schemas.validation import parse_number, parse_timestamp, validate_event
from tests.conftest import rec


def check(raw, sensors):
    return validate_event(raw, sensors.by_id())


def test_valid_event_parses(sensors):
    r = check(rec(), sensors)
    assert r.event and not r.rejection
    assert r.event.sensor_value == 10.5 and r.event.quality_status == "VALID" and r.event.sequence_number == 1


@pytest.mark.parametrize("field", ["test_run_id", "sensor_id", "event_timestamp"])
def test_missing_or_blank_required_fields_rejected(sensors, field):
    for bad in ("", "   ", None):
        r = check(rec(**{field: bad}), sensors)
        assert r.rejection and r.rejection.rule_id in ("DQ_BLANK_IDENTIFIER", "DQ_MISSING_FIELD")


def test_invalid_timestamp(sensors):
    assert check(rec(event_timestamp="not-a-time"), sensors).rejection.rule_id == "DQ_INVALID_TIMESTAMP"
    assert parse_timestamp("2026-01-01T00:00:00") is not None  # naive treated as UTC
    assert parse_timestamp(True) is None


@pytest.mark.parametrize("val", ["abc", "NaN", "inf", "-inf", "1,5"])
def test_non_numeric_values_rejected(sensors, val):
    assert check(rec(sensor_value=val), sensors).rejection.rule_id == "DQ_NON_NUMERIC_VALUE"


def test_missing_value_is_not_zero(sensors):
    r = check(rec(sensor_value=""), sensors)
    assert r.event.sensor_value is None and r.event.quality_status == "MISSING"
    assert any(w.rule_id == "DQ_MISSING_VALUE" for w in r.warnings)
    z = check(rec(sensor_value="0"), sensors)
    assert z.event.sensor_value == 0.0 and z.event.quality_status == "VALID"
    assert parse_number("") == (None, True) and parse_number("x") == (None, False)


def test_unexpected_unit_rejected_without_conversion(sensors):
    r = check(rec(sensor_unit="psi"), sensors)
    assert r.rejection.rule_id == "DQ_UNEXPECTED_UNIT" and "No conversion" in r.rejection.explanation


def test_blank_unit_defaults_to_configured(sensors):
    assert check(rec(sensor_unit=""), sensors).event.sensor_unit == "bar"


def test_out_of_validation_bounds(sensors):
    assert check(rec(sensor_value="9999"), sensors).rejection.rule_id == "DQ_OUT_OF_BOUNDS"


def test_unknown_and_disabled_sensor(sensors):
    assert check(rec(sensor_id="mystery"), sensors).rejection.rule_id == "DQ_UNKNOWN_SENSOR"
    sensors.sensors[0].enabled = False
    assert check(rec(), sensors).rejection.rule_id == "DQ_SENSOR_DISABLED"


def test_unsupported_schema_and_bad_sequence(sensors):
    assert check(rec(schema_version="9.9"), sensors).rejection.rule_id == "DQ_UNSUPPORTED_SCHEMA"
    assert check(rec(sequence_number="abc"), sensors).rejection.rule_id == "DQ_INVALID_SEQUENCE"
    assert check(rec(sequence_number="1.5"), sensors).rejection.rule_id == "DQ_INVALID_SEQUENCE"


def test_non_dict_and_undecodable(sensors):
    assert check("string", sensors).rejection.rule_id == "DQ_NOT_AN_OBJECT"
    assert check({"_undecodable": "x"}, sensors).rejection.rule_id == "DQ_UNDECODABLE_PAYLOAD"


def test_late_event_warning_and_derived_id(sensors):
    r = validate_event(rec(ingestion_timestamp="2026-01-01T00:01:00Z", event_id=""), sensors.by_id(), late_event_seconds=5)
    assert any(w.rule_id == "DQ_LATE_EVENT" for w in r.warnings) and r.event.event_id.startswith("derived-")
