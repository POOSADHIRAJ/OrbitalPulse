
from anomaly_detector import detect_anomalies


def make_record(cycle, sensor_value, engine_id=1, valid=True):
    values = [engine_id, cycle, 0.1, 0.2, 0.3] + [10.0] * 21
    values[5] = sensor_value

    return {
        "valid": valid,
        "source": "synthetic_test",
        "source_line": cycle,
        "values": values,
    }


def test_detects_synthetic_spike():
    records = [
        make_record(cycle, 10.0)
        for cycle in range(1, 12)
    ]
    records.append(make_record(12, 100.0))

    alerts = detect_anomalies(records)

    assert any(
        alert["sensor"] == "sensor_column_5"
        and alert["anomaly_type"] == "statistical_outlier"
        for alert in alerts
    )


def test_constant_sensor_does_not_trigger_false_freeze():
    records = [
        make_record(cycle, 10.0)
        for cycle in range(1, 16)
    ]

    alerts = detect_anomalies(records)

    assert not any(
        alert["sensor"] == "sensor_column_5"
        and alert["anomaly_type"] == "possible_frozen_sensor"
        for alert in alerts
    )
    assert not any(
        alert["anomaly_type"] == "statistical_outlier"
        for alert in alerts
    )


def test_detects_repeated_values_after_variation():
    sensor_values = [10.0, 12.0, 9.0, 11.0] + [11.0] * 8

    records = [
        make_record(cycle, value)
        for cycle, value in enumerate(sensor_values, start=1)
    ]

    alerts = detect_anomalies(records)

    freeze_alerts = [
        alert for alert in alerts
        if alert["sensor"] == "sensor_column_5"
        and alert["anomaly_type"] == "possible_frozen_sensor"
    ]

    assert len(freeze_alerts) == 1
    assert freeze_alerts[0]["consecutive_identical_readings"] == 8


def test_ignores_invalid_records():
    records = [
        make_record(cycle, 10.0, valid=False)
        for cycle in range(1, 20)
    ]

    assert detect_anomalies(records) == []


def test_engine_histories_are_separate():
    records = [
        make_record(cycle, 10.0, engine_id=1)
        for cycle in range(1, 12)
    ]

    records.extend(
        make_record(cycle, 100.0, engine_id=2)
        for cycle in range(1, 12)
    )

    records.append(make_record(12, 80.0, engine_id=1))

    alerts = detect_anomalies(records)

    assert any(
        alert["engine_id"] == 1
        and alert["sensor"] == "sensor_column_5"
        and alert["anomaly_type"] == "statistical_outlier"
        for alert in alerts
    )


def test_ignores_tiny_deviation_when_mad_is_near_zero():
    values = [2388.00, 2388.01, 2388.00, 2388.01] * 3

    records = [
        make_record(cycle, value)
        for cycle, value in enumerate(values, start=1)
    ]

    records.append(make_record(13, 2388.06))

    alerts = detect_anomalies(records)

    assert not any(
        alert["sensor"] == "sensor_column_5"
        and alert["anomaly_type"] == "statistical_outlier"
        for alert in alerts
    )


def test_near_zero_mad_still_flags_large_deviation():
    records = [
        make_record(cycle, 10.0)
        for cycle in range(1, 12)
    ]

    records.append(make_record(12, 12.0))

    alerts = detect_anomalies(records)

    assert any(
        alert["sensor"] == "sensor_column_5"
        and alert["anomaly_type"] == "statistical_outlier"
        and alert["rule"] == "near_zero_mad_relative_tolerance"
        for alert in alerts
    )


def test_small_variations_do_not_trigger_fallback_alerts():
    values = [
        10.00, 10.01, 9.99, 10.02, 10.00,
        9.98, 10.01, 10.00, 10.02, 9.99,
        10.01, 10.00, 9.99, 10.02, 10.01,
    ]

    records = [
        make_record(cycle, value)
        for cycle, value in enumerate(values, start=1)
    ]

    alerts = detect_anomalies(records)

    assert not any(
        alert["sensor"] == "sensor_column_5"
        and alert["anomaly_type"] == "statistical_outlier"
        for alert in alerts
    )


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
