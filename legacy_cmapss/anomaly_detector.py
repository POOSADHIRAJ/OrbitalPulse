
from collections import defaultdict, deque
from statistics import median
import math


def detect_anomalies(
    records,
    sensor_start_index=5,
    history_size=25,
    min_history=8,
    robust_z_threshold=6.0,
    freeze_run_length=8,
    relative_mad_floor=0.001,
    fallback_tolerance_pct=0.01,
):
    """
    Demonstration anomaly screening for C-MAPSS-style records.

    This uses recent sensor history to identify unusual readings
    and repeated identical readings.

    These are configurable screening rules, not validated
    engineering limits, fault diagnoses, or safety decisions.
    """

    if history_size < 1:
        raise ValueError("history_size must be at least 1")

    if min_history < 1:
        raise ValueError("min_history must be at least 1")

    if freeze_run_length < 2:
        raise ValueError("freeze_run_length must be at least 2")

    if robust_z_threshold <= 0:
        raise ValueError("robust_z_threshold must be positive")

    if relative_mad_floor < 0:
        raise ValueError("relative_mad_floor cannot be negative")

    if fallback_tolerance_pct < 0:
        raise ValueError("fallback_tolerance_pct cannot be negative")

    history = defaultdict(lambda: deque(maxlen=history_size))
    last_values = {}
    same_value_runs = defaultdict(int)
    alerts = []

    for record in records:
        if not isinstance(record, dict) or not record.get("valid"):
            continue

        values = record.get("values")

        if not isinstance(values, list) or not values:
            continue

        engine_id = values[0]

        for index in range(sensor_start_index, len(values)):
            value = values[index]

            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                continue

            key = (engine_id, index)
            prior = history[key]
            previous = last_values.get(key)

            if previous is not None and value == previous:
                same_value_runs[key] += 1
            else:
                same_value_runs[key] = 1

            # Check for a possible frozen sensor.
            if (
                same_value_runs[key] == freeze_run_length
                and len(set(prior)) > 1
            ):
                alerts.append({
                    "source": record.get("source"),
                    "source_line": record.get("source_line"),
                    "engine_id": engine_id,
                    "sensor": f"sensor_column_{index}",
                    "observed_value": value,
                    "anomaly_type": "possible_frozen_sensor",
                    "rule": "repeated_identical_values_after_variation",
                    "consecutive_identical_readings": (
                        same_value_runs[key]
                    ),
                    "explanation": (
                        "Repeated identical readings followed recent "
                        "variation. A frozen signal is one possibility; "
                        "stable conditions or quantization may also "
                        "explain this pattern."
                    ),
                    "confidence": "unconfirmed",
                })

            # Compare against previous readings, before updating history.
            if len(prior) >= min_history:
                baseline = median(prior)

                deviations = [
                    abs(old_value - baseline)
                    for old_value in prior
                ]

                mad = median(deviations)
                difference = abs(value - baseline)

                mad_floor = max(
                    1e-9,
                    abs(baseline) * relative_mad_floor,
                )

                if mad > mad_floor:
                    robust_z = 0.6745 * difference / mad
                    is_outlier = robust_z > robust_z_threshold
                    detection_method = "rolling_median_mad"

                else:
                    # When recent variation is too small to trust
                    # the statistical score, use a larger tolerance.
                    # The default is 1% of the baseline, not an
                    # engineering or safety limit.
                    robust_z = None

                    tolerance = max(
                        1e-9,
                        abs(baseline) * fallback_tolerance_pct,
                    )

                    is_outlier = difference > tolerance
                    detection_method = (
                        "near_zero_mad_relative_tolerance"
                    )

                if is_outlier:
                    change_pct = (
                        (value - baseline) / abs(baseline) * 100
                        if baseline != 0
                        else None
                    )

                    alerts.append({
                        "source": record.get("source"),
                        "source_line": record.get("source_line"),
                        "engine_id": engine_id,
                        "sensor": f"sensor_column_{index}",
                        "observed_value": value,
                        "baseline_median": baseline,
                        "absolute_deviation": difference,
                        "robust_z_score": robust_z,
                        "change_percent_from_baseline": change_pct,
                        "anomaly_type": "statistical_outlier",
                        "rule": detection_method,
                        "explanation": (
                            "The reading differs from recent sensor "
                            "history. This is an unusual-reading screen, "
                            "not a diagnosis or safety determination."
                        ),
                        "confidence": "screening_alert",
                    })

            # Update the sensor history after screening.
            prior.append(value)
            last_values[key] = value

    return alerts
