"""Deterministic synthetic telemetry generator.

All output is labelled source_type=SYNTHETIC. Same seed + same sensor config => identical output.
The signal shapes are illustrative; they do not model any real engine or test stand.
"""
from __future__ import annotations

import csv
import json
import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from orbitalpulse import SCHEMA_VERSION
from orbitalpulse.config import SensorConfig, SensorSpec, load_sensor_config

CAMPAIGN_ID = "SYNTH-CAMPAIGN-001"
BASE_START = datetime(2026, 1, 15, 10, 0, 0, tzinfo=UTC)
PHASE_FRACTIONS = [("PREPARATION", 0.0), ("STARTUP", 0.15), ("TEST_WINDOW", 0.25),
                   ("SHUTDOWN", 0.75), ("POST_TEST", 0.85)]
CSV_COLUMNS = ["event_id", "campaign_id", "test_run_id", "sensor_id", "sequence_number", "event_timestamp",
               "ingestion_timestamp", "test_phase", "sensor_value", "sensor_unit", "quality_status",
               "source_type", "schema_version"]


@dataclass
class RunPlan:
    run_id: str
    scenario: str            # clean | quality_defects | suspicious_patterns
    duration_s: float = 60.0
    seed: int = 1
    start: datetime = BASE_START
    interval_scale: float = 1.0   # multiplies each sensor's expected interval
    gain: float = 1.0
    campaign_id: str = CAMPAIGN_ID
    description: str = ""


DEMO_PLANS = [
    RunPlan("RUN-001-CLEAN", "clean", 60, seed=101, start=BASE_START,
            description="Clean synthetic run: no injected defects."),
    RunPlan("RUN-002-DQ-DEFECTS", "quality_defects", 60, seed=202, start=BASE_START + timedelta(hours=2),
            description="Synthetic run with injected data-quality defects (gaps, duplicates, malformed rows...)."),
    RunPlan("RUN-003-SUSPICIOUS", "suspicious_patterns", 60, seed=303, start=BASE_START + timedelta(hours=4),
            description="Valid-looking data with injected unusual sensor patterns (spike, drop, flatline, drift...)."),
    RunPlan("RUN-004-CLEAN-B", "clean", 90, seed=404, start=BASE_START + timedelta(hours=6), interval_scale=2.0,
            gain=1.04, description="Second clean run, longer, half sampling rate, +4% gain (for comparison demo)."),
]


def _smooth(x: float) -> float:
    x = min(max(x, 0.0), 1.0)
    return x * x * (3 - 2 * x)


def _envelope(frac: float) -> float:
    if frac < 0.15:
        return 0.0
    if frac < 0.25:
        return _smooth((frac - 0.15) / 0.10)
    if frac < 0.75:
        return 1.0 + 0.01 * math.sin((frac - 0.25) * 40)
    if frac < 0.85:
        return 1.0 - _smooth((frac - 0.75) / 0.10)
    return 0.0


def phase_at(frac: float) -> str:
    name = PHASE_FRACTIONS[0][0]
    for label, start in PHASE_FRACTIONS:
        if frac >= start:
            name = label
    return name


def _iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _sensor_series(spec: SensorSpec, plan: RunPlan, rng: random.Random) -> list[dict]:
    interval = (spec.expected_interval_s or 1.0) * plan.interval_scale
    n = int(plan.duration_s / interval)
    prof = spec.synthetic_profile
    idle, peak, sd = prof.get("idle", 0.0), prof.get("peak", 1.0), prof.get("noise_sd", 0.0)
    lag = prof.get("lag", False)
    state = 0.0
    out = []
    for k in range(n):
        frac = k * interval / plan.duration_s
        env = _envelope(frac)
        if lag:
            state += (env - state) * min(1.0, interval / prof.get('tau_s', 3.0))
            env = state
        value = idle + (peak - idle) * env * (plan.gain if peak != idle else 1.0) + rng.gauss(0, sd)
        lo = spec.validation_min if spec.validation_min is not None else -math.inf
        hi = spec.validation_max if spec.validation_max is not None else math.inf
        value = min(max(value, lo), hi)
        ts = plan.start + timedelta(seconds=k * interval)
        out.append({
            "sensor_id": spec.sensor_id, "seq": k, "ts": ts, "value": round(value, 4), "unit": spec.unit,
            "phase": phase_at(frac), "ingest": ts + timedelta(milliseconds=rng.randint(20, 80)),
            "interval": interval,
        })
    return out


def _to_record(plan: RunPlan, s: dict) -> dict:
    return {
        "event_id": f"{plan.run_id}:{s['sensor_id']}:{s['seq']:06d}", "campaign_id": plan.campaign_id,
        "test_run_id": plan.run_id, "sensor_id": s["sensor_id"], "sequence_number": s["seq"],
        "event_timestamp": _iso(s["ts"]), "ingestion_timestamp": _iso(s["ingest"]), "test_phase": s["phase"],
        "sensor_value": "" if s["value"] is None else s["value"], "sensor_unit": s["unit"],
        "quality_status": "UNCHECKED", "source_type": "SYNTHETIC", "schema_version": SCHEMA_VERSION,
    }


def _apply_suspicious(series: dict[str, list[dict]], plan: RunPlan, rng: random.Random) -> None:
    n = {k: len(v) for k, v in series.items()}
    cp = series["chamber_pressure"]
    for frac in (0.40, 0.58):  # isolated spikes (also exceed the operating range)
        cp[int(n["chamber_pressure"] * frac)]["value"] = 95.0
    fl = series["feed_line_pressure"]
    for i in range(int(n["feed_line_pressure"] * 0.50), int(n["feed_line_pressure"] * 0.50) + 3):
        fl[i]["value"] = round(fl[i]["value"] * 0.35, 4)  # sudden drop
    temp = series["temperature"]
    start = int(n["temperature"] * 0.45)
    frozen = temp[start]["value"]
    for i in range(start, start + 26):
        temp[i]["value"] = frozen  # frozen reading
    vib = series["vibration_rms"]
    a, b = int(n["vibration_rms"] * 0.27), int(n["vibration_rms"] * 0.73)
    for i in range(a, b):  # gradual drift
        vib[i]["value"] = round(vib[i]["value"] + 7.5 * (i - a) / (b - a), 4)
    for s in series["load_cell_measurement"]:  # clock offset on one channel
        s["ts"] += timedelta(seconds=3.0)
        s["ingest"] += timedelta(seconds=3.0)
    # phase label glitch (all sensors) inside the test window: TEST_WINDOW -> STARTUP -> TEST_WINDOW
    t0 = plan.start + timedelta(seconds=plan.duration_s * 0.55)
    t1 = t0 + timedelta(seconds=1.5)
    for sid, lst in series.items():
        for s in lst:
            if t0 <= s["ts"] < t1 and s["phase"] == "TEST_WINDOW" and sid != "load_cell_measurement":
                s["phase"] = "STARTUP"


def _apply_defects(series: dict[str, list[dict]], plan: RunPlan, rng: random.Random):
    """Returns (extra_records, tail_ops) after mutating series. Defects are explicit and counted."""
    # missing samples: random 1% across sensors, plus sensor dropout block on feed line, plus NULL values
    for sid, lst in series.items():
        series[sid] = [s for s in lst if not (rng.random() < 0.01)]
    fl = series["feed_line_pressure"]
    lo, hi = int(len(fl) * 0.50), int(len(fl) * 0.50) + 50
    series["feed_line_pressure"] = fl[:lo] + fl[hi:]  # ~5 s dropout
    for s in rng.sample(series["vibration_rms"], 12):
        s["value"] = None  # NULL measurement (missing, not zero)
    for s in rng.sample(series["thrust_measurement"], 15):  # delayed events
        s["ingest"] += timedelta(seconds=30)


def generate_run(plan: RunPlan, sensors: SensorConfig | None = None) -> list[dict]:
    """Records in ARRIVAL order (defects such as duplicates/out-of-order apply to this order)."""
    sensors = sensors or load_sensor_config()
    rng = random.Random(plan.seed)
    series = {sp.sensor_id: _sensor_series(sp, plan, random.Random(rng.randint(0, 2**31)))
              for sp in sensors.sensors if sp.enabled}
    if plan.scenario == "suspicious_patterns":
        _apply_suspicious(series, plan, rng)
    if plan.scenario == "quality_defects":
        _apply_defects(series, plan, rng)

    records = [_to_record(plan, s) for sid in sorted(series) for s in series[sid]]
    records.sort(key=lambda r: (r["event_timestamp"], r["sensor_id"]))  # arrival ~ time order

    if plan.scenario == "quality_defects":
        for i in rng.sample(range(len(records) - 60), 40):  # out-of-order arrival: swap two events of one sensor
            j = next((k for k in range(i + 1, i + 60) if records[k]["sensor_id"] == records[i]["sensor_id"]), None)
            if j is not None:
                records[i], records[j] = records[j], records[i]
        for i in sorted(rng.sample(range(len(records)), 30), reverse=True):  # duplicate deliveries
            records.insert(i + 1, dict(records[i]))
        base = records[len(records) // 3]
        bad = [
            {**base, "event_id": f"{plan.run_id}:MALFORMED:1", "sensor_value": "ERR#"},
            {**base, "event_id": f"{plan.run_id}:MALFORMED:2", "event_timestamp": "not-a-time"},
            {**base, "event_id": f"{plan.run_id}:MALFORMED:3", "sensor_id": "chamber_pressure", "sensor_unit": "psi"},
            {**base, "event_id": f"{plan.run_id}:MALFORMED:4", "sensor_id": "chamber_pressure", "sensor_unit": "bar",
             "sensor_value": 9999},
            {**base, "event_id": f"{plan.run_id}:MALFORMED:5", "sensor_id": "mystery_sensor", "sensor_unit": "x"},
            {**base, "event_id": f"{plan.run_id}:MALFORMED:6", "sensor_id": ""},
            {**base, "event_id": f"{plan.run_id}:MALFORMED:7", "sensor_value": "NaN"},
            {**base, "event_id": f"{plan.run_id}:MALFORMED:8", "sequence_number": "abc"},
        ]
        for j, rec in enumerate(bad):
            records.insert(int(len(records) * (j + 1) / (len(bad) + 1)), rec)
    return records


def generate_campaign(plans: list[RunPlan] | None = None, sensors: SensorConfig | None = None) -> dict[str, list[dict]]:
    sensors = sensors or load_sensor_config()
    return {p.run_id: generate_run(p, sensors) for p in (plans or DEMO_PLANS)}


def write_csv(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def write_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def scenario_map(plans: list[RunPlan] | None = None) -> dict[str, str]:
    return {p.run_id: p.scenario for p in (plans or DEMO_PLANS)}
