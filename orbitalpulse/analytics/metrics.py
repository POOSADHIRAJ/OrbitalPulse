"""Descriptive per-run / per-sensor metrics. Observed values vs estimates are labelled explicitly."""
from __future__ import annotations

import math
import sqlite3
import statistics
from typing import Any

from orbitalpulse.quality.rules import effective_interval


def _filters(run_id, sensor_id=None, phase=None, start=None, end=None):
    where, args = ["run_id = ?"], [run_id]
    if sensor_id:
        where.append("sensor_id = ?"); args.append(sensor_id)
    if phase:
        where.append("phase = ?"); args.append(phase)
    if start is not None:
        where.append("event_epoch >= ?"); args.append(start)
    if end is not None:
        where.append("event_epoch <= ?"); args.append(end)
    return " AND ".join(where), args


def sensor_metrics(conn: sqlite3.Connection, run_id: str, sensors: dict, *, sensor_id=None, phase=None,
                   start=None, end=None, interval_tolerance: float = 0.5) -> list[dict[str, Any]]:
    where, args = _filters(run_id, sensor_id, phase, start, end)
    rows = conn.execute(f"SELECT sensor_id, event_epoch, value, quality_status, event_ts FROM events "
                        f"WHERE {where} ORDER BY sensor_id, event_epoch", args).fetchall()
    quarantined = {r["sensor_id"]: r["n"] for r in conn.execute(
        "SELECT sensor_id, COUNT(*) n FROM quarantine WHERE run_id=? GROUP BY sensor_id", (run_id,))}
    grouped: dict[str, list] = {}
    for r in rows:
        grouped.setdefault(r["sensor_id"], []).append(r)
    out = []
    for sid, recs in sorted(grouped.items()):
        spec = sensors.get(sid)
        vals = [r["value"] for r in recs if r["value"] is not None and not math.isnan(r["value"])]
        times = sorted({r["event_epoch"] for r in recs})
        deltas = [b - a for a, b in zip(times, times[1:]) if b > a]
        duration = (times[-1] - times[0]) if len(times) > 1 else 0.0
        expected_cfg = spec.expected_interval_s if spec else None
        expected, deviates = effective_interval(times, expected_cfg, interval_tolerance)
        missing = sum(1 for r in recs if r["value"] is None)
        est_expected_n = (duration / expected + 1) if (expected and duration > 0) else None
        m: dict[str, Any] = {
            "sensor_id": sid, "unit": spec.unit if spec else None,
            "record_count": len(recs), "valid_count": len(vals), "missing_value_count": missing,
            "invalid_count": quarantined.get(sid, 0),
            "min": min(vals) if vals else None, "max": max(vals) if vals else None,
            "mean": statistics.fmean(vals) if vals else None,
            "median": statistics.median(vals) if vals else None,
            "std": statistics.pstdev(vals) if len(vals) > 1 else (0.0 if vals else None),
            "first_timestamp": recs[0]["event_ts"] if recs else None,
            "last_timestamp": recs[-1]["event_ts"] if recs else None,
            "duration_s": duration,
            "observed_interval_median_s": statistics.median(deltas) if deltas else None,
            "expected_interval_s": expected_cfg, "effective_interval_s": expected,
            "estimated_missing_sample_rate": (max(0.0, 1 - len(times) / est_expected_n)
                                              if est_expected_n else None),
            "completeness_pct": (100.0 * len(vals) / est_expected_n if est_expected_n else
                                 (100.0 * len(vals) / len(recs) if recs else None)),
            "completeness_basis": (("estimated from OBSERVED cadence (differs from configured interval)" if deviates
                                    else "estimated from configured sample interval") if est_expected_n
                                   else "valid / received records (no expected interval)"),
        }
        if m["completeness_pct"] is not None:
            m["completeness_pct"] = min(100.0, m["completeness_pct"])
        out.append(m)
    return out


def run_summary(conn: sqlite3.Connection, run_id: str, sensors: dict) -> dict[str, Any]:
    ev = conn.execute("SELECT COUNT(*) n, SUM(value IS NULL) miss, MIN(event_epoch) a, MAX(event_epoch) b, "
                      "MIN(event_ts) a_ts, MAX(event_ts) b_ts FROM events WHERE run_id=?", (run_id,)).fetchone()
    quar = conn.execute("SELECT COUNT(*) FROM quarantine WHERE run_id=?", (run_id,)).fetchone()[0]
    issues = {r["severity"]: r["n"] for r in conn.execute(
        "SELECT severity, COUNT(*) n FROM quality_issues WHERE run_id=? GROUP BY severity", (run_id,))}
    finds = conn.execute("SELECT rule_id, COUNT(*) n FROM findings WHERE run_id=? GROUP BY rule_id", (run_id,)).fetchall()
    metrics = sensor_metrics(conn, run_id, sensors)
    comps = [m["completeness_pct"] for m in metrics if m["completeness_pct"] is not None]
    phases = phase_durations(conn, run_id)
    return {
        "run_id": run_id, "event_count": ev["n"] or 0, "missing_value_count": ev["miss"] or 0,
        "quarantined_count": quar, "first_timestamp": ev["a_ts"], "last_timestamp": ev["b_ts"],
        "duration_s": ((ev["b"] - ev["a"]) if ev["n"] else 0.0),
        "completeness_pct": statistics.fmean(comps) if comps else None,
        "issues_by_severity": issues, "issue_count": sum(issues.values()),
        "findings_by_rule": {r["rule_id"]: r["n"] for r in finds},
        "finding_count": sum(r["n"] for r in finds), "sensor_count": len(metrics), "phase_durations_s": phases,
    }


def phase_durations(conn: sqlite3.Connection, run_id: str) -> dict[str, float]:
    rows = conn.execute("SELECT phase, MIN(event_epoch) a, MAX(event_epoch) b FROM events WHERE run_id=? "
                        "GROUP BY phase", (run_id,)).fetchall()
    return {r["phase"]: round(r["b"] - r["a"], 3) for r in rows}
