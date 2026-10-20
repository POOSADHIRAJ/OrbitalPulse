"""Run-to-run comparison built from stored metrics. Differences are descriptive only: they never imply a
design improvement or deterioration, and different units are never compared."""
from __future__ import annotations

import sqlite3
from typing import Any

from orbitalpulse.analytics.metrics import run_summary, sensor_metrics

METRIC_KEYS = ("mean", "median", "min", "max", "std", "completeness_pct", "record_count")


def _diff(base: float | None, other: float | None) -> dict[str, float | None]:
    if base is None or other is None:
        return {"abs": None, "pct": None}
    return {"abs": other - base, "pct": (100.0 * (other - base) / abs(base)) if abs(base) > 1e-9 else None}


def _units(conn, run_id) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for r in conn.execute("SELECT DISTINCT sensor_id, unit FROM events WHERE run_id=?", (run_id,)):
        out.setdefault(r["sensor_id"], set()).add(r["unit"])
    return out


def compare_runs(conn: sqlite3.Connection, run_ids: list[str], sensors: dict, interval_tolerance: float = 0.5) -> dict[str, Any]:
    if len(run_ids) < 2:
        raise ValueError("Select at least two runs to compare.")
    summaries = {r: run_summary(conn, r, sensors) for r in run_ids}
    metrics = {r: {m["sensor_id"]: m for m in sensor_metrics(conn, r, sensors, interval_tolerance=interval_tolerance)}
               for r in run_ids}
    units = {r: _units(conn, r) for r in run_ids}
    meta = {r: conn.execute("SELECT campaign_id, scenario, synthetic FROM runs WHERE run_id=?", (r,)).fetchone()
            for r in run_ids}
    warnings: list[dict] = []
    base = run_ids[0]

    if len({meta[r]["campaign_id"] for r in run_ids}) > 1:
        warnings.append({"level": "INFO", "message": "Runs belong to different campaigns."})
    durs = [summaries[r]["duration_s"] for r in run_ids]
    if durs and max(durs) > 0 and (max(durs) - min(durs)) / max(durs) > 0.1:
        warnings.append({"level": "INFO", "message": "Run durations differ by more than 10%. Use elapsed-time alignment "
                         "for visual overlay; stored timestamps are never modified."})

    sensor_ids = sorted({s for r in run_ids for s in metrics[r]})
    per_sensor = []
    for sid in sensor_ids:
        present = [r for r in run_ids if sid in metrics[r]]
        status, notes = "OK", []
        if len(present) < len(run_ids):
            status = "CAUTION"
            notes.append("Sensor missing from: " + ", ".join(r for r in run_ids if r not in present))
        all_units = {u for r in present for u in units[r].get(sid, set())}
        if len(all_units) > 1:
            status = "NOT_COMPARABLE"
            notes.append(f"Units differ across runs ({', '.join(sorted(all_units))}); no conversion attempted.")
            warnings.append({"level": "WARNING", "sensor_id": sid, "message": notes[-1]})
        eff = [metrics[r][sid]["effective_interval_s"] for r in present if metrics[r][sid]["effective_interval_s"]]
        if eff and max(eff) / min(eff) > 1.1:
            status = "CAUTION" if status == "OK" else status
            detail = ", ".join(f"{r}: {metrics[r][sid]['effective_interval_s']:.3g}s" for r in present)
            msg = (f"Sampling intervals differ for {sid} ({detail}). "
                   "Statistics are computed on different sample densities.")
            notes.append(msg)
            warnings.append({"level": "WARNING", "sensor_id": sid, "message": msg})
        row: dict[str, Any] = {"sensor_id": sid, "status": status, "notes": notes,
                               "unit": sorted(all_units)[0] if len(all_units) == 1 else None,
                               "per_run": {r: {k: metrics[r][sid][k] for k in METRIC_KEYS + ("effective_interval_s",)}
                                           for r in present}, "diff_vs_baseline": {}}
        if status != "NOT_COMPARABLE" and base in present:
            for r in present:
                if r != base:
                    row["diff_vs_baseline"][r] = {k: _diff(metrics[base][sid][k], metrics[r][sid][k])
                                                  for k in ("mean", "median", "min", "max", "std", "completeness_pct")}
        per_sensor.append(row)

    rules = sorted({k for r in run_ids for k in summaries[r]["findings_by_rule"]})
    return {
        "run_ids": run_ids, "baseline_run": base,
        "runs": [{**summaries[r], "scenario": meta[r]["scenario"], "campaign_id": meta[r]["campaign_id"],
                  "synthetic": bool(meta[r]["synthetic"])} for r in run_ids],
        "sensors": per_sensor, "warnings": warnings,
        "anomaly_counts_by_rule": {rule: {r: summaries[r]["findings_by_rule"].get(rule, 0) for r in run_ids} for rule in rules},
        "phase_durations_s": {r: summaries[r]["phase_durations_s"] for r in run_ids},
        "method_note": ("Differences are computed against the baseline run (first selected) as other - baseline; percentage change "
                        "is shown only when the baseline is non-zero. Elapsed-time alignment (optional, visual only) subtracts each "
                        "run's first event time. A difference does not prove improvement or deterioration."),
    }
