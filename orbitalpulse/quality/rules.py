"""Run-level data-quality rules computed from stored events (event-level rules live in
schemas.validation / ingestion.core). All results are explainable `QualityFinding`s.
Severities describe DATA quality only, not engine safety."""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC

from orbitalpulse.config import SensorConfig, Settings


@dataclass
class RunIssue:
    rule_id: str
    sensor_id: str | None
    severity: str
    observed: str
    expected: str
    explanation: str
    window_start: str | None = None
    window_end: str | None = None
    count: int = 1


def load_series(conn: sqlite3.Connection, run_id: str) -> dict[str, list[dict]]:
    """Per-sensor events in event-time order, each with its arrival rank."""
    out: dict[str, list[dict]] = defaultdict(list)
    for r in conn.execute("SELECT rowid_arrival, sensor_id, event_epoch, event_ts, value, sequence_number, phase "
                          "FROM events WHERE run_id=? ORDER BY sensor_id, event_epoch, rowid_arrival", (run_id,)):
        out[r["sensor_id"]].append(dict(r))
    return out


def check_run(conn: sqlite3.Connection, run_id: str, sensors: SensorConfig, settings: Settings) -> list[RunIssue]:
    q = settings.quality
    spec_map = sensors.by_id()
    issues: list[RunIssue] = []
    series = load_series(conn, run_id)
    rate_dev: list[tuple] = []

    for sid, recs in series.items():
        spec = spec_map.get(sid)
        expected = spec.expected_interval_s if spec else None
        unit = spec.unit if spec else ""

        # --- out-of-order arrival (arrival rank vs event time) ---
        by_arrival = sorted(recs, key=lambda r: r["rowid_arrival"])
        ooo = [(a, b) for a, b in zip(by_arrival, by_arrival[1:]) if b["event_epoch"] < a["event_epoch"]]
        if ooo:
            issues.append(RunIssue(
                "DQ_OUT_OF_ORDER", sid, "WARNING", f"{len(ooo)} events arrived with an older timestamp than the previous event",
                "non-decreasing event timestamps in arrival order",
                f"{len(ooo)} out-of-order arrivals for {sid}. Metrics use event time, so values are reordered; "
                "arrival order is preserved in the raw archive.",
                ooo[0][1]["event_ts"], ooo[-1][1]["event_ts"], len(ooo)))

        # --- missing sequence numbers ---
        seqs = sorted({r["sequence_number"] for r in recs if r["sequence_number"] is not None})
        if len(seqs) > 1:
            missing = (seqs[-1] - seqs[0] + 1) - len(seqs)
            if missing > 0:
                gaps = [(a, b) for a, b in zip(seqs, seqs[1:]) if b - a > 1]
                issues.append(RunIssue(
                    "DQ_MISSING_SEQUENCE", sid, "WARNING" if missing < 0.05 * len(seqs) else "ERROR",
                    f"{missing} sequence numbers absent between {seqs[0]} and {seqs[-1]} ({len(gaps)} gap(s))",
                    "contiguous sequence numbers",
                    f"{missing} sample(s) never received for {sid}. This is an OBSERVED count of absent "
                    "sequence numbers (exact, because the source supplies sequence numbers).", count=missing))

        # --- time gaps and implausible intervals (event time) ---
        times = sorted({r["event_epoch"] for r in recs})
        tol = q["interval_tolerance_factor"]
        eff, deviates = effective_interval(times, expected, tol)
        if deviates and eff and expected:
            rate_dev.append((sid, eff / expected, eff, expected))
        if eff and len(times) > 1:
            gap_thr = eff * q["gap_factor"]
            gaps = [(a, b) for a, b in zip(times, times[1:]) if (b - a) > gap_thr]
            if gaps:
                worst = max(b - a for a, b in gaps)
                issues.append(RunIssue(
                    "DQ_TIME_GAP", sid, "WARNING", f"{len(gaps)} gap(s), longest {worst:.2f}s",
                    f"interval <= {gap_thr:.2f}s ({q['gap_factor']}x expected {eff:.3g}s)",
                    f"Interval between consecutive samples of {sid} exceeded {q['gap_factor']}x the configured "
                    "sample interval.", _iso(gaps[0][0]), _iso(gaps[-1][1]), len(gaps)))

        # --- flatline (identical consecutive values) ---
        issues.extend(flatline_issues(sid, recs, q["flatline_min_samples"], q["flatline_epsilon_rel"], unit))

    # --- sampling-interval deviation: consistent ratio across sensors = run-level rate setting ---
    if rate_dev:
        ratios = [x[1] for x in rate_dev]
        tol_c = q.get("run_rate_consistency_tolerance", 0.15)
        if len(rate_dev) >= 3 and len(rate_dev) == len(series) and max(ratios) / min(ratios) - 1 <= tol_c:
            issues.append(RunIssue(
                "DQ_RUN_SAMPLE_RATE_DIFFERS", None, "INFO", f"all {len(rate_dev)} sensors sampled ~{sum(ratios)/len(ratios):.2f}x "
                "slower/faster than configured", "observed interval ~ configured expected_interval_s",
                "Every sensor deviates from its configured interval by a similar factor, which suggests a different "
                "acquisition rate for this run rather than per-sensor faults. Completeness is estimated against the "
                "OBSERVED cadence and run comparisons will warn about differing rates."))
        else:
            for sid, _ratio, eff, exp in rate_dev:
                issues.append(RunIssue(
                    "DQ_IMPLAUSIBLE_INTERVAL", sid, "WARNING", f"median observed interval {eff:.3f}s",
                    f"{exp}s +/- {int(q['interval_tolerance_factor'] * 100)}%",
                    f"Observed median sampling interval for {sid} differs from the configured expectation. "
                    "Either the source rate differs or the configuration is wrong."))
    return issues


def effective_interval(times: list[float], expected: float | None, tol: float) -> tuple[float | None, bool]:
    """Expected interval to use for gap/completeness maths. If the observed median interval deviates from the
    configured one by more than ``tol`` the observed cadence is used (and the deviation is reported separately)."""
    if not expected:
        return None, False
    deltas = sorted(b - a for a, b in zip(times, times[1:]) if b > a)
    if not deltas:
        return expected, False
    med = deltas[len(deltas) // 2]
    if abs(med - expected) / expected > tol:
        return med, True
    return expected, False


def _iso(epoch: float) -> str:
    from datetime import datetime
    return datetime.fromtimestamp(epoch, UTC).isoformat()


def find_flat_runs(recs: list[dict], min_len: int, eps_rel: float) -> list[tuple[int, int]]:
    """(start_idx, end_idx) of runs of >= min_len identical non-null values."""
    runs, i, n = [], 0, len(recs)
    while i < n:
        v = recs[i]["value"]
        j = i
        while v is not None and j + 1 < n and recs[j + 1]["value"] is not None and \
                abs(recs[j + 1]["value"] - v) <= eps_rel * max(1.0, abs(v)):
            j += 1
        if v is not None and j - i + 1 >= min_len:
            runs.append((i, j))
        i = j + 1
    return runs


def flatline_issues(sid, recs, min_len, eps_rel, unit) -> list[RunIssue]:
    out = []
    for a, b in find_flat_runs(recs, int(min_len), eps_rel):
        out.append(RunIssue(
            "DQ_FLATLINE", sid, "WARNING", f"{b - a + 1} identical readings of {recs[a]['value']} {unit}",
            f"fewer than {int(min_len)} identical consecutive readings",
            f"{sid} repeated exactly the same value. Possible frozen/stuck channel, quantisation or a genuinely "
            "constant signal - review recommended, not a confirmed fault.",
            recs[a]["event_ts"], recs[b]["event_ts"], b - a + 1))
    return out
