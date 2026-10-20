"""Rule-based, explainable anomaly engine. No ML, no LLM; every explanation is generated from the
rule result and observed values. Findings are *unusual sensor patterns / review recommended* -
never confirmed engine malfunctions."""
from __future__ import annotations

import hashlib
import statistics
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from orbitalpulse.config import SensorConfig, SensorSpec, Settings
from orbitalpulse.quality.rules import effective_interval, find_flat_runs, load_series

CATEGORIES = ("THRESHOLD_EXCEEDED", "UNUSUAL_SENSOR_PATTERN", "POSSIBLE_DATA_QUALITY_PROBLEM",
              "INSUFFICIENT_EVIDENCE", "REVIEW_RECOMMENDED")
GENERIC_LIMITATION = ("Rule-based screening on synthetic demonstration data. An unusual pattern is not a confirmed "
                      "malfunction; sensor, wiring, processing or genuine physical causes are all possible.")


@dataclass
class Finding:
    rule_id: str
    rule_name: str
    category: str
    severity: str
    sensor_id: str | None
    window_start: str | None
    window_end: str | None
    evidence: dict[str, Any]
    threshold: dict[str, Any]
    explanation: str
    limitations: str = GENERIC_LIMITATION
    finding_id: str = field(default="")

    def assign_id(self, run_id: str) -> "Finding":
        raw = f"{run_id}|{self.rule_id}|{self.sensor_id}|{self.window_start}|{self.window_end}"
        self.finding_id = "F-" + hashlib.sha1(raw.encode()).hexdigest()[:14]
        return self


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat()


def _group(indices: list[int], max_gap: int = 3) -> list[list[int]]:
    groups: list[list[int]] = []
    for i in indices:
        if groups and i - groups[-1][-1] <= max_gap:
            groups[-1].append(i)
        else:
            groups.append([i])
    return groups


def analyze(conn, run_id: str, sensors: SensorConfig, settings: Settings) -> list[Finding]:
    cfg = settings.anomaly
    spec_map = sensors.by_id()
    series = load_series(conn, run_id)
    findings: list[Finding] = []

    if not series:
        return [Finding("AN_INSUFFICIENT_DATA", "Insufficient data", "INSUFFICIENT_EVIDENCE", "INFO", None, None,
                        None, {"event_count": 0}, {"min_samples": cfg["min_samples"]},
                        "The run has no stored events, so no rule could be evaluated. No conclusion is drawn.",
                        "Absence of findings here means 'not evaluated', not 'no problem'.").assign_id(run_id)]

    for sid, recs in sorted(series.items()):
        spec = spec_map.get(sid)
        if spec is None or not spec.enabled:
            continue
        valued = [r for r in recs if r["value"] is not None]
        if len(valued) < cfg["min_samples"]:
            findings.append(Finding(
                "AN_INSUFFICIENT_DATA", "Insufficient data", "INSUFFICIENT_EVIDENCE", "INFO", sid,
                recs[0]["event_ts"], recs[-1]["event_ts"],
                {"valid_samples": len(valued), "total_records": len(recs)}, {"min_samples": cfg["min_samples"]},
                f"{sid} has only {len(valued)} valid samples (minimum {int(cfg['min_samples'])}); rules were not "
                "evaluated for this sensor. This is missing evidence, not a clean bill of health.",
                "Result reflects data volume only."))
            continue
        findings += _range(sid, spec, valued)
        findings += _spike_drop(sid, spec, valued, cfg)
        findings += _flatline(sid, spec, valued, cfg)
        findings += _drift(sid, spec, valued, cfg)
        findings += _missing_burst(sid, spec, recs, cfg)
        findings += _interval(sid, spec, recs, cfg)
    findings += _cross_sensor(series, spec_map, cfg)
    findings += _phase(series, settings)
    return [f.assign_id(run_id) for f in findings]


# ---------------------------------------------------------------- rules
def _range(sid, spec: SensorSpec, valued) -> list[Finding]:
    lo, hi = spec.operating_min, spec.operating_max
    if lo is None and hi is None:
        return []
    bad = [i for i, r in enumerate(valued)
           if (lo is not None and r["value"] < lo) or (hi is not None and r["value"] > hi)]
    out = []
    for g in _group(bad, 1):
        vals = [valued[i]["value"] for i in g]
        peak = max(vals, key=lambda v: abs(v - (hi if max(vals) > (hi if hi is not None else 1e308) else lo)))
        out.append(Finding(
            "AN_RANGE_EXCEEDED", "Configured operating range exceeded", "THRESHOLD_EXCEEDED", "WARNING", sid,
            valued[g[0]]["event_ts"], valued[g[-1]]["event_ts"],
            {"samples_outside_range": len(g), "extreme_value": peak, "unit": spec.unit,
             "first_value": vals[0]},
            {"operating_min": lo, "operating_max": hi, "unit": spec.unit, "origin": "synthetic demo setting"},
            f"{len(g)} consecutive sample(s) of {sid} were outside the configured range [{lo}, {hi}] {spec.unit}; "
            f"most extreme value {peak} {spec.unit}. Threshold exceeded - review recommended.",
            "The range is a user-editable synthetic demonstration setting, not an engineering limit. "
            "The value passed validation bounds, so it is treated as a valid but unusual measurement."))
    return out


def _spike_drop(sid, spec, valued, cfg) -> list[Finding]:
    win, min_hist = int(cfg["spike_window"]), int(cfg["spike_min_history"])
    z_thr, rel_thr = cfg["spike_robust_z"], cfg["spike_min_rel_change"]
    span = (spec.operating_max - spec.operating_min) if (spec.operating_max is not None and
                                                         spec.operating_min is not None) else None
    ups: list[tuple] = []
    downs: list[tuple] = []
    vals = [r["value"] for r in valued]
    for i in range(min_hist, len(vals)):
        hist = vals[max(0, i - win):i]
        base = statistics.median(hist)
        mad = statistics.median([abs(x - base) for x in hist])
        dev = vals[i] - base
        scale = max(abs(base), 0.1 * span if span else 0.0, 1e-9)
        robust_z = 0.6745 * abs(dev) / mad if mad > 1e-12 else float("inf") if dev else 0.0
        if robust_z > z_thr and abs(dev) / scale >= rel_thr:
            (ups if dev > 0 else downs).append((i, base, mad, robust_z, dev, scale))
    out = []
    for kind, items, rid, name in (("up", ups, "AN_SPIKE", "Sudden spike vs rolling baseline"),
                                   ("down", downs, "AN_DROP", "Sudden drop vs rolling baseline")):
        for g in _group([x[0] for x in items], 3):
            first = next(x for x in items if x[0] == g[0])
            after = vals[g[-1] + 1:g[-1] + 1 + int(cfg.get("spike_revert_samples", 4))]
            # an isolated excursion reverts to the baseline; ramps/level shifts do not and are not spikes
            if after and abs(statistics.median(after) - first[1]) > 0.5 * rel_thr * first[5]:
                continue
            peak = max((vals[i] for i in g), key=lambda v: abs(v - first[1]))
            z = first[3]
            out.append(Finding(
                rid, name, "UNUSUAL_SENSOR_PATTERN", "WARNING", sid, valued[g[0]]["event_ts"], valued[g[-1]]["event_ts"],
                {"samples_flagged": len(g), "observed_extreme": peak, "baseline_median": round(first[1], 4),
                 "baseline_mad": round(first[2], 4), "change_pct_of_baseline": round(100 * (peak - first[1]) / first[5], 1),
                 "robust_z_first_sample": None if z == float("inf") else round(z, 2), "unit": spec.unit},
                {"robust_z": z_thr, "min_relative_change": rel_thr, "baseline_window_samples": win},
                f"{sid} {'rose' if kind == 'up' else 'fell'} to {peak} {spec.unit} against a rolling median baseline of "
                f"{first[1]:.3f} {spec.unit} ({len(g)} sample(s) flagged). Both the robust z-score threshold "
                f"({z_thr}) and the minimum relative change ({int(rel_thr * 100)}%) were exceeded.",
                "Baseline is the median of the preceding samples, so genuine fast transients or phase changes can "
                "also trigger it. Isolated single-sample values are consistent with sensor glitches or real events."))
    return out


def _flatline(sid, spec, valued, cfg) -> list[Finding]:
    out = []
    for a, b in find_flat_runs(valued, int(cfg["flatline_min_samples"]), 1e-9):
        v = valued[a]["value"]
        out.append(Finding(
            "AN_FLATLINE", "Flatlined / frozen sensor", "POSSIBLE_DATA_QUALITY_PROBLEM", "WARNING", sid,
            valued[a]["event_ts"], valued[b]["event_ts"],
            {"identical_samples": b - a + 1, "repeated_value": v, "unit": spec.unit,
             "duration_s": round(valued[b]["event_epoch"] - valued[a]["event_epoch"], 3)},
            {"min_identical_samples": cfg["flatline_min_samples"]},
            f"{sid} reported exactly {v} {spec.unit} for {b - a + 1} consecutive samples "
            f"({valued[b]['event_epoch'] - valued[a]['event_epoch']:.1f}s). Possible frozen sensor or stuck data path.",
            "A genuinely steady or heavily quantised signal can look the same. Compare with neighbouring channels."))
    return out


def _drift(sid, spec, valued, cfg) -> list[Finding]:
    seg = [r for r in valued if r["phase"] == "TEST_WINDOW"]
    seg = seg[int(len(seg) * cfg.get("drift_settle_fraction", 0.0)):]  # exclude settling at window start
    if len(seg) < int(cfg["drift_min_samples"]):
        return []
    xs = [r["event_epoch"] - seg[0]["event_epoch"] for r in seg]
    ys = [r["value"] for r in seg]
    duration = xs[-1]
    mean_y = statistics.fmean(ys)
    if duration <= 0 or abs(mean_y) < 1e-9:
        return []
    mx = statistics.fmean(xs)
    sxx = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - mean_y) for x, y in zip(xs, ys)) / sxx if sxx else 0.0
    rel = slope * duration / abs(mean_y)
    blocks = 10
    size = len(ys) // blocks
    means = [statistics.fmean(ys[i * size:(i + 1) * size]) for i in range(blocks)]
    steps = [b - a for a, b in zip(means, means[1:])]
    sign = 1 if slope > 0 else -1
    mono = sum(1 for s in steps if s * sign > 0) / len(steps)
    if abs(rel) >= cfg["drift_rel_change"] and mono >= cfg["drift_monotonic_fraction"]:
        return [Finding(
            "AN_DRIFT", "Gradual drift in steady window", "UNUSUAL_SENSOR_PATTERN", "WARNING", sid,
            seg[0]["event_ts"], seg[-1]["event_ts"],
            {"linear_trend_per_s": round(slope, 5), "relative_change_over_window_pct": round(rel * 100, 1),
             "monotonic_block_fraction": round(mono, 2), "window_mean": round(mean_y, 3), "unit": spec.unit},
            {"relative_change": cfg["drift_rel_change"], "monotonic_fraction": cfg["drift_monotonic_fraction"],
             "window": "TEST_WINDOW phase"},
            f"Within TEST_WINDOW, {sid} shows a consistent {'upward' if slope > 0 else 'downward'} trend of "
            f"{abs(rel) * 100:.0f}% of its window mean ({mono * 100:.0f}% of block-to-block steps in the same "
            "direction). Gradual drift - review recommended.",
            "Slow changes can be genuine operating effects (e.g. thermal soak). Only the labelled TEST_WINDOW "
            "phase is examined, so mislabelled phases affect this rule.")]
    return []


def _missing_burst(sid, spec, recs, cfg) -> list[Finding]:
    out = []
    times = sorted({r["event_epoch"] for r in recs})
    exp, _ = effective_interval(times, spec.expected_interval_s, 0.5)
    if exp:
        thr = exp * cfg["missing_burst_factor"]
        for a, b in zip(times, times[1:]):
            if b - a > thr:
                missing_est = round((b - a) / exp - 1)
                out.append(Finding(
                    "AN_MISSING_BURST", "Missing-data burst", "POSSIBLE_DATA_QUALITY_PROBLEM", "WARNING", sid,
                    _iso(a), _iso(b),
                    {"gap_s": round(b - a, 3), "estimated_missing_samples": missing_est, "basis": "estimate from configured interval"},
                    {"max_gap_factor": cfg["missing_burst_factor"], "expected_interval_s": exp},
                    f"No samples from {sid} for {b - a:.2f}s (>{cfg['missing_burst_factor']}x the expected {exp}s). "
                    f"About {missing_est} samples are estimated missing (an estimate, not an exact count).",
                    "Could be sensor dropout, network loss, or an ingestion interruption."))
    run, start = 0, None
    for r in recs:
        if r["value"] is None:
            run += 1
            start = start or r
            if run == int(cfg["missing_burst_factor"]):
                out.append(Finding(
                    "AN_MISSING_BURST", "Missing-data burst", "POSSIBLE_DATA_QUALITY_PROBLEM", "WARNING", sid,
                    start["event_ts"], r["event_ts"], {"consecutive_null_values": run}, {"min_consecutive": cfg["missing_burst_factor"]},
                    f"{run}+ consecutive records of {sid} carried no measurement value.", "Missing is not zero."))
        else:
            run, start = 0, None
    return out


def _interval(sid, spec, recs, cfg) -> list[Finding]:
    times = sorted({r["event_epoch"] for r in recs})
    exp, _ = effective_interval(times, spec.expected_interval_s, 0.5)
    deltas = [b - a for a, b in zip(times, times[1:])]
    if exp:  # exclude bursts (reported by the missing-burst rule)
        deltas = [d for d in deltas if d <= exp * cfg["missing_burst_factor"]]
    if len(deltas) < 10:
        return []
    mean = statistics.fmean(deltas)
    cv = statistics.pstdev(deltas) / mean if mean else 0.0
    if cv > cfg["interval_cv_threshold"]:
        return [Finding(
            "AN_INTERVAL_IRREGULAR", "Sampling interval irregularity", "POSSIBLE_DATA_QUALITY_PROBLEM", "WARNING", sid,
            _iso(times[0]), _iso(times[-1]),
            {"interval_cv": round(cv, 3), "mean_interval_s": round(mean, 4), "min_interval_s": round(min(deltas), 4),
             "max_interval_s": round(max(deltas), 4)},
            {"cv_threshold": cfg["interval_cv_threshold"]},
            f"Sampling intervals for {sid} vary strongly (coefficient of variation {cv:.2f}, threshold "
            f"{cfg['interval_cv_threshold']}).", "Jitter may come from the source clock, batching, or transport.")]
    return []


def _cross_sensor(series, spec_map, cfg) -> list[Finding]:
    spans = {sid: (recs[0]["event_epoch"], recs[-1]["event_epoch"], recs) for sid, recs in series.items()
             if sid in spec_map and spec_map[sid].enabled and recs}
    if len(spans) < 3:
        return []
    med_first = statistics.median(a for a, _, _ in spans.values())
    med_last = statistics.median(b for _, b, _ in spans.values())
    out = []
    for sid, (a, b, recs) in spans.items():
        times = sorted({r["event_epoch"] for r in recs})
        own = statistics.median([y - x for x, y in zip(times, times[1:])]) if len(times) > 1 else 0
        tol = cfg["cross_sensor_mismatch_s"] + own
        if abs(a - med_first) > tol or abs(b - med_last) > tol:
            out.append(Finding(
                "AN_CROSS_SENSOR_TIME_MISMATCH", "Cross-sensor timestamp mismatch", "POSSIBLE_DATA_QUALITY_PROBLEM",
                "WARNING", sid, _iso(min(a, med_first)), _iso(max(b, med_last)),
                {"first_offset_s": round(a - med_first, 3), "last_offset_s": round(b - med_last, 3)},
                {"tolerance_s": round(tol, 3), "reference": "median first/last timestamp across sensors"},
                f"{sid} starts {a - med_first:+.2f}s and ends {b - med_last:+.2f}s relative to the median of all sensors "
                f"(tolerance {tol:.2f}s). Possible clock offset or late/early start of this channel.",
                "Channels may legitimately start/stop at different times; check the acquisition setup."))
    return out


def _phase(series, settings: Settings) -> list[Finding]:
    order = settings.expected_phase_order
    idx = {p: i for i, p in enumerate(order)}
    bad: dict[tuple[str, str], dict] = {}
    for sid, recs in series.items():
        prev = None
        for r in recs:
            ph = r["phase"]
            if ph not in idx:
                continue
            if prev is not None and ph != prev and idx[ph] != idx[prev] + 1:
                d = bad.setdefault((prev, ph), {"sensors": set(), "start": r["event_ts"], "end": r["event_ts"]})
                d["sensors"].add(sid)
                d["start"] = min(d["start"], r["event_ts"])
                d["end"] = max(d["end"], r["event_ts"])
            prev = ph
    return [Finding(
        "AN_PHASE_TRANSITION", "Unexpected phase transition", "UNUSUAL_SENSOR_PATTERN", "WARNING", None,
        d["start"], d["end"], {"transition": f"{a} -> {b}", "sensors_affected": sorted(d["sensors"])},
        {"expected_phase_order": order},
        f"Phase labels moved {a} -> {b}, which does not follow the configured order "
        f"({' -> '.join(order)}); seen on {len(d['sensors'])} sensor(s).",
        "Phase labels come from the data source. A label glitch is more likely than a real out-of-order test sequence; "
        "this is a software timeline check, not operating guidance.") for (a, b), d in sorted(bad.items())]
