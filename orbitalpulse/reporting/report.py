"""Deterministic JSON / CSV / HTML run reports. Same data + config => identical content, apart from the
explicit `generated_at` field. Executive summary is generated from computed values only."""
from __future__ import annotations

import csv
import html
import io
import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

from orbitalpulse import DISCLAIMER, PROCESSING_VERSION, SCHEMA_VERSION, __version__
from orbitalpulse.analytics.compare import compare_runs
from orbitalpulse.analytics.metrics import run_summary, sensor_metrics
from orbitalpulse.config import config_fingerprint, load_sensor_config, load_settings

LIMITATIONS = [
    "Rule-based screening only; findings are 'review recommended', never confirmed malfunctions.",
    "Thresholds and sensor ranges are synthetic demonstration settings, not engineering limits.",
    "Completeness and missing-sample rates are ESTIMATES derived from configured/observed sampling intervals "
    "unless sequence numbers are present (then missing-sequence counts are exact).",
    "Offline analytics only; not for control, launch authorization, shutdown logic, or safety-critical monitoring.",
]


def build_report(conn: sqlite3.Connection, run_ids: list[str], *, include_comparison: bool = True) -> dict[str, Any]:
    sensors_cfg, settings = load_sensor_config(), load_settings()
    sensors = sensors_cfg.by_id()
    runs = []
    for rid in sorted(run_ids):
        meta = conn.execute("SELECT * FROM runs WHERE run_id=?", (rid,)).fetchone()
        if meta is None:
            raise KeyError(rid)
        jobs = [dict(j) for j in conn.execute(
            "SELECT job_id, source_type, source_name, file_sha256, status, rows_total, imported, duplicates, quarantined, "
            "processing_version FROM ingestion_jobs WHERE job_id IN (SELECT DISTINCT job_id FROM events WHERE run_id=?) "
            "ORDER BY job_id", (rid,))]
        issues = [dict(i) for i in conn.execute(
            "SELECT issue_id, scope, rule_id, severity, sensor_id, event_id, observed, expected, explanation, source_ref, "
            "window_start, window_end, count FROM quality_issues WHERE run_id=? ORDER BY severity DESC, rule_id, "
            "sensor_id, issue_id LIMIT 500", (rid,))]
        issue_counts = [dict(i) for i in conn.execute(
            "SELECT rule_id, severity, COUNT(*) n FROM quality_issues WHERE run_id=? GROUP BY rule_id, severity "
            "ORDER BY rule_id", (rid,))]
        findings = []
        for f in conn.execute("SELECT * FROM findings WHERE run_id=? ORDER BY rule_id, sensor_id, window_start, finding_id", (rid,)):
            d = dict(f)
            d["evidence"], d["threshold"] = json.loads(d["evidence"]), json.loads(d["threshold"])
            d["evidence_reference"] = f"/api/runs/{rid}/timeseries?sensor_ids={d['sensor_id']}" if d["sensor_id"] else None
            findings.append(d)
        summary = run_summary(conn, rid, sensors)
        runs.append({
            "run_id": rid, "campaign_id": meta["campaign_id"], "scenario": meta["scenario"],
            "synthetic": bool(meta["synthetic"]), "source_type": meta["source_type"], "ingestion_jobs": jobs,
            "summary": summary, "sensor_metrics": sensor_metrics(conn, rid, sensors),
            "quality_issue_counts": issue_counts, "quality_issues": issues, "findings": findings,
            "executive_summary": _executive_summary(summary, findings, issue_counts),
        })
    report: dict[str, Any] = {
        "meta": {"title": "OrbitalPulse run report", "generated_at": datetime.now(UTC).isoformat(),
                 "app_version": __version__, "schema_version": SCHEMA_VERSION, "processing_version": PROCESSING_VERSION,
                 "config_version": config_fingerprint(sensors_cfg, settings), "sensor_config": sensors_cfg.version,
                 "settings_version": settings.version},
        "disclaimer": DISCLAIMER if any(r["synthetic"] for r in runs) else DISCLAIMER.replace("SYNTHETIC DATA. ", ""),
        "runs": runs, "limitations": LIMITATIONS,
    }
    if include_comparison and len(runs) > 1:
        report["comparison"] = compare_runs(conn, [r["run_id"] for r in runs], sensors)
    return report


def _executive_summary(summary: dict, findings: list[dict], issue_counts: list[dict]) -> list[str]:
    lines = [f"{summary['event_count']:,} events stored over {summary['duration_s']:.1f}s across {summary['sensor_count']} sensors; "
             f"{summary['quarantined_count']:,} record(s) quarantined."]
    if summary["completeness_pct"] is not None:
        lines.append(f"Average estimated completeness is {summary['completeness_pct']:.1f}% (estimate; see limitations).")
    sev = summary["issues_by_severity"]
    lines.append("Data-quality issues: " + (", ".join(f"{n} {s}" for s, n in sorted(sev.items())) if sev else "none recorded") + ".")
    real = [f for f in findings if f["rule_id"] != "AN_INSUFFICIENT_DATA"]
    insuff = len(findings) - len(real)
    if real:
        by_rule: dict[str, int] = {}
        for f in real:
            by_rule[f["rule_id"]] = by_rule.get(f["rule_id"], 0) + 1
        lines.append("Screening findings (review recommended): " + ", ".join(f"{r} x{n}" for r, n in sorted(by_rule.items())) + ".")
    else:
        lines.append("No screening rule fired. This does not prove the data or the test were healthy.")
    if insuff:
        lines.append(f"{insuff} sensor(s) had insufficient data for rule evaluation (missing evidence).")
    lines.append("These are rule outputs on the recorded data, not engineering conclusions.")
    return lines


def report_csv(report: dict) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["run_id", "sensor_id", "unit", "record_count", "valid_count", "missing_value_count", "invalid_count", "min",
                "max", "mean", "median", "std", "first_timestamp", "last_timestamp", "duration_s", "observed_interval_median_s",
                "expected_interval_s", "estimated_missing_sample_rate", "completeness_pct", "completeness_basis"])
    for run in report["runs"]:
        for m in run["sensor_metrics"]:
            w.writerow([run["run_id"]] + [m[k] for k in ("sensor_id", "unit", "record_count", "valid_count",
                        "missing_value_count", "invalid_count", "min", "max", "mean", "median", "std", "first_timestamp",
                        "last_timestamp", "duration_s", "observed_interval_median_s", "expected_interval_s",
                        "estimated_missing_sample_rate", "completeness_pct", "completeness_basis")])
    return buf.getvalue()


def report_html(report: dict) -> str:
    e = html.escape
    def f(v, n=3):
        return "-" if v is None else (f"{v:.{n}f}" if isinstance(v, float) else e(str(v)))
    parts = [f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>OrbitalPulse report</title><style>
body{{font:14px/1.5 system-ui,sans-serif;background:#0f1419;color:#d7dde4;max-width:1100px;margin:2rem auto;padding:0 1rem}}
h1,h2,h3{{color:#fff}}table{{border-collapse:collapse;width:100%;margin:.5rem 0 1.5rem;font-size:13px}}
th,td{{border:1px solid #2a3440;padding:4px 8px;text-align:left}}th{{background:#1a222c}}
.warn{{border:1px solid #b58a1a;background:#2a2410;padding:.6rem 1rem;border-radius:4px}}.mut{{color:#8b98a7}}
</style></head><body><h1>OrbitalPulse run report</h1><div class="warn">{e(report['disclaimer'])}</div>
<p class="mut">Generated {e(report['meta']['generated_at'])} - app {e(report['meta']['app_version'])} - schema {e(report['meta']['schema_version'])}
 - processing {e(report['meta']['processing_version'])} - config {e(report['meta']['config_version'])}</p>"""]
    for r in report["runs"]:
        parts.append(f"<h2>Run {e(r['run_id'])}</h2><p class='mut'>Campaign {e(r['campaign_id'])} - scenario {e(str(r['scenario']))} - "
                     f"source {e(str(r['source_type']))} - synthetic: {r['synthetic']}</p><h3>Executive summary</h3><ul>"
                     + "".join(f"<li>{e(x)}</li>" for x in r["executive_summary"]) + "</ul>")
        parts.append("<h3>Ingestion</h3><table><tr><th>Job</th><th>Source</th><th>Status</th><th>Rows</th><th>Imported</th><th>Duplicates</th><th>Quarantined</th></tr>"
                     + "".join(f"<tr><td>{e(j['job_id'])}</td><td>{e(str(j['source_name']))}</td><td>{e(j['status'])}</td><td>{j['rows_total']}</td>"
                               f"<td>{j['imported']}</td><td>{j['duplicates']}</td><td>{j['quarantined']}</td></tr>" for j in r["ingestion_jobs"]) + "</table>")
        parts.append("<h3>Sensor metrics</h3><table><tr><th>Sensor</th><th>Unit</th><th>Records</th><th>Missing</th><th>Min</th><th>Max</th><th>Mean</th><th>Median</th><th>Std</th><th>Completeness % (est.)</th></tr>"
                     + "".join(f"<tr><td>{e(m['sensor_id'])}</td><td>{e(str(m['unit']))}</td><td>{m['record_count']}</td><td>{m['missing_value_count']}</td>"
                               f"<td>{f(m['min'])}</td><td>{f(m['max'])}</td><td>{f(m['mean'])}</td><td>{f(m['median'])}</td><td>{f(m['std'])}</td><td>{f(m['completeness_pct'],1)}</td></tr>"
                               for m in r["sensor_metrics"]) + "</table>")
        parts.append("<h3>Data-quality issues</h3><table><tr><th>Rule</th><th>Severity</th><th>Count</th></tr>"
                     + "".join(f"<tr><td>{e(i['rule_id'])}</td><td>{e(i['severity'])}</td><td>{i['n']}</td></tr>" for i in r["quality_issue_counts"]) + "</table>")
        parts.append("<h3>Anomaly findings (review recommended)</h3><table><tr><th>Rule</th><th>Sensor</th><th>Window</th><th>Explanation</th><th>Review</th></tr>"
                     + "".join(f"<tr><td>{e(x['rule_id'])}</td><td>{e(str(x['sensor_id']))}</td><td>{e(str(x['window_start']))} - {e(str(x['window_end']))}</td>"
                               f"<td>{e(x['explanation'])}<br><span class='mut'>Limitations: {e(x['limitations'])}</span></td><td>{e(x['review_status'])}</td></tr>"
                               for x in r["findings"]) + "</table>")
    if "comparison" in report:
        c = report["comparison"]
        parts.append("<h2>Comparison</h2><p class='mut'>" + e(c["method_note"]) + "</p>" + "".join(f"<div class='warn'>{e(w['message'])}</div>" for w in c["warnings"])
                     + "<table><tr><th>Sensor</th><th>Status</th>" + "".join(f"<th>{e(r)} mean</th>" for r in c["run_ids"]) + "</tr>"
                     + "".join(f"<tr><td>{e(s['sensor_id'])}</td><td>{e(s['status'])}</td>" + "".join(f"<td>{f(s['per_run'].get(r, {}).get('mean'))}</td>" for r in c["run_ids"]) + "</tr>" for s in c["sensors"]) + "</table>")
    parts.append("<h2>Known limitations</h2><ul>" + "".join(f"<li>{e(x)}</li>" for x in report["limitations"]) + "</ul></body></html>")
    return "".join(parts)
