"""Post-ingestion analysis: recomputes run-level quality issues and anomaly findings deterministically.
Analyst review decisions on findings are preserved across re-analysis (matched by finding_id)."""
from __future__ import annotations

import json
import sqlite3

from orbitalpulse import PROCESSING_VERSION
from orbitalpulse.analytics.anomalies import analyze
from orbitalpulse.config import SensorConfig, Settings, config_fingerprint, load_sensor_config, load_settings
from orbitalpulse.ingestion.core import _issue_id, now_iso
from orbitalpulse.quality.rules import check_run
from orbitalpulse.storage.db import log_activity


def analyze_run(conn: sqlite3.Connection, run_id: str, sensors: SensorConfig | None = None,
                settings: Settings | None = None) -> dict:
    sensors = sensors or load_sensor_config()
    settings = settings or load_settings()
    cfg_ver = config_fingerprint(sensors, settings)
    run = conn.execute("SELECT campaign_id FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if run is None:
        raise KeyError(run_id)
    ts = now_iso()

    conn.execute("DELETE FROM quality_issues WHERE run_id=? AND scope='RUN'", (run_id,))
    issues = check_run(conn, run_id, sensors, settings)
    for i in issues:
        conn.execute(
            "INSERT OR REPLACE INTO quality_issues(issue_id, scope, campaign_id, run_id, event_id, sensor_id, rule_id,"
            " severity, observed, expected, explanation, detected_at, source_ref, job_id, window_start, window_end, count)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (_issue_id("RUN", i.rule_id, run_id, i.sensor_id, i.window_start), "RUN", run["campaign_id"], run_id, None,
             i.sensor_id, i.rule_id, i.severity, i.observed, i.expected, i.explanation, ts,
             f"run:{run_id}", None, i.window_start, i.window_end, i.count))

    findings = analyze(conn, run_id, sensors, settings)
    keep = {f.finding_id for f in findings}
    for (fid,) in conn.execute("SELECT finding_id FROM findings WHERE run_id=?", (run_id,)).fetchall():
        if fid not in keep:
            conn.execute("DELETE FROM findings WHERE finding_id=?", (fid,))
    for f in findings:
        conn.execute(
            "INSERT INTO findings(finding_id, run_id, sensor_id, rule_id, rule_name, category, severity, window_start,"
            " window_end, evidence, threshold, explanation, limitations, config_version, processing_version, detected_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(finding_id) DO UPDATE SET evidence=excluded.evidence,"
            " threshold=excluded.threshold, explanation=excluded.explanation, severity=excluded.severity,"
            " config_version=excluded.config_version, processing_version=excluded.processing_version",
            (f.finding_id, run_id, f.sensor_id, f.rule_id, f.rule_name, f.category, f.severity, f.window_start,
             f.window_end, json.dumps(f.evidence), json.dumps(f.threshold), f.explanation, f.limitations, cfg_ver,
             PROCESSING_VERSION, ts))
    conn.execute("UPDATE runs SET analyzed_at=?, config_version=? WHERE run_id=?", (ts, cfg_ver, run_id))
    log_activity(conn, "ANALYZE", f"{run_id}: {len(issues)} run-level quality issues, {len(findings)} findings")
    conn.commit()
    return {"run_id": run_id, "quality_issues": len(issues), "findings": len(findings), "config_version": cfg_ver}


def analyze_runs(conn: sqlite3.Connection, run_ids) -> list[dict]:
    sensors, settings = load_sensor_config(), load_settings()
    return [analyze_run(conn, r, sensors, settings) for r in sorted(set(run_ids))]
