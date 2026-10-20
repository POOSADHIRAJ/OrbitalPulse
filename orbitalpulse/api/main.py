"""OrbitalPulse REST API (FastAPI). Run with:  python -m uvicorn orbitalpulse.api.main:app --port 8000"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator, Literal, NoReturn

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from orbitalpulse import DISCLAIMER, PROCESSING_VERSION, SCHEMA_VERSION, __version__
from orbitalpulse.analytics.compare import compare_runs
from orbitalpulse.analytics.metrics import run_summary, sensor_metrics
from orbitalpulse.analytics.timeseries import run_bounds, timeseries
from orbitalpulse.api import procs
from orbitalpulse.config import (
    PROJECT_ROOT,
    SensorConfig,
    Settings,
    config_fingerprint,
    data_dir,
    load_sensor_config,
    load_settings,
    save_override,
)
from orbitalpulse.demo import load_demo, reset_demo
from orbitalpulse.ingestion.csv_ingest import IngestError, ingest_csv, preview_csv
from orbitalpulse.pipeline import analyze_run, analyze_runs
from orbitalpulse.reporting.report import build_report, report_csv, report_html
from orbitalpulse.schemas.events import SEVERITY_ORDER
from orbitalpulse.storage import archive
from orbitalpulse.storage.db import log_activity, session
from orbitalpulse.streaming.status import broker_reachable, read_status

log = logging.getLogger("orbitalpulse.api")
FRONTEND = PROJECT_ROOT / "frontend"

app = FastAPI(title="OrbitalPulse API", version=__version__,
              description=DISCLAIMER + "\n\nAll findings are rule-based screening results, not engineering conclusions.")
origins = [o.strip() for o in os.environ.get("ORBITALPULSE_CORS_ORIGINS", "http://localhost:8000,http://127.0.0.1:8000,"
                                                                          "http://localhost:5173").split(",") if o.strip()]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
                   allow_headers=["*"])


def get_conn() -> Iterator:
    with session() as conn:
        yield conn


@app.exception_handler(Exception)
async def unhandled(_: Request, exc: Exception) -> JSONResponse:
    log.exception("unhandled error: %s", exc)  # full detail only in the server log
    return JSONResponse(status_code=500, content={"detail": "Internal server error. See server log."})


def _limits() -> dict:
    return load_settings().limits


def _page(conn, sql: str, args: list, limit: int | None, offset: int, order: str, count_sql: str | None = None) -> dict:
    lim = min(limit or _limits()["default_page_size"], _limits()["max_page_size"])
    total = conn.execute(count_sql or f"SELECT COUNT(*) FROM ({sql})", args).fetchone()[0]
    rows = conn.execute(f"{sql} ORDER BY {order} LIMIT ? OFFSET ?", [*args, lim, offset]).fetchall()
    return {"items": [dict(r) for r in rows], "total": total, "limit": lim, "offset": offset}


def _need_run(conn, run_id: str):
    row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        raise HTTPException(404, f"Run '{run_id}' not found")
    return row


def _epoch(value: str | None, name: str) -> float | None:
    """ISO-8601 (naive = UTC) or epoch seconds -> epoch float."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        pass
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(422, f"'{name}' must be ISO-8601 or epoch seconds") from None
    return (ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts).timestamp()


# ------------------------------------------------------------------ health / status
@app.get("/api/health", tags=["system"])
def health() -> dict:
    """Liveness only: the process is up."""
    return {"status": "ok", "version": __version__}


@app.get("/api/ready", tags=["system"])
def ready(conn=Depends(get_conn)) -> JSONResponse:
    """Readiness: database reachable + configuration valid. Kafka is optional and reported separately."""
    checks: dict[str, Any] = {}
    ok = True
    try:
        conn.execute("SELECT 1 FROM schema_meta LIMIT 1").fetchone()
        checks["database"] = {"ok": True}
    except Exception as exc:
        ok = False
        checks["database"] = {"ok": False, "error": str(exc)}
    try:
        load_sensor_config(), load_settings()
        checks["configuration"] = {"ok": True}
    except Exception as exc:
        ok = False
        checks["configuration"] = {"ok": False, "error": str(exc)[:300]}
    checks["data_dir_writable"] = {"ok": os.access(data_dir(), os.W_OK)}
    ok = ok and checks["data_dir_writable"]["ok"]
    checks["kafka_optional"] = broker_reachable(load_settings().kafka["bootstrap_servers"], 0.5)
    return JSONResponse(status_code=200 if ok else 503, content={"ready": ok, "checks": checks})


@app.get("/api/status", tags=["system"])
def status(conn=Depends(get_conn)) -> dict:
    """Application mode and service status. Mode is LIVE_KAFKA only if a broker answers AND a consumer heartbeat is fresh."""
    k = load_settings().kafka
    broker = broker_reachable(k["bootstrap_servers"], 0.7)
    consumer = read_status(conn, "kafka_consumer")
    live = bool(broker["reachable"] and consumer and consumer["alive"])
    return {"mode": "LIVE_KAFKA" if live else "BATCH_DEMO",
            "mode_explanation": ("Kafka broker reachable and consumer heartbeat fresh." if live else
                                 "Batch/demo mode: CSV import and demo data work without Kafka."),
            "kafka": {**broker, "topic_raw": k["topic_raw"], "consumer_group": k["consumer_group"]},
            "consumer": consumer or {"state": "NOT_STARTED", "alive": False},
            "consumer_process": procs.status("consumer"), "producer_process": procs.status("producer"),
            "versions": {"app": __version__, "schema": SCHEMA_VERSION, "processing": PROCESSING_VERSION},
            "disclaimer": DISCLAIMER}


@app.get("/api/overview", tags=["system"])
def overview(conn=Depends(get_conn)) -> dict:
    sensors = load_sensor_config().by_id()
    n = lambda sql: conn.execute(sql).fetchone()[0]  # noqa: E731 (fixed SQL, no user input)
    runs = [r["run_id"] for r in conn.execute("SELECT run_id FROM runs ORDER BY run_id")]
    comps = [s["completeness_pct"] for s in (run_summary(conn, r, sensors) for r in runs) if s["completeness_pct"] is not None]
    return {
        "campaigns": n("SELECT COUNT(*) FROM campaigns"), "runs": len(runs), "events_valid": n("SELECT COUNT(*) FROM events"),
        "events_missing_value": n("SELECT COUNT(*) FROM events WHERE value IS NULL"),
        "quarantined": n("SELECT COUNT(*) FROM quarantine"), "duplicates_ignored": n("SELECT COALESCE(SUM(duplicates),0) FROM ingestion_jobs"),
        "completeness_pct": sum(comps) / len(comps) if comps else None,
        "issues_by_severity": {r["severity"]: r["n"] for r in conn.execute("SELECT severity, COUNT(*) n FROM quality_issues GROUP BY severity")},
        "findings_by_review": {r["review_status"]: r["n"] for r in conn.execute("SELECT review_status, COUNT(*) n FROM findings GROUP BY review_status")},
        "recent_activity": [dict(r) for r in conn.execute("SELECT ts, kind, message FROM activity ORDER BY id DESC LIMIT 12")],
        "recent_jobs": [dict(r) for r in conn.execute("SELECT job_id, source_type, source_name, status, started_at, rows_total, imported, duplicates, quarantined FROM ingestion_jobs ORDER BY started_at DESC LIMIT 8")],
        "disclaimer": DISCLAIMER,
    }


# ------------------------------------------------------------------ campaigns / runs
@app.get("/api/campaigns", tags=["runs"])
def campaigns(conn=Depends(get_conn)) -> list[dict]:
    rows = conn.execute("SELECT c.campaign_id, c.name, c.synthetic, c.created_at, COUNT(r.run_id) run_count FROM campaigns c "
                        "LEFT JOIN runs r USING(campaign_id) GROUP BY c.campaign_id ORDER BY c.campaign_id").fetchall()
    return [dict(r) for r in rows]


@app.get("/api/runs", tags=["runs"])
def runs(campaign_id: str | None = None, conn=Depends(get_conn)) -> list[dict]:
    sensors = load_sensor_config().by_id()
    sql, args = "SELECT * FROM runs", []
    if campaign_id:
        sql, args = sql + " WHERE campaign_id=?", [campaign_id]
    out = []
    for r in conn.execute(sql + " ORDER BY run_id", args).fetchall():
        d = dict(r)
        d["summary"] = run_summary(conn, r["run_id"], sensors)
        out.append(d)
    return out


@app.get("/api/runs/{run_id}", tags=["runs"])
def run_detail(run_id: str, conn=Depends(get_conn)) -> dict:
    row = _need_run(conn, run_id)
    d = dict(row)
    d["summary"] = run_summary(conn, run_id, load_sensor_config().by_id())
    d["ingestion_jobs"] = [dict(j) for j in conn.execute(
        "SELECT job_id, source_type, source_name, status, started_at, finished_at, rows_total, imported, duplicates, quarantined "
        "FROM ingestion_jobs WHERE job_id IN (SELECT DISTINCT job_id FROM events WHERE run_id=?) ORDER BY started_at", (run_id,))]
    return d


@app.post("/api/runs/{run_id}/reanalyze", tags=["runs"])
def reanalyze(run_id: str, conn=Depends(get_conn)) -> dict:
    _need_run(conn, run_id)
    return analyze_run(conn, run_id)


@app.get("/api/runs/{run_id}/metrics", tags=["analytics"])
def metrics(run_id: str, sensor_id: str | None = None, phase: str | None = None, start: str | None = None,
            end: str | None = None, conn=Depends(get_conn)) -> dict:
    _need_run(conn, run_id)
    return {"run_id": run_id, "filters": {"sensor_id": sensor_id, "phase": phase, "start": start, "end": end},
            "metrics": sensor_metrics(conn, run_id, load_sensor_config().by_id(), sensor_id=sensor_id, phase=phase,
                                      start=_epoch(start, "start"), end=_epoch(end, "end")),
            "note": "Completeness / missing-sample rate are estimates unless sequence numbers prove the exact count."}


@app.get("/api/runs/{run_id}/events", tags=["analytics"])
def events(run_id: str, sensor_id: str | None = None, phase: str | None = None, start: str | None = None,
           end: str | None = None, limit: int = Query(100, ge=1), offset: int = Query(0, ge=0), conn=Depends(get_conn)) -> dict:
    _need_run(conn, run_id)
    where: list[str] = ["run_id=?"]
    args: list[Any] = [run_id]
    for col, val in (("sensor_id", sensor_id), ("phase", phase)):
        if val:
            where.append(f"{col}=?"); args.append(val)
    for bound, op in ((_epoch(start, "start"), ">="), (_epoch(end, "end"), "<=")):
        if bound is not None:
            where.append(f"event_epoch{op}?"); args.append(bound)
    sql = ("SELECT event_id, sensor_id, event_ts, ingest_ts, sequence_number, value, unit, phase, quality_status, job_id, "
           f"source_type, schema_version FROM events WHERE {' AND '.join(where)}")
    return _page(conn, sql, args, limit, offset, "event_epoch, rowid_arrival")


@app.get("/api/runs/{run_id}/timeseries", tags=["analytics"])
def run_timeseries(run_id: str, sensor_ids: str = Query(..., description="comma-separated"), phase: str | None = None,
                   start: str | None = None, end: str | None = None, max_points: int | None = None,
                   align: Literal["absolute", "elapsed"] = "absolute", conn=Depends(get_conn)) -> dict:
    _need_run(conn, run_id)
    cap = min(max_points or _limits()["max_chart_points"], _limits()["max_chart_points"])
    specs = load_sensor_config().by_id()
    series = []
    for sid in [s.strip() for s in sensor_ids.split(",") if s.strip()][:12]:
        if sid not in specs:
            raise HTTPException(404, f"Unknown sensor '{sid}'")
        ts = timeseries(conn, run_id, sid, max_points=cap, phase=phase, start=_epoch(start, "start"), end=_epoch(end, "end"), align=align)
        ts.update(unit=specs[sid].unit, expected_interval_s=specs[sid].expected_interval_s, display_name=specs[sid].display_name)
        series.append(ts)
    return {"run_id": run_id, "series": series}


@app.get("/api/runs/{run_id}/timeline", tags=["runs"])
def timeline(run_id: str, buckets: int = Query(60, ge=10, le=240), conn=Depends(get_conn)) -> dict:
    row = _need_run(conn, run_id)
    b = run_bounds(conn, run_id)
    if not b:
        return {"run_id": run_id, "empty": True, "run": dict(row)}
    a, z = b
    width = max((z - a) / buckets, 1e-6)
    hist = [0] * buckets
    for (ep,) in conn.execute("SELECT event_epoch FROM events WHERE run_id=?", (run_id,)):
        hist[min(buckets - 1, int((ep - a) / width))] += 1
    iso = lambda e: datetime.fromtimestamp(e, UTC).isoformat()  # noqa: E731
    phases = [{"phase": r["phase"], "start": iso(r["a"]), "end": iso(r["b"]), "start_s": r["a"] - a, "end_s": r["b"] - a}
              for r in conn.execute("SELECT phase, MIN(event_epoch) a, MAX(event_epoch) b FROM events WHERE run_id=? GROUP BY phase ORDER BY a", (run_id,))]
    def to_s(ts):
        return (datetime.fromisoformat(ts).timestamp() - a) if ts else None
    issues = [{**dict(r), "start_s": to_s(r["window_start"]), "end_s": to_s(r["window_end"])} for r in conn.execute(
        "SELECT issue_id, rule_id, severity, sensor_id, window_start, window_end, explanation FROM quality_issues "
        "WHERE run_id=? AND window_start IS NOT NULL ORDER BY window_start LIMIT 300", (run_id,))]
    findings = [{**dict(r), "start_s": to_s(r["window_start"]), "end_s": to_s(r["window_end"])} for r in conn.execute(
        "SELECT finding_id, rule_id, rule_name, severity, sensor_id, window_start, window_end, review_status FROM findings "
        "WHERE run_id=? AND window_start IS NOT NULL ORDER BY window_start LIMIT 300", (run_id,))]
    gaps = [i for i in issues if i["rule_id"] in ("DQ_TIME_GAP",)] + [f for f in findings if f["rule_id"] == "AN_MISSING_BURST"]
    jobs = [dict(j) for j in conn.execute("SELECT job_id, source_type, status, started_at, finished_at FROM ingestion_jobs "
                                          "WHERE job_id IN (SELECT DISTINCT job_id FROM events WHERE run_id=?) ORDER BY started_at", (run_id,))]
    return {"run_id": run_id, "run": dict(row), "start": iso(a), "end": iso(z), "duration_s": z - a,
            "event_volume": {"bucket_s": width, "counts": hist}, "phases": phases, "quality_issues": issues,
            "findings": findings, "gaps": gaps, "ingestion_jobs": jobs,
            "ingestion_interruptions": [j for j in jobs if j["status"] not in ("COMPLETED", "COMPLETED_WITH_ERRORS")]}


# ------------------------------------------------------------------ comparison
@app.get("/api/compare", tags=["analytics"])
def compare(run_ids: str = Query(..., description="comma-separated, at least two"), conn=Depends(get_conn)) -> dict:
    ids = list(dict.fromkeys(s.strip() for s in run_ids.split(",") if s.strip()))
    if len(ids) < 2:
        raise HTTPException(422, "Provide at least two distinct run ids.")
    for r in ids:
        _need_run(conn, r)
    return compare_runs(conn, ids, load_sensor_config().by_id())


# ------------------------------------------------------------------ quality / quarantine
def _issue_filters(run_id, rule_id, severity, sensor_id, scope):
    where, args = ["1=1"], []
    for col, val in (("run_id", run_id), ("rule_id", rule_id), ("severity", severity), ("sensor_id", sensor_id), ("scope", scope)):
        if val:
            where.append(f"{col}=?"); args.append(val)
    return " AND ".join(where), args


@app.get("/api/quality/issues", tags=["quality"])
def quality_issues(run_id: str | None = None, rule_id: str | None = None, severity: str | None = None,
                   sensor_id: str | None = None, scope: str | None = None, limit: int = Query(100, ge=1),
                   offset: int = Query(0, ge=0), conn=Depends(get_conn)) -> dict:
    where, args = _issue_filters(run_id, rule_id, severity, sensor_id, scope)
    res = _page(conn, f"SELECT * FROM quality_issues WHERE {where}", args, limit, offset, "detected_at DESC, issue_id")
    res["facets"] = {"rules": [r[0] for r in conn.execute("SELECT DISTINCT rule_id FROM quality_issues ORDER BY 1")]}
    return res


@app.get("/api/quality/issues/{issue_id}", tags=["quality"])
def quality_issue(issue_id: str, conn=Depends(get_conn)) -> dict:
    row = conn.execute("SELECT * FROM quality_issues WHERE issue_id=?", (issue_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "Issue not found")
    d = dict(row)
    job = conn.execute("SELECT raw_archive FROM ingestion_jobs WHERE job_id=?", (row["job_id"],)).fetchone() if row["job_id"] else None
    d["raw_payload"] = archive.lookup(job["raw_archive"], row["source_ref"]) if job and row["source_ref"] else None
    return d


def _csv_stream(rows: Iterator[dict], fields: list[str]) -> Iterator[str]:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow(r)
        if buf.tell() > 65536:
            yield buf.getvalue(); buf.seek(0); buf.truncate()
    yield buf.getvalue()


@app.get("/api/quality/issues.csv", tags=["quality"])
def quality_issues_csv(run_id: str | None = None, rule_id: str | None = None, severity: str | None = None,
                       sensor_id: str | None = None, scope: str | None = None, conn=Depends(get_conn)) -> StreamingResponse:
    where, args = _issue_filters(run_id, rule_id, severity, sensor_id, scope)
    cur = conn.execute(f"SELECT * FROM quality_issues WHERE {where} ORDER BY run_id, rule_id, issue_id LIMIT 100000", args)
    fields = [d[0] for d in cur.description]
    return StreamingResponse(_csv_stream((dict(r) for r in cur), fields), media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="quality_issues.csv"'})


def _quar_filters(run_id, rule_id, sensor_id, job_id):
    where, args = ["1=1"], []
    for col, val in (("run_id", run_id), ("rule_id", rule_id), ("sensor_id", sensor_id), ("job_id", job_id)):
        if val:
            where.append(f"{col}=?"); args.append(val)
    return " AND ".join(where), args


@app.get("/api/quarantine", tags=["quality"])
def quarantine(run_id: str | None = None, rule_id: str | None = None, sensor_id: str | None = None, job_id: str | None = None,
               limit: int = Query(100, ge=1), offset: int = Query(0, ge=0), conn=Depends(get_conn)) -> dict:
    where, args = _quar_filters(run_id, rule_id, sensor_id, job_id)
    res = _page(conn, f"SELECT * FROM quarantine WHERE {where}", args, limit, offset, "quarantine_id")
    res["by_rule"] = {r["rule_id"]: r["n"] for r in conn.execute("SELECT rule_id, COUNT(*) n FROM quarantine GROUP BY rule_id")}
    return res


@app.get("/api/quarantine.csv", tags=["quality"])
def quarantine_csv(run_id: str | None = None, rule_id: str | None = None, sensor_id: str | None = None,
                   job_id: str | None = None, conn=Depends(get_conn)) -> StreamingResponse:
    where, args = _quar_filters(run_id, rule_id, sensor_id, job_id)
    cur = conn.execute(f"SELECT * FROM quarantine WHERE {where} ORDER BY quarantine_id LIMIT 100000", args)
    fields = [d[0] for d in cur.description]
    return StreamingResponse(_csv_stream((dict(r) for r in cur), fields), media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="quarantine.csv"'})


@app.post("/api/quarantine/replay", tags=["quality"])
def quarantine_replay(rule_id: str = Query("DQ_UNKNOWN_SENSOR"), conn=Depends(get_conn)) -> dict:
    """Re-validate quarantined payloads of one rule (e.g. after adding a sensor to the config). Valid ones are ingested."""
    from orbitalpulse.ingestion.core import IngestCounts, finish_job, ingest_records, start_job
    rows = conn.execute("SELECT quarantine_id, raw_payload, source_ref FROM quarantine WHERE rule_id=? AND replayed=0", (rule_id,)).fetchall()
    if not rows:
        return {"replayed": 0, "message": "Nothing to replay for that rule."}
    job_id, arch = start_job(conn, "QUARANTINE_REPLAY", rule_id)
    counts = IngestCounts()
    ingest_records(conn, ((f"replay:{r['quarantine_id']}", json.loads(r["raw_payload"])) for r in rows), job_id=job_id, archive=arch, counts=counts)
    finish_job(conn, job_id, counts, arch)
    # mark rows that now ingest cleanly (present in events) as replayed
    ok = 0
    for r in rows:
        eid = json.loads(r["raw_payload"]).get("event_id")
        if eid and conn.execute("SELECT 1 FROM events WHERE event_id=?", (eid,)).fetchone():
            conn.execute("UPDATE quarantine SET replayed=1 WHERE quarantine_id=?", (r["quarantine_id"],)); ok += 1
    conn.commit()
    analyze_runs(conn, counts.runs)
    return {"attempted": len(rows), "now_valid": ok, "still_quarantined": counts.quarantined, "job_id": job_id}


# ------------------------------------------------------------------ findings
def _finding(row) -> dict:
    d = dict(row)
    d["evidence"], d["threshold"] = json.loads(d["evidence"]), json.loads(d["threshold"])
    return d


@app.get("/api/findings", tags=["anomalies"])
def findings(run_id: str | None = None, rule_id: str | None = None, sensor_id: str | None = None, severity: str | None = None,
             review_status: str | None = None, category: str | None = None, limit: int = Query(100, ge=1),
             offset: int = Query(0, ge=0), conn=Depends(get_conn)) -> dict:
    where, args = ["1=1"], []
    for col, val in (("run_id", run_id), ("rule_id", rule_id), ("sensor_id", sensor_id), ("severity", severity),
                     ("review_status", review_status), ("category", category)):
        if val:
            where.append(f"{col}=?"); args.append(val)
    res = _page(conn, f"SELECT * FROM findings WHERE {' AND '.join(where)}", args, limit, offset, "run_id, window_start, finding_id")
    for it in res["items"]:
        it["evidence"], it["threshold"] = json.loads(it["evidence"]), json.loads(it["threshold"])
    res["facets"] = {"rules": [r[0] for r in conn.execute("SELECT DISTINCT rule_id FROM findings ORDER BY 1")]}
    return res


@app.get("/api/findings/{finding_id}", tags=["anomalies"])
def finding_detail(finding_id: str, padding_s: float = Query(5.0, ge=0, le=600), conn=Depends(get_conn)) -> dict:
    row = conn.execute("SELECT * FROM findings WHERE finding_id=?", (finding_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "Finding not found")
    d = _finding(row)
    d["evidence_events"], d["evidence_window"] = [], None
    if row["sensor_id"] and row["window_start"]:
        a = datetime.fromisoformat(row["window_start"]).timestamp() - padding_s
        b = datetime.fromisoformat(row["window_end"]).timestamp() + padding_s
        d["evidence_window"] = {"start_epoch": a, "end_epoch": b, "padding_s": padding_s}
        d["evidence_events"] = [dict(r) for r in conn.execute(
            "SELECT event_id, event_ts, event_epoch, value, unit, phase, sequence_number FROM events WHERE run_id=? AND sensor_id=? "
            "AND event_epoch BETWEEN ? AND ? ORDER BY event_epoch LIMIT 400", (row["run_id"], row["sensor_id"], a, b))]
    return d


class ReviewIn(BaseModel):
    status: Literal["OPEN", "REVIEWED", "FALSE_POSITIVE"]
    reviewer: str | None = None
    note: str | None = None


@app.patch("/api/findings/{finding_id}/review", tags=["anomalies"])
def review_finding(finding_id: str, body: ReviewIn, conn=Depends(get_conn)) -> dict:
    if conn.execute("SELECT 1 FROM findings WHERE finding_id=?", (finding_id,)).fetchone() is None:
        raise HTTPException(404, "Finding not found")
    conn.execute("UPDATE findings SET review_status=?, reviewed_at=?, reviewer=?, review_note=? WHERE finding_id=?",
                 (body.status, None if body.status == "OPEN" else datetime.now(UTC).isoformat(),
                  (body.reviewer or "")[:80] or None, (body.note or "")[:1000] or None, finding_id))
    log_activity(conn, "REVIEW", f"{finding_id} marked {body.status}")
    conn.commit()
    return _finding(conn.execute("SELECT * FROM findings WHERE finding_id=?", (finding_id,)).fetchone())


# ------------------------------------------------------------------ ingestion
def _raise(exc: IngestError) -> NoReturn:
    raise HTTPException(exc.status, detail={"message": str(exc), **exc.extra})


def _read_upload(file: UploadFile) -> bytes:
    limit = _limits()["max_upload_mb"] * 1024 * 1024
    data = file.file.read(limit + 1)
    if len(data) > limit:
        raise HTTPException(413, f"File exceeds the {_limits()['max_upload_mb']} MB limit.")
    return data


def _mapping(raw: str | None) -> dict | None:
    if not raw:
        return None
    try:
        m = json.loads(raw)
        if not isinstance(m, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in m.items()):
            raise ValueError
        return m
    except ValueError:
        raise HTTPException(422, "mapping must be a JSON object of {internal_field: csv_column}") from None


@app.post("/api/ingest/csv/preview", tags=["ingestion"])
def csv_preview(file: UploadFile = File(...), mapping: str | None = Form(None)) -> dict:
    data = _read_upload(file)
    try:
        return preview_csv(Path(file.filename or "upload.csv").name, data, _mapping(mapping))
    except IngestError as exc:
        _raise(exc)


@app.post("/api/ingest/csv", tags=["ingestion"], status_code=201)
def csv_ingest(file: UploadFile = File(...), mapping: str | None = Form(None), default_campaign: str = Form("UNASSIGNED"),
                     default_run_id: str | None = Form(None), conn=Depends(get_conn)) -> dict:
    data = _read_upload(file)
    try:
        return ingest_csv(conn, Path(file.filename or "upload.csv").name, data, mapping=_mapping(mapping),
                          default_campaign=default_campaign.strip() or "UNASSIGNED", default_run_id=(default_run_id or "").strip() or None)
    except IngestError as exc:
        _raise(exc)


@app.get("/api/ingestions", tags=["ingestion"])
def ingestions(limit: int = Query(50, ge=1), offset: int = Query(0, ge=0), conn=Depends(get_conn)) -> dict:
    return _page(conn, "SELECT * FROM ingestion_jobs", [], limit, offset, "started_at DESC")


@app.post("/api/demo/load", tags=["demo"])
def demo_load(reset: bool = False, conn=Depends(get_conn)) -> dict:
    """Generate the deterministic synthetic campaign and import it through the real CSV pipeline (idempotent)."""
    return load_demo(conn, reset=reset)


@app.post("/api/demo/reset", tags=["demo"])
def demo_reset(conn=Depends(get_conn)) -> dict:
    """Delete ALL stored data (events, quarantine, findings, jobs). Configuration overrides are kept."""
    reset_demo(conn)
    return {"reset": True}


class StreamStart(BaseModel):
    runs: list[str] | None = None
    rate: float = 200.0
    redeliver: float = 0.0


@app.post("/api/stream/producer/start", tags=["streaming"])
def producer_start(body: StreamStart) -> dict:
    k = load_settings().kafka
    reach = broker_reachable(k["bootstrap_servers"], 0.7)
    if not reach["reachable"]:
        raise HTTPException(503, detail={"message": "Kafka broker is not reachable; start it first (scripts\\kafka_start.ps1). "
                                                    "Batch/demo mode remains available.", **reach})
    if any(not r.replace("-", "").replace("_", "").isalnum() for r in (body.runs or [])):
        raise HTTPException(422, "invalid run id")
    args = ["--rate", str(max(0.0, min(body.rate, 20000))), "--redeliver", str(max(0.0, min(body.redeliver, 0.5)))]
    if body.runs:
        args += ["--runs", *body.runs]
    return procs.start("producer", args)


@app.post("/api/stream/producer/stop", tags=["streaming"])
def producer_stop() -> dict:
    return procs.stop("producer")


@app.post("/api/stream/consumer/start", tags=["streaming"])
def consumer_start() -> dict:
    k = load_settings().kafka
    reach = broker_reachable(k["bootstrap_servers"], 0.7)
    if not reach["reachable"]:
        raise HTTPException(503, detail={"message": "Kafka broker is not reachable; start it first.", **reach})
    return procs.start("consumer", [])


@app.post("/api/stream/consumer/stop", tags=["streaming"])
def consumer_stop() -> dict:
    return procs.stop("consumer")


# ------------------------------------------------------------------ reports
@app.get("/api/report", tags=["reports"])
def report(run_ids: str = Query(..., description="comma-separated run ids"), format: Literal["json", "csv", "html"] = "json",
           comparison: bool = True, conn=Depends(get_conn)) -> Response:
    ids = [s.strip() for s in run_ids.split(",") if s.strip()]
    if not ids:
        raise HTTPException(422, "run_ids is required")
    try:
        rep = build_report(conn, ids, include_comparison=comparison)
    except KeyError as exc:
        raise HTTPException(404, f"Run {exc} not found") from None
    name = "orbitalpulse_report_" + "_".join(ids)[:80]
    if format == "csv":
        return Response(report_csv(rep), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{name}.csv"'})
    if format == "html":
        return HTMLResponse(report_html(rep), headers={"Content-Disposition": f'attachment; filename="{name}.html"'})
    return JSONResponse(rep, headers={"Content-Disposition": f'attachment; filename="{name}.json"'})


@app.get("/api/campaigns/{campaign_id}/report", tags=["reports"])
def campaign_report(campaign_id: str, format: Literal["json", "csv", "html"] = "json", conn=Depends(get_conn)) -> Response:
    ids = [r["run_id"] for r in conn.execute("SELECT run_id FROM runs WHERE campaign_id=? ORDER BY run_id", (campaign_id,))]
    if not ids:
        raise HTTPException(404, "Campaign not found")
    return report(",".join(ids), format, True, conn)


# ------------------------------------------------------------------ configuration
@app.get("/api/config/sensors", tags=["config"])
def get_sensors() -> dict:
    cfg = load_sensor_config()
    return {**cfg.model_dump(), "note": "SYNTHETIC demonstration settings - not engineering limits."}


@app.put("/api/config/sensors", tags=["config"])
def put_sensors(body: SensorConfig, conn=Depends(get_conn)) -> dict:
    save_override("sensors.json", body.model_dump())
    ids = [r["run_id"] for r in conn.execute("SELECT run_id FROM runs")]
    return {"saved": True, "reanalyzed": analyze_runs(conn, ids)}


@app.get("/api/config/settings", tags=["config"])
def get_settings() -> dict:
    return load_settings().model_dump()


@app.put("/api/config/settings", tags=["config"])
def put_settings(body: Settings, conn=Depends(get_conn)) -> dict:
    save_override("settings.json", body.model_dump())
    ids = [r["run_id"] for r in conn.execute("SELECT run_id FROM runs")]
    return {"saved": True, "reanalyzed": analyze_runs(conn, ids)}


@app.post("/api/config/reset", tags=["config"])
def reset_config(conn=Depends(get_conn)) -> dict:
    for name in ("sensors.json", "settings.json"):
        (data_dir() / "config" / name).unlink(missing_ok=True)
    ids = [r["run_id"] for r in conn.execute("SELECT run_id FROM runs")]
    return {"reset_to_defaults": True, "reanalyzed": analyze_runs(conn, ids)}


@app.get("/api/config/export", tags=["config"])
def export_config() -> JSONResponse:
    s, t = load_sensor_config(), load_settings()
    return JSONResponse({"config_version": config_fingerprint(s, t), "sensors": s.model_dump(), "settings": t.model_dump(),
                         "schema_version": SCHEMA_VERSION, "disclaimer": DISCLAIMER},
                        headers={"Content-Disposition": 'attachment; filename="orbitalpulse_config.json"'})


@app.get("/api/schema", tags=["config"])
def schema_info() -> dict:
    from orbitalpulse.schemas.events import TelemetryEvent
    return {"current": SCHEMA_VERSION, "supported": ["1.0"], "event_schema": TelemetryEvent.model_json_schema(),
            "severity_levels": list(SEVERITY_ORDER)}


if FRONTEND.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND), html=True), name="frontend")
