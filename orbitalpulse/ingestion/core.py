"""Shared ingestion core used by CSV upload, demo loading and the Kafka consumer.

Guarantees: raw payload archived first; invalid records quarantined with reason; duplicate
event_ids never overwrite (identical payload = duplicate delivery, different payload = conflict
-> quarantine). Writes are idempotent, so at-least-once delivery is safe.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Iterable

from orbitalpulse import PROCESSING_VERSION
from orbitalpulse.config import SensorConfig, Settings, load_sensor_config, load_settings
from orbitalpulse.schemas.validation import Rejection, payload_hash, validate_event
from orbitalpulse.storage.archive import RawArchive
from orbitalpulse.storage.db import log_activity


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class IngestCounts:
    total: int = 0
    imported: int = 0
    duplicates: int = 0
    quarantined: int = 0
    warnings: int = 0
    runs: set[str] = field(default_factory=set)

    def as_dict(self) -> dict:
        return {"total": self.total, "imported": self.imported, "duplicates": self.duplicates,
                "quarantined": self.quarantined, "warnings": self.warnings, "runs": sorted(self.runs)}


def start_job(conn: sqlite3.Connection, source_type: str, source_name: str | None,
              file_sha256: str | None = None, job_id: str | None = None) -> tuple[str, RawArchive]:
    job_id = job_id or f"job-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    conn.execute(
        "INSERT INTO ingestion_jobs(job_id, source_type, source_name, file_sha256, started_at, status,"
        " processing_version) VALUES (?,?,?,?,?,?,?)",
        (job_id, source_type, source_name, file_sha256, now_iso(), "RUNNING", PROCESSING_VERSION))
    conn.commit()
    return job_id, RawArchive(job_id)


def finish_job(conn: sqlite3.Connection, job_id: str, counts: IngestCounts, archive: RawArchive | None,
               status: str | None = None, detail: str | None = None) -> None:
    status = status or ("COMPLETED_WITH_ERRORS" if counts.quarantined else "COMPLETED")
    conn.execute(
        "UPDATE ingestion_jobs SET finished_at=?, status=?, rows_total=?, imported=?, duplicates=?,"
        " rejected=?, quarantined=?, detail=?, raw_archive=? WHERE job_id=?",
        (now_iso(), status, counts.total, counts.imported, counts.duplicates, counts.quarantined,
         counts.quarantined, detail, archive.path.name if archive else None, job_id))
    log_activity(conn, "INGEST", f"{job_id}: {status} imported={counts.imported} "
                 f"duplicates={counts.duplicates} quarantined={counts.quarantined}")
    conn.commit()
    if archive:
        archive.close()


def _issue_id(*parts: Any) -> str:
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:24]


def _add_issue(conn, *, scope, campaign_id, run_id, event_id, sensor_id, rej: Rejection, source_ref, job_id):
    conn.execute(
        "INSERT OR IGNORE INTO quality_issues(issue_id, scope, campaign_id, run_id, event_id, sensor_id,"
        " rule_id, severity, observed, expected, explanation, detected_at, source_ref, job_id)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (_issue_id(rej.rule_id, run_id, event_id, sensor_id, source_ref), scope, campaign_id, run_id,
         event_id, sensor_id, rej.rule_id, rej.severity, rej.observed, rej.expected, rej.explanation,
         now_iso(), source_ref, job_id))


def _quarantine(conn, job_id, raw, rej: Rejection, source_ref, event_id=None, run_id=None, sensor_id=None):
    if isinstance(raw, dict):
        run_id = run_id or (str(raw.get("test_run_id")).strip() if raw.get("test_run_id") else None)
        sensor_id = sensor_id or (str(raw.get("sensor_id")).strip() if raw.get("sensor_id") else None)
        event_id = event_id or (str(raw.get("event_id")).strip() if raw.get("event_id") else None)
    payload = json.dumps(raw, default=str)
    key = hashlib.sha1(f"{source_ref}|{rej.rule_id}|{payload}".encode()).hexdigest()
    cur = conn.execute(
        "INSERT OR IGNORE INTO quarantine(job_id, run_id, sensor_id, event_id, rule_id, severity, observed, expected,"
        " explanation, raw_payload, source_ref, created_at, dedupe_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (job_id, run_id, sensor_id, event_id, rej.rule_id, rej.severity, rej.observed, rej.expected,
         rej.explanation, payload, source_ref, now_iso(), key))
    return cur.rowcount == 1  # False => this exact rejected payload was already quarantined (replay)


def _ensure_run(conn, cache: set, campaign_id, run_id, source_type, job_id, scenario=None, synthetic=True):
    if run_id in cache:
        return
    conn.execute("INSERT OR IGNORE INTO campaigns(campaign_id, name, synthetic, created_at) VALUES (?,?,?,?)",
                 (campaign_id, campaign_id, int(synthetic), now_iso()))
    conn.execute("INSERT OR IGNORE INTO runs(run_id, campaign_id, scenario, synthetic, source_type,"
                 " first_job_id, created_at) VALUES (?,?,?,?,?,?,?)",
                 (run_id, campaign_id, scenario, int(synthetic), source_type, job_id, now_iso()))
    cache.add(run_id)


def ingest_records(
    conn: sqlite3.Connection,
    records: Iterable[tuple[str, Any]],
    *,
    job_id: str,
    archive: RawArchive | None,
    sensors: SensorConfig | None = None,
    settings: Settings | None = None,
    default_campaign: str = "UNASSIGNED",
    counts: IngestCounts | None = None,
    run_scenarios: dict[str, str] | None = None,
    commit_every: int = 2000,
    commit: bool = True,
) -> IngestCounts:
    """Ingest an iterable of (source_ref, raw_record). Returns cumulative counts.

    ``commit=False`` lets the Kafka consumer commit DB work and Kafka offsets in a controlled order.
    """
    sensors = sensors or load_sensor_config()
    settings = settings or load_settings()
    sensor_map = sensors.by_id()
    counts = counts or IngestCounts()
    run_cache: set[str] = set()
    late = settings.quality.get("late_event_seconds")
    pending = 0

    for source_ref, raw in records:
        counts.total += 1
        if archive:
            archive.write(source_ref, raw)
        result = validate_event(raw, sensor_map, default_campaign=default_campaign, late_event_seconds=late)

        if result.rejection:
            fresh = _quarantine(conn, job_id, raw, result.rejection, source_ref)
            rid = str(raw.get("test_run_id")).strip() if isinstance(raw, dict) and raw.get("test_run_id") else None
            _add_issue(conn, scope="EVENT", campaign_id=None, run_id=rid,
                       event_id=None, sensor_id=raw.get("sensor_id") if isinstance(raw, dict) else None,
                       rej=result.rejection, source_ref=source_ref, job_id=job_id)
            counts.quarantined += 1 if fresh else 0
            counts.duplicates += 0 if fresh else 1
        else:
            ev = result.event
            assert ev is not None
            _ensure_run(conn, run_cache, ev.campaign_id, ev.test_run_id, ev.source_type, job_id,
                        (run_scenarios or {}).get(ev.test_run_id), ev.source_type == "SYNTHETIC")
            phash = payload_hash(ev)
            cur = conn.execute(
                "INSERT OR IGNORE INTO events(event_id, payload_hash, campaign_id, run_id, sensor_id, event_ts,"
                " event_epoch, ingest_ts, sequence_number, value, unit, phase, source_type, schema_version,"
                " quality_status, job_id, processing_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ev.event_id, phash, ev.campaign_id, ev.test_run_id, ev.sensor_id, ev.event_timestamp.isoformat(),
                 ev.event_timestamp.timestamp(), ev.ingestion_timestamp.isoformat() if ev.ingestion_timestamp else None,
                 ev.sequence_number, ev.sensor_value, ev.sensor_unit, ev.test_phase, ev.source_type,
                 ev.schema_version, ev.quality_status, job_id, PROCESSING_VERSION))
            counts.runs.add(ev.test_run_id)
            if cur.rowcount == 1:
                counts.imported += 1
                for w in result.warnings:
                    _add_issue(conn, scope="EVENT", campaign_id=ev.campaign_id, run_id=ev.test_run_id,
                               event_id=ev.event_id, sensor_id=ev.sensor_id, rej=w, source_ref=source_ref, job_id=job_id)
                    counts.warnings += 1
            else:
                existing = conn.execute("SELECT payload_hash FROM events WHERE event_id=?", (ev.event_id,)).fetchone()
                if existing and existing["payload_hash"] == phash:
                    counts.duplicates += 1
                    _add_issue(conn, scope="EVENT", campaign_id=ev.campaign_id, run_id=ev.test_run_id,
                               event_id=ev.event_id, sensor_id=ev.sensor_id, source_ref=source_ref, job_id=job_id,
                               rej=Rejection("DQ_DUPLICATE_DELIVERY", "INFO", "same event_id, identical payload",
                                             "each event_id delivered once",
                                             "Event was delivered again; ignored idempotently (first copy kept)."))
                else:
                    rej = Rejection("DQ_DUPLICATE_EVENT_ID_CONFLICT", "ERROR", "same event_id, different payload",
                                    "unique event_id per measurement",
                                    "event_id already exists with different content; original NOT overwritten.")
                    fresh = _quarantine(conn, job_id, raw, rej, source_ref, event_id=ev.event_id,
                                        run_id=ev.test_run_id, sensor_id=ev.sensor_id)
                    _add_issue(conn, scope="EVENT", campaign_id=ev.campaign_id, run_id=ev.test_run_id,
                               event_id=ev.event_id, sensor_id=ev.sensor_id, rej=rej, source_ref=source_ref, job_id=job_id)
                    counts.quarantined += 1 if fresh else 0
                    counts.duplicates += 0 if fresh else 1
        pending += 1
        if commit and pending >= commit_every:
            conn.commit()
            pending = 0
    if commit:
        conn.commit()
    return counts
