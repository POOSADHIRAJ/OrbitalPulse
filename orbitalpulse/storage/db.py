"""SQLite persistence: schema initialisation, connections and small helpers.

Schema strategy: ``init_schema`` is idempotent and records ``schema_meta.db_schema_version``.
Future changes append numbered migrations to MIGRATIONS (none needed yet).
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import UTC
from pathlib import Path
from typing import Iterator

from orbitalpulse.config import data_dir

DB_SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS ingestion_jobs (
  job_id TEXT PRIMARY KEY, source_type TEXT NOT NULL, source_name TEXT, file_sha256 TEXT,
  started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL,
  rows_total INTEGER DEFAULT 0, imported INTEGER DEFAULT 0, duplicates INTEGER DEFAULT 0,
  rejected INTEGER DEFAULT 0, quarantined INTEGER DEFAULT 0, detail TEXT,
  processing_version TEXT, raw_archive TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS ux_job_file ON ingestion_jobs(file_sha256)
  WHERE file_sha256 IS NOT NULL AND status IN ('COMPLETED','COMPLETED_WITH_ERRORS');
CREATE TABLE IF NOT EXISTS campaigns (
  campaign_id TEXT PRIMARY KEY, name TEXT, synthetic INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id),
  scenario TEXT, synthetic INTEGER NOT NULL DEFAULT 1, source_type TEXT, first_job_id TEXT,
  created_at TEXT NOT NULL, analyzed_at TEXT, config_version TEXT, metadata TEXT);
CREATE TABLE IF NOT EXISTS events (
  rowid_arrival INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE, payload_hash TEXT NOT NULL, campaign_id TEXT NOT NULL,
  run_id TEXT NOT NULL, sensor_id TEXT NOT NULL, event_ts TEXT NOT NULL, event_epoch REAL NOT NULL,
  ingest_ts TEXT, sequence_number INTEGER, value REAL, unit TEXT NOT NULL, phase TEXT NOT NULL,
  source_type TEXT, schema_version TEXT NOT NULL, quality_status TEXT NOT NULL,
  job_id TEXT NOT NULL, processing_version TEXT);
CREATE INDEX IF NOT EXISTS ix_events_run_sensor ON events(run_id, sensor_id, event_epoch);
CREATE TABLE IF NOT EXISTS quarantine (
  quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, run_id TEXT, sensor_id TEXT,
  event_id TEXT, rule_id TEXT NOT NULL, severity TEXT NOT NULL, observed TEXT, expected TEXT,
  explanation TEXT, raw_payload TEXT NOT NULL, source_ref TEXT, created_at TEXT NOT NULL,
  replayed INTEGER NOT NULL DEFAULT 0, dedupe_key TEXT UNIQUE);
CREATE INDEX IF NOT EXISTS ix_quar_run ON quarantine(run_id, rule_id);
CREATE TABLE IF NOT EXISTS quality_issues (
  issue_id TEXT PRIMARY KEY, scope TEXT NOT NULL, campaign_id TEXT, run_id TEXT, event_id TEXT,
  sensor_id TEXT, rule_id TEXT NOT NULL, severity TEXT NOT NULL, observed TEXT, expected TEXT,
  explanation TEXT NOT NULL, detected_at TEXT NOT NULL, source_ref TEXT, job_id TEXT,
  window_start TEXT, window_end TEXT, count INTEGER DEFAULT 1);
CREATE INDEX IF NOT EXISTS ix_issue_run ON quality_issues(run_id, rule_id, severity);
CREATE TABLE IF NOT EXISTS findings (
  finding_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, sensor_id TEXT, rule_id TEXT NOT NULL,
  rule_name TEXT NOT NULL, category TEXT NOT NULL, severity TEXT NOT NULL,
  window_start TEXT, window_end TEXT, evidence TEXT NOT NULL, threshold TEXT NOT NULL,
  explanation TEXT NOT NULL, limitations TEXT NOT NULL, config_version TEXT, processing_version TEXT,
  detected_at TEXT NOT NULL, review_status TEXT NOT NULL DEFAULT 'OPEN', reviewed_at TEXT,
  reviewer TEXT, review_note TEXT);
CREATE INDEX IF NOT EXISTS ix_find_run ON findings(run_id, rule_id);
CREATE TABLE IF NOT EXISTS service_status (
  name TEXT PRIMARY KEY, updated_at TEXT NOT NULL, details TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS activity (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, kind TEXT NOT NULL, message TEXT NOT NULL);
"""


def db_path() -> Path:
    return data_dir() / "orbitalpulse.db"


def connect(path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path or db_path()), timeout=30, check_same_thread=False)  # one connection per request, used sequentially
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.execute("INSERT OR IGNORE INTO schema_meta VALUES ('db_schema_version', ?)", (str(DB_SCHEMA_VERSION),))
    conn.commit()


@contextmanager
def session(path: Path | None = None) -> Iterator[sqlite3.Connection]:
    conn = connect(path)
    try:
        init_schema(conn)
        yield conn
    finally:
        conn.close()


def log_activity(conn: sqlite3.Connection, kind: str, message: str) -> None:
    from datetime import datetime
    conn.execute("INSERT INTO activity(ts, kind, message) VALUES (?,?,?)",
                 (datetime.now(UTC).isoformat(), kind, message))


def reset_all(conn: sqlite3.Connection) -> None:
    """Delete all runtime data (keeps schema and config). Raw archive files are kept unless removed by caller."""
    for table in ("findings", "quality_issues", "quarantine", "events", "runs", "campaigns",
                  "ingestion_jobs", "activity"):
        conn.execute(f"DELETE FROM {table}")  # fixed table names, no user input
    conn.commit()
