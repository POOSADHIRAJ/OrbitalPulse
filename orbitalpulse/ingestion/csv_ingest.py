"""Batch CSV ingestion (long format: one measurement per row) with configurable column mapping."""
from __future__ import annotations

import csv
import hashlib
import io
import sqlite3
from typing import Any

from orbitalpulse.config import load_settings
from orbitalpulse.ingestion.core import IngestCounts, _add_issue, _quarantine, finish_job, ingest_records, start_job
from orbitalpulse.pipeline import analyze_runs
from orbitalpulse.schemas.events import ALL_FIELDS
from orbitalpulse.schemas.validation import Rejection

REQUIRED_INTERNAL = ("test_run_id", "sensor_id", "event_timestamp", "sensor_value")
ALIASES = {
    "event_timestamp": ["timestamp", "time", "ts", "datetime", "event_time"],
    "test_run_id": ["run_id", "run", "test_run", "testrun"],
    "sensor_id": ["sensor", "channel", "channel_id", "sensor_name"],
    "sensor_value": ["value", "reading", "measurement", "val"],
    "sensor_unit": ["unit", "units"],
    "test_phase": ["phase"],
    "sequence_number": ["seq", "sequence", "sample_no"],
    "campaign_id": ["campaign"],
    "event_id": ["id", "uuid"],
}


class IngestError(Exception):
    """User-correctable ingestion problem (maps to HTTP 4xx)."""
    def __init__(self, message: str, status: int = 422, extra: dict | None = None):
        super().__init__(message)
        self.status, self.extra = status, extra or {}


def _decode(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise IngestError(f"File is not valid UTF-8 text: {exc}", 415) from exc


def check_upload(filename: str, content: bytes) -> None:
    limit = load_settings().limits["max_upload_mb"] * 1024 * 1024
    if not filename.lower().endswith(".csv"):
        raise IngestError("Only .csv files are accepted.", 415)
    if len(content) > limit:
        raise IngestError(f"File exceeds the {limit // (1024 * 1024)} MB upload limit.", 413)
    if not content.strip():
        raise IngestError("File is empty.", 422)


def suggest_mapping(columns: list[str]) -> dict[str, str]:
    lower = {c.strip().lower(): c for c in columns}
    mapping: dict[str, str] = {}
    for internal in ALL_FIELDS:
        if internal in lower:
            mapping[internal] = lower[internal]
            continue
        for alias in ALIASES.get(internal, []):
            if alias in lower:
                mapping[internal] = lower[alias]
                break
    return mapping


def preview_csv(filename: str, content: bytes, mapping: dict[str, str] | None = None, n: int = 10) -> dict[str, Any]:
    check_upload(filename, content)
    text = _decode(content)
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise IngestError("File has no header row.") from None
    header = [h.strip() for h in header]
    rows: list[dict] = []
    total = 0
    for row in reader:
        if not row:
            continue
        total += 1
        if len(rows) < n:
            rows.append(dict(zip(header, row)) if len(row) == len(header) else {"_malformed_row": row})
    mapping = mapping or suggest_mapping(header)
    missing = [f for f in REQUIRED_INTERNAL if f not in mapping]
    unknown = [v for v in mapping.values() if v not in header]
    return {"filename": filename, "columns": header, "row_count": total, "suggested_mapping": suggest_mapping(header),
            "applied_mapping": mapping, "missing_required_fields": missing, "mapping_unknown_columns": unknown,
            "extra_columns": [c for c in header if c not in mapping.values()], "sample_rows": rows,
            "sha256": hashlib.sha256(content).hexdigest(), "ready": not missing and not unknown and total > 0}


def ingest_csv(conn: sqlite3.Connection, filename: str, content: bytes, *, mapping: dict[str, str] | None = None,
               default_campaign: str = "UNASSIGNED", default_run_id: str | None = None,
               source_type: str = "CSV_UPLOAD", analyze: bool = True) -> dict[str, Any]:
    check_upload(filename, content)
    digest = hashlib.sha256(content).hexdigest()
    prev = conn.execute("SELECT job_id FROM ingestion_jobs WHERE file_sha256=? AND status IN "
                        "('COMPLETED','COMPLETED_WITH_ERRORS')", (digest,)).fetchone()
    if prev:
        raise IngestError("This exact file was already imported; refusing to import it twice.", 409,
                          {"existing_job_id": prev["job_id"]})
    text = _decode(content)
    reader = csv.reader(io.StringIO(text))
    try:
        header = [h.strip() for h in next(reader)]
    except StopIteration:
        raise IngestError("File has no header row.") from None
    mapping = mapping or suggest_mapping(header)
    bad_cols = [v for v in mapping.values() if v not in header]
    if bad_cols:
        raise IngestError(f"Mapping refers to columns not in the file: {bad_cols}", 422, {"columns": header})
    missing = [f for f in REQUIRED_INTERNAL if f not in mapping and not (f == "test_run_id" and default_run_id)]
    if missing:
        raise IngestError(f"Missing required column(s) after mapping: {missing}", 422,
                          {"columns": header, "suggested_mapping": suggest_mapping(header)})

    job_id, archive = start_job(conn, source_type, filename, digest)
    counts = IngestCounts()
    index = {c: i for i, c in enumerate(header)}

    def rows():
        for line_no, row in enumerate(reader, start=2):
            if not row:
                continue
            ref = f"{filename}:{line_no}"
            if len(row) != len(header):
                counts.total += 1
                archive.write(ref, {"_malformed_row": row})
                rej = Rejection("DQ_MALFORMED_ROW", "ERROR", f"{len(row)} fields", f"{len(header)} fields",
                                "CSV row has a different number of fields than the header.")
                fresh = _quarantine(conn, job_id, {"_malformed_row": row, "_line": line_no}, rej, ref)
                counts.quarantined += 1 if fresh else 0
                counts.duplicates += 0 if fresh else 1
                _add_issue(conn, scope="EVENT", campaign_id=None, run_id=default_run_id, event_id=None, sensor_id=None,
                           rej=rej, source_ref=ref, job_id=job_id)
                continue
            raw = dict(zip(header, row))
            for internal, external in mapping.items():
                raw[internal] = row[index[external]]
            if default_run_id and not raw.get("test_run_id"):
                raw["test_run_id"] = default_run_id
            if default_campaign and not raw.get("campaign_id"):
                raw["campaign_id"] = default_campaign
            yield ref, raw

    try:
        ingest_records(conn, rows(), job_id=job_id, archive=archive, counts=counts, default_campaign=default_campaign)
        if counts.total == 0:
            finish_job(conn, job_id, counts, archive, status="FAILED", detail="no data rows")
            raise IngestError("File contains a header but no data rows.", 422, {"job_id": job_id})
        finish_job(conn, job_id, counts, archive)
    except IngestError:
        raise
    except Exception as exc:  # interrupted/failed import is recorded, not hidden
        conn.rollback()
        finish_job(conn, job_id, counts, archive, status="FAILED", detail=f"{type(exc).__name__}: {exc}")
        raise
    analysis = analyze_runs(conn, counts.runs) if analyze else []
    return {"job_id": job_id, "source_name": filename, "sha256": digest, "counts": counts.as_dict(),
            "rows_total": counts.total, "imported": counts.imported, "duplicates": counts.duplicates,
            "quarantined": counts.quarantined, "runs": sorted(counts.runs), "analysis": analysis}
