"""Append-only raw archive: every payload is written here BEFORE validation."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from orbitalpulse.config import data_dir


def archive_dir() -> Path:
    path = data_dir() / "raw"
    path.mkdir(parents=True, exist_ok=True)
    return path


class RawArchive:
    """One JSONL file per ingestion job. Opened in append mode; never rewritten."""

    def __init__(self, job_id: str):
        safe = "".join(c for c in job_id if c.isalnum() or c in "-_")
        self.path = archive_dir() / f"{safe}.jsonl"
        self._fh = self.path.open("a", encoding="utf-8", newline="\n")

    def write(self, source_ref: str, payload: Any) -> None:
        self._fh.write(json.dumps({"source_ref": source_ref, "payload": payload}, default=str) + "\n")

    def flush(self) -> None:
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


def lookup(archive_name: str | None, source_ref: str) -> Any | None:
    """Find one raw payload in a job's archive by source_ref. archive_name must be a bare file name we wrote."""
    if not archive_name or archive_name != Path(archive_name).name or not archive_name.endswith(".jsonl"):
        return None
    path = archive_dir() / archive_name
    if not path.is_file():
        return None
    needle = json.dumps(source_ref)
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if needle in line[:len(needle) + 40]:
                rec = json.loads(line)
                if rec.get("source_ref") == source_ref:
                    return rec.get("payload")
    return None
