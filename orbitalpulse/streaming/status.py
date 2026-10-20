"""Honest service status: real TCP reachability check + consumer heartbeat age. Nothing is faked."""
from __future__ import annotations

import json
import socket
import sqlite3
from datetime import UTC, datetime

HEARTBEAT_STALE_S = 15.0


def broker_reachable(bootstrap: str, timeout: float = 1.0) -> dict:
    host, _, port = bootstrap.split(",")[0].rpartition(":")
    try:
        with socket.create_connection((host or "localhost", int(port or 9092)), timeout=timeout):
            return {"reachable": True, "bootstrap": bootstrap, "error": None}
    except (OSError, ValueError) as exc:
        return {"reachable": False, "bootstrap": bootstrap, "error": f"{type(exc).__name__}: {exc}"}


def write_status(conn: sqlite3.Connection, name: str, details: dict) -> None:
    conn.execute("INSERT INTO service_status(name, updated_at, details) VALUES (?,?,?) ON CONFLICT(name) DO UPDATE "
                 "SET updated_at=excluded.updated_at, details=excluded.details",
                 (name, datetime.now(UTC).isoformat(), json.dumps(details)))
    conn.commit()


def read_status(conn: sqlite3.Connection, name: str) -> dict | None:
    r = conn.execute("SELECT updated_at, details FROM service_status WHERE name=?", (name,)).fetchone()
    if not r:
        return None
    age = (datetime.now(UTC) - datetime.fromisoformat(r["updated_at"])).total_seconds()
    details = json.loads(r["details"])
    details.update({"updated_at": r["updated_at"], "age_s": round(age, 1),
                    "alive": details.get("state") in ("RUNNING", "IDLE") and age < HEARTBEAT_STALE_S})
    return details
