"""Demo data: generate deterministic CSVs and import them through the real CSV pipeline."""
from __future__ import annotations

import shutil
import sqlite3

from orbitalpulse.config import data_dir
from orbitalpulse.ingestion.csv_ingest import IngestError, ingest_csv
from orbitalpulse.simulator.generator import CAMPAIGN_ID, DEMO_PLANS, generate_run, scenario_map, write_csv
from orbitalpulse.storage.archive import archive_dir
from orbitalpulse.storage.db import reset_all


def demo_dir():
    p = data_dir() / "demo"
    p.mkdir(parents=True, exist_ok=True)
    return p


def generate_demo_files(plans=None) -> list[str]:
    plans = plans or DEMO_PLANS
    paths = []
    for plan in plans:
        path = demo_dir() / f"{plan.run_id}.csv"
        write_csv(path, generate_run(plan))
        paths.append(str(path))
    return paths


def load_demo(conn: sqlite3.Connection, *, reset: bool = False) -> dict:
    if reset:
        reset_demo(conn)
    files = generate_demo_files()
    results, skipped = [], []
    for plan in DEMO_PLANS:
        path = demo_dir() / f"{plan.run_id}.csv"
        try:
            res = ingest_csv(conn, path.name, path.read_bytes(), default_campaign=CAMPAIGN_ID,
                             source_type="SYNTHETIC_DEMO")
            conn.execute("UPDATE runs SET scenario=? WHERE run_id=?", (scenario_map()[plan.run_id], plan.run_id))
            conn.commit()
            results.append(res)
        except IngestError as exc:
            if exc.status == 409:
                skipped.append(plan.run_id)
            else:
                raise
    return {"campaign_id": CAMPAIGN_ID, "files": files, "imported_runs": [r["runs"] for r in results],
            "already_loaded_runs": skipped, "jobs": [r["job_id"] for r in results]}


def reset_demo(conn: sqlite3.Connection) -> None:
    reset_all(conn)
    for sub in (demo_dir(), archive_dir()):
        shutil.rmtree(sub, ignore_errors=True)
