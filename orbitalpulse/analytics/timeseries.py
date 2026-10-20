"""Time-series access with bounded output (min/max bucket downsampling). Stored timestamps are never altered;
`align=elapsed` only adds an elapsed-seconds x value for visual overlay."""
from __future__ import annotations

import sqlite3
from typing import Any


def downsample(points: list[tuple], max_points: int) -> tuple[list[tuple], bool]:
    """Keep the min and max sample of each bucket so spikes survive downsampling. points: (x, y, ...)"""
    if len(points) <= max_points:
        return points, False
    buckets = max(1, max_points // 2)
    size = len(points) / buckets
    out: list[tuple] = []
    for b in range(buckets):
        chunk = points[int(b * size):int((b + 1) * size)]
        valued = [p for p in chunk if p[1] is not None]
        if not valued:
            out.extend(chunk[:1])
            continue
        lo, hi = min(valued, key=lambda p: p[1]), max(valued, key=lambda p: p[1])
        out.extend(sorted({id(lo): lo, id(hi): hi}.values(), key=lambda p: p[0]))
    if out[0] is not points[0]:
        out.insert(0, points[0])      # always keep the first and last sample
    if out[-1] is not points[-1]:
        out.append(points[-1])
    return out, True


def run_bounds(conn: sqlite3.Connection, run_id: str) -> tuple[float, float] | None:
    r = conn.execute("SELECT MIN(event_epoch) a, MAX(event_epoch) b FROM events WHERE run_id=?", (run_id,)).fetchone()
    return (r["a"], r["b"]) if r and r["a"] is not None else None


def timeseries(conn: sqlite3.Connection, run_id: str, sensor_id: str, *, max_points: int, phase: str | None = None,
               start: float | None = None, end: float | None = None, align: str = "absolute") -> dict[str, Any]:
    where: list[str] = ["run_id=?", "sensor_id=?"]
    args: list[Any] = [run_id, sensor_id]
    if phase:
        where.append("phase=?"); args.append(phase)
    if start is not None:
        where.append("event_epoch>=?"); args.append(start)
    if end is not None:
        where.append("event_epoch<=?"); args.append(end)
    rows = conn.execute(f"SELECT event_epoch, value, event_ts, event_id, phase, quality_status FROM events WHERE "
                        f"{' AND '.join(where)} ORDER BY event_epoch, rowid_arrival", args).fetchall()
    bounds = run_bounds(conn, run_id)
    origin = bounds[0] if (align == "elapsed" and bounds) else 0.0
    pts = [(r["event_epoch"] - origin, r["value"], r["event_ts"], r["event_id"], r["phase"]) for r in rows]
    pts, reduced = downsample(pts, max_points)
    return {"run_id": run_id, "sensor_id": sensor_id, "align": align, "origin_epoch": origin if align == "elapsed" else None,
            "total_points": len(rows), "returned_points": len(pts), "downsampled": reduced,
            "x": [p[0] for p in pts], "y": [p[1] for p in pts], "timestamps": [p[2] for p in pts],
            "event_ids": [p[3] for p in pts], "phases": [p[4] for p in pts]}
