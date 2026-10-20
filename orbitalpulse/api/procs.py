"""Start/stop the Kafka producer/consumer as child processes of the API (optional convenience for the dashboard)."""
from __future__ import annotations

import subprocess
import sys
import time

from orbitalpulse.config import data_dir

_PROCS: dict[str, dict] = {}
MODULES = {"producer": "orbitalpulse.ingestion.kafka_producer", "consumer": "orbitalpulse.streaming.kafka_consumer"}


def start(name: str, args: list[str]) -> dict:
    cur = _PROCS.get(name)
    if cur and cur["proc"].poll() is None:
        return status(name)
    logs = data_dir() / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    log = (logs / f"{name}.log").open("ab")
    proc = subprocess.Popen([sys.executable, "-W", "ignore", "-m", MODULES[name], *args], stdout=log, stderr=subprocess.STDOUT)
    _PROCS[name] = {"proc": proc, "started": time.time(), "args": args, "log": str(logs / f"{name}.log")}
    return status(name)


def stop(name: str) -> dict:
    cur = _PROCS.get(name)
    if cur and cur["proc"].poll() is None:
        cur["proc"].terminate()
        try:
            cur["proc"].wait(timeout=10)
        except subprocess.TimeoutExpired:
            cur["proc"].kill()
    return status(name)


def status(name: str) -> dict:
    cur = _PROCS.get(name)
    if not cur:
        return {"running": False, "managed_by_api": False}
    code = cur["proc"].poll()
    tail = ""
    try:
        with open(cur["log"], "rb") as fh:
            fh.seek(max(0, fh.seek(0, 2) - 600))
            tail = fh.read().decode("utf-8", "replace")
    except OSError:
        pass
    return {"running": code is None, "exit_code": code, "managed_by_api": True, "pid": cur["proc"].pid,
            "args": cur["args"], "log_tail": tail}
