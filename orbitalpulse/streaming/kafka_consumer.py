"""Kafka consumer -> validated, idempotent SQLite storage.

Delivery semantics (honest version): AT-LEAST-ONCE delivery with IDEMPOTENT storage. For each batch we
1) write to SQLite and COMMIT, then 2) commit Kafka offsets. A crash between the two replays the batch; duplicate
event_ids are ignored by the storage layer, so the stored result is the same as exactly-once *effect*. We do NOT claim
Kafka-level exactly-once. On a processing failure the batch is not committed and the consumer seeks back and retries.

Usage:  python -m orbitalpulse.streaming.kafka_consumer [--idle-exit 10]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import UTC, datetime

from orbitalpulse.config import load_sensor_config, load_settings
from orbitalpulse.ingestion.core import IngestCounts, finish_job, ingest_records, start_job
from orbitalpulse.pipeline import analyze_runs
from orbitalpulse.storage.db import session
from orbitalpulse.streaming.status import write_status

log = logging.getLogger("orbitalpulse.consumer")


def decode(message):
    try:
        value = json.loads(message.value.decode("utf-8"))
        return value if isinstance(value, dict) else {"_undecodable": repr(value)[:200]}
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return {"_undecodable": f"{type(exc).__name__}: {message.value[:120]!r}"}


def run(bootstrap: str, topic: str, group: str, batch_size: int, idle_exit: float | None, analyze_every: float = 3.0,
        from_beginning: bool = True) -> int:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError

    from orbitalpulse.streaming.status import broker_reachable
    reach = broker_reachable(bootstrap)
    if not reach["reachable"]:
        print(f"ERROR: Kafka broker not reachable at {bootstrap} ({reach['error']}).", file=sys.stderr)
        return 2

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    try:
        signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    except (ValueError, AttributeError):
        pass

    try:
        consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap, group_id=group, enable_auto_commit=False,
                                 auto_offset_reset="earliest" if from_beginning else "latest",
                                 max_poll_records=batch_size, consumer_timeout_ms=1000)
    except KafkaError as exc:
        print(f"ERROR: could not create Kafka consumer for {bootstrap}: {exc}", file=sys.stderr)
        return 2

    sensors, settings = load_sensor_config(), load_settings()
    counts, dirty = IngestCounts(), set()
    last_analyze = last_data = time.time()
    consumed = 0
    started = datetime.now(UTC).isoformat()
    with session() as conn:
        job_id, archive = start_job(conn, "KAFKA", f"{topic}@{bootstrap}")

        def heartbeat(state: str) -> None:
            lag = None
            try:
                parts = consumer.assignment()
                if parts:
                    ends = consumer.end_offsets(list(parts))
                    lag = sum(max(0, ends[tp] - consumer.position(tp)) for tp in parts)
            except Exception as exc:  # lag is optional; never fake it
                log.debug("lag unavailable: %s", exc)
            write_status(conn, "kafka_consumer", {"state": state, "pid": os.getpid(), "started_at": started, "topic": topic,
                         "group": group, "job_id": job_id, "consumed": consumed, "counts": counts.as_dict(),
                         "consumer_lag": lag, "lag_note": "sum of (end offset - position) over assigned partitions"})

        heartbeat("RUNNING")
        log.info("consuming %s from %s as group %s", topic, bootstrap, group)
        try:
            while not stop["flag"]:
                polled = consumer.poll(timeout_ms=1000, max_records=batch_size)
                msgs = [m for batch in polled.values() for m in batch]
                if msgs:
                    last_data = time.time()
                    first_offsets: dict[tuple[str, int], int] = {}
                    for m in msgs:
                        first_offsets.setdefault((m.topic, m.partition), m.offset)
                    try:
                        records = ((f"kafka:{m.topic}:{m.partition}:{m.offset}", decode(m)) for m in msgs)
                        ingest_records(conn, records, job_id=job_id, archive=archive, sensors=sensors, settings=settings,
                                       counts=counts, commit=False)
                        conn.commit()          # 1) durable in SQLite
                        archive.flush()
                        consumer.commit()      # 2) then commit offsets
                        consumed += len(msgs)
                        dirty |= counts.runs
                    except Exception as exc:
                        conn.rollback()
                        log.error("batch failed (%s); offsets NOT committed, retrying", exc)
                        from kafka import TopicPartition
                        for (t, p), off in first_offsets.items():
                            consumer.seek(TopicPartition(t, p), off)
                        time.sleep(2)
                        continue
                now = time.time()
                if dirty and (now - last_analyze >= analyze_every or not msgs):
                    analyze_runs(conn, dirty)
                    dirty.clear()
                    last_analyze = now
                heartbeat("RUNNING" if msgs else "IDLE")
                if idle_exit is not None and consumed > 0 and now - last_data > idle_exit:
                    break
        finally:
            if dirty:
                analyze_runs(conn, dirty)
            finish_job(conn, job_id, counts, archive, status="COMPLETED_WITH_ERRORS" if counts.quarantined else "COMPLETED")
            heartbeat("STOPPED")
            consumer.close()
    print(json.dumps({"consumed": consumed, **counts.as_dict()}))
    return 0


def main(argv=None) -> int:
    k = load_settings().kafka
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default=k["bootstrap_servers"])
    p.add_argument("--topic", default=k["topic_raw"])
    p.add_argument("--group", default=k["consumer_group"])
    p.add_argument("--batch-size", type=int, default=int(k["batch_size"]))
    p.add_argument("--idle-exit", type=float, default=None, help="exit after N seconds without messages (once data was seen)")
    p.add_argument("--latest", action="store_true", help="start at the end of the topic for a NEW group")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return run(a.bootstrap, a.topic, a.group, a.batch_size, a.idle_exit, from_beginning=not a.latest)


if __name__ == "__main__":
    raise SystemExit(main())
