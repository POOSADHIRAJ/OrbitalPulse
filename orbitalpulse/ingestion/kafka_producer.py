"""Publish synthetic (or CSV) telemetry to Kafka.

Partition key = "<test_run_id>|<sensor_id>": all events of one sensor in one run go to ONE partition, so per-sensor
ordering (needed for sequence-gap / interval checks) is preserved while different sensors spread across partitions.
Cross-sensor ordering is NOT guaranteed; cross-sensor checks therefore use event timestamps, never arrival order.

Usage:  python -m orbitalpulse.ingestion.kafka_producer --runs RUN-001-CLEAN RUN-002-DQ-DEFECTS --rate 1000
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time

from orbitalpulse.config import load_settings
from orbitalpulse.simulator.generator import DEMO_PLANS, generate_run


def key_for(record: dict) -> bytes:
    return f"{record.get('test_run_id')}|{record.get('sensor_id')}".encode()


def publish(records, *, bootstrap: str, topic: str, rate: float = 0.0, redeliver: float = 0.0, seed: int = 7,
            log=print) -> dict:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    from orbitalpulse.streaming.status import broker_reachable
    reach = broker_reachable(bootstrap)
    if not reach["reachable"]:
        raise ConnectionError(f"Kafka broker not reachable at {bootstrap} ({reach['error']}). "
                              "Start it first (scripts\\kafka_start.ps1).")
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=5, linger_ms=20,
                                 value_serializer=lambda v: json.dumps(v).encode("utf-8"), key_serializer=lambda k: k)
    except KafkaError as exc:
        raise ConnectionError(f"Could not create Kafka producer for {bootstrap}: {exc}") from exc
    rng = random.Random(seed)
    sent = redelivered = errors = 0
    start = time.time()

    def on_err(exc):  # noqa: ANN001
        nonlocal errors
        errors += 1
        log(f"send failed: {exc}")

    try:
        for rec in records:
            producer.send(topic, key=key_for(rec), value=rec).add_errback(on_err)
            sent += 1
            if redeliver and rng.random() < redeliver:  # simulate at-least-once duplicate delivery
                producer.send(topic, key=key_for(rec), value=rec).add_errback(on_err)
                redelivered += 1
            if rate and sent % max(1, int(rate // 10) or 1) == 0:
                time.sleep(max(0.0, sent / rate - (time.time() - start)))
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=10)
    return {"published": sent, "redelivered_duplicates": redelivered, "errors": errors, "topic": topic,
            "seconds": round(time.time() - start, 2)}


def main(argv=None) -> int:
    kafka = load_settings().kafka
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default=kafka["bootstrap_servers"])
    p.add_argument("--topic", default=kafka["topic_raw"])
    p.add_argument("--runs", nargs="*", help="Demo run IDs (default: all demo runs)")
    p.add_argument("--csv", help="Publish rows of a CSV file instead of simulator output")
    p.add_argument("--rate", type=float, default=500.0, help="events/second (0 = unthrottled)")
    p.add_argument("--redeliver", type=float, default=0.0, help="fraction of events to deliver twice (idempotency demo)")
    a = p.parse_args(argv)

    if a.csv:
        with open(a.csv, newline="", encoding="utf-8-sig") as fh:
            records = list(csv.DictReader(fh))
    else:
        plans = [pl for pl in DEMO_PLANS if not a.runs or pl.run_id in a.runs]
        if not plans:
            print("No matching demo runs.", file=sys.stderr)
            return 1
        records = [r for pl in plans for r in generate_run(pl)]
    try:
        result = publish(records, bootstrap=a.bootstrap, topic=a.topic, rate=a.rate, redeliver=a.redeliver)
    except ConnectionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result))
    return 0 if result["errors"] == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
