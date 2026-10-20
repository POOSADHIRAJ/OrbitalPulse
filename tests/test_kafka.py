"""Kafka tests. Broker-dependent tests are SKIPPED (not faked) when no broker is reachable.
Override the broker with ORBITALPULSE_TEST_KAFKA=host:port."""
import json
import os
import subprocess
import sys
import uuid

import pytest

from orbitalpulse.simulator.generator import DEMO_PLANS, generate_run
from orbitalpulse.streaming.status import broker_reachable

BOOTSTRAP = os.environ.get("ORBITALPULSE_TEST_KAFKA", "localhost:9092")
HAVE_BROKER = broker_reachable(BOOTSTRAP, 1.0)["reachable"]
needs_broker = pytest.mark.skipif(not HAVE_BROKER, reason=f"no Kafka broker reachable at {BOOTSTRAP}; Kafka integration NOT verified")


def run_module(mod, args, data_dir, timeout=120):
    env = {**os.environ, "ORBITALPULSE_DATA_DIR": str(data_dir)}
    return subprocess.run([sys.executable, "-W", "ignore", "-m", mod, *args], capture_output=True, text=True, env=env, timeout=timeout)


def test_producer_and_consumer_fail_cleanly_without_broker(tmp_path):
    dead = "127.0.0.1:1"
    p = run_module("orbitalpulse.ingestion.kafka_producer", ["--bootstrap", dead, "--runs", "RUN-001-CLEAN"], tmp_path)
    assert p.returncode == 2 and "not reachable" in p.stderr
    c = run_module("orbitalpulse.streaming.kafka_consumer", ["--bootstrap", dead], tmp_path)
    assert c.returncode == 2 and "not reachable" in c.stderr


def test_partition_key_keeps_sensor_ordering_together():
    from orbitalpulse.ingestion.kafka_producer import key_for
    a, b = generate_run(DEMO_PLANS[0])[:2]
    assert key_for({**a, "sensor_id": "x"}) == key_for({**b, "sensor_id": "x"})
    assert key_for({**a, "sensor_id": "x"}) != key_for({**a, "sensor_id": "y"})


@needs_broker
def test_end_to_end_stream_idempotent_and_replayable(tmp_path):
    from orbitalpulse.ingestion.kafka_producer import publish
    topic, group = f"orbitalpulse.test.{uuid.uuid4().hex[:8]}", f"g-{uuid.uuid4().hex[:6]}"
    records = generate_run(DEMO_PLANS[1])  # defect run: duplicates + malformed + gaps
    res = publish(records, bootstrap=BOOTSTRAP, topic=topic, rate=0, redeliver=0.05, log=lambda *_: None)
    assert res["errors"] == 0 and res["published"] == len(records) and res["redelivered_duplicates"] > 0

    out = run_module("orbitalpulse.streaming.kafka_consumer", ["--bootstrap", BOOTSTRAP, "--topic", topic, "--group", group, "--idle-exit", "4"], tmp_path)
    assert out.returncode == 0, out.stderr[-500:]
    summary = json.loads(out.stdout.strip().splitlines()[-1])
    sent = res["published"] + res["redelivered_duplicates"]
    assert summary["consumed"] == sent == summary["imported"] + summary["duplicates"] + summary["quarantined"]
    assert summary["quarantined"] == 8

    from orbitalpulse.storage.db import connect, init_schema
    os.environ["ORBITALPULSE_DATA_DIR"] = str(tmp_path)
    conn = connect(); init_schema(conn)
    stored = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    assert stored == summary["imported"] and conn.execute("SELECT COUNT(*) FROM findings").fetchone()[0] > 0
    hb = json.loads(conn.execute("SELECT details FROM service_status WHERE name='kafka_consumer'").fetchone()[0])
    assert hb["state"] == "STOPPED" and hb["consumer_lag"] in (0, None)

    # a brand-new consumer group replays the whole topic: storage must not change
    again = run_module("orbitalpulse.streaming.kafka_consumer", ["--bootstrap", BOOTSTRAP, "--topic", topic, "--group", group + "-replay", "--idle-exit", "4"], tmp_path)
    s2 = json.loads(again.stdout.strip().splitlines()[-1])
    assert s2["imported"] == 0 and s2["quarantined"] == 0
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == stored
    conn.close()


@needs_broker
def test_poison_message_is_quarantined_not_fatal(tmp_path):
    from kafka import KafkaProducer
    topic, group = f"orbitalpulse.test.{uuid.uuid4().hex[:8]}", f"g-{uuid.uuid4().hex[:6]}"
    prod = KafkaProducer(bootstrap_servers=BOOTSTRAP)
    good = generate_run(DEMO_PLANS[0])[0]
    for payload in (b"{not json", b"[1,2,3]", json.dumps(good).encode()):
        prod.send(topic, payload)
    prod.flush(); prod.close()
    out = run_module("orbitalpulse.streaming.kafka_consumer", ["--bootstrap", BOOTSTRAP, "--topic", topic, "--group", group, "--idle-exit", "4"], tmp_path)
    s = json.loads(out.stdout.strip().splitlines()[-1])
    assert (s["imported"], s["quarantined"]) == (1, 2)
