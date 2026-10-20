"""Create a local single-node KRaft broker config under data/kafka and format its storage (idempotent).

Prints the properties-file path (last line of stdout) so start scripts can launch the broker.
Kafka location: $KAFKA_HOME, else ./kafka_2.13-4.1.2, else C:\\k\\kafka. Does not download anything.
"""
from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def find_kafka() -> Path:
    candidates = [os.environ.get("KAFKA_HOME"), ROOT / "kafka_2.13-4.1.2", r"C:\k\kafka"]
    for c in candidates:
        if c and (Path(c) / "libs").is_dir():
            return Path(c)
    raise SystemExit("Kafka not found. Set KAFKA_HOME to your Kafka folder (e.g. C:\\k\\kafka).")


def main() -> None:
    kafka = find_kafka()
    base = Path(os.environ.get("ORBITALPULSE_DATA_DIR", ROOT / "data")) / "kafka"
    logs = base / "logs"
    base.mkdir(parents=True, exist_ok=True)
    props = base / "server.properties"
    posix = lambda p: str(p).replace("\\", "/")  # noqa: E731  (Kafka .properties treat backslash as escape)
    props.write_text("\n".join([
        "process.roles=broker,controller", "node.id=1",
        "controller.quorum.bootstrap.servers=localhost:9093",
        "listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093",
        "advertised.listeners=PLAINTEXT://127.0.0.1:9092", "inter.broker.listener.name=PLAINTEXT",
        "controller.listener.names=CONTROLLER",
        "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        f"log.dirs={posix(logs)}", "num.partitions=3", "offsets.topic.replication.factor=1",
        "transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1",
        "share.coordinator.state.topic.replication.factor=1", "share.coordinator.state.topic.min.isr=1",
        "group.initial.rebalance.delay.ms=0", "auto.create.topics.enable=true", "log.retention.hours=24", ""]), encoding="utf-8")
    if not (logs / "meta.properties").is_file():
        import base64
        win = os.name == "nt"
        if not win:  # archives made on Windows lose the executable bit
            for sh in (kafka / "bin").glob("*.sh"):
                sh.chmod(sh.stat().st_mode | 0o111)
        tool = kafka / "bin" / ("windows" if win else "") / ("kafka-storage.bat" if win else "kafka-storage.sh")
        cluster = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")  # 22-char Kafka cluster id
        subprocess.run([str(tool), "format", "--standalone", "-t", cluster, "-c", str(props)], check=True)
    print(f"KAFKA_HOME={kafka}", file=sys.stderr)
    print(str(props))


if __name__ == "__main__":
    main()
