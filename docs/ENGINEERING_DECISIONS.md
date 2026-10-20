# Engineering decisions

| Area | Chosen | Alternatives considered | Why |
|---|---|---|---|
| Storage | SQLite (WAL) + JSONL raw archive | Postgres, DuckDB/Parquet | Zero setup on Windows, transactional, easy to inspect; raw archive keeps evidence independent of the DB. |
| API | FastAPI + pydantic | Flask | Validation, OpenAPI `/docs` for free. |
| Frontend | Vanilla JS + hand-written SVG charts, served by the API | React/Vite, Plotly | No Node/build step, works offline, fewer moving parts. Trade-off: fewer chart features (no zoom/pan). |
| Streaming | Kafka via `kafka-python` | Redpanda, RabbitMQ | Kafka was the existing setup (4.1.2, Java 21). Pure-Python client avoids native builds. |
| Delivery semantics | At-least-once + idempotent storage | Kafka transactions / exactly-once | Simple, verifiable (full-topic replay changes nothing), honestly described. |
| Anomalies | Rolling median/MAD rules | ML/LLM | Explainable, deterministic, no paid dependency; matches the brief. |
| Invalid values | Out-of-validation-bounds = quarantined; out-of-operating-range = valid but flagged | Treat both as flags | Separates "invalid data" from "unusual but valid measurement". |
| Missing values | NULL + status MISSING | 0 / NaN | Missing is not zero. |
| Config | JSON files + API overrides in `data/config` | YAML, DB | No extra dependency; shipped defaults never mutated. |
| Docker, dbt, Great Expectations, Snowflake/AWS, Airflow, Spark | **Not used** | | No real need yet; the brief asks not to add them for keywords. |

## Implemented / partial / planned
- **Implemented**: everything listed in the README feature table.
- **Partial**: Kafka lag (available only while the consumer runs); dashboard start/stop of producer/consumer (child processes of the API; not hardened); quarantine replay (API only, no UI button).
- **Planned**: schema migrations framework, data-quality score, Parquet export, GitHub Actions, Docker Compose, PDF reports, pan/zoom charts, authentication.
