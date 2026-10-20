import pytest


@pytest.fixture()
def loaded(client):
    assert client.post("/api/demo/load").status_code == 200
    return client


def test_health_ready_status(client):
    assert client.get("/api/health").json()["status"] == "ok"
    r = client.get("/api/ready")
    assert r.status_code == 200 and r.json()["checks"]["database"]["ok"] and "kafka_optional" in r.json()["checks"]
    s = client.get("/api/status").json()
    assert s["mode"] in ("BATCH_DEMO", "LIVE_KAFKA") and "disclaimer" in s


def test_kafka_status_is_honest_when_broker_absent(client, monkeypatch):
    from orbitalpulse.api import main
    monkeypatch.setattr(main, "broker_reachable", lambda *a, **k: {"reachable": False, "bootstrap": "x:1", "error": "refused"})
    assert client.get("/api/status").json()["mode"] == "BATCH_DEMO"
    r = client.post("/api/stream/producer/start", json={})
    assert r.status_code == 503 and "not reachable" in r.json()["detail"]["message"]
    assert client.get("/api/ready").status_code == 200  # Kafka is optional


def test_empty_state(client):
    assert client.get("/api/runs").json() == [] and client.get("/api/overview").json()["runs"] == 0
    assert client.get("/api/runs/NOPE").status_code == 404


def test_runs_pagination_and_filtering(loaded):
    page = loaded.get("/api/runs/RUN-001-CLEAN/events", params={"sensor_id": "temperature", "limit": 10, "offset": 5}).json()
    assert len(page["items"]) == 10 and page["total"] == 120 and page["offset"] == 5
    assert all(i["sensor_id"] == "temperature" for i in page["items"])
    assert loaded.get("/api/runs/RUN-001-CLEAN/events", params={"limit": 999999}).json()["limit"] == 1000  # capped
    assert loaded.get("/api/runs/RUN-001-CLEAN/events", params={"phase": "POST_TEST", "sensor_id": "temperature"}).json()["total"] > 0
    assert loaded.get("/api/runs/RUN-001-CLEAN/events", params={"start": "garbage"}).status_code == 422
    assert loaded.get("/api/runs/RUN-001-CLEAN/events", params={"limit": 0}).status_code == 422


def test_timeseries_bounded_and_elapsed_alignment(loaded):
    t = loaded.get("/api/runs/RUN-001-CLEAN/timeseries", params={"sensor_ids": "chamber_pressure", "max_points": 100, "align": "elapsed"}).json()["series"][0]
    assert t["total_points"] == 600 and t["returned_points"] <= 110 and t["downsampled"] and min(t["x"]) == 0
    assert loaded.get("/api/runs/RUN-001-CLEAN/timeseries", params={"sensor_ids": "nope"}).status_code == 404
    # stored timestamps unchanged by alignment
    assert loaded.get("/api/runs/RUN-001-CLEAN/events", params={"limit": 1}).json()["items"][0]["event_ts"].startswith("2026-01-15T10:00:00")


def test_quality_filters_exports_and_raw_payload(loaded):
    q = loaded.get("/api/quality/issues", params={"run_id": "RUN-002-DQ-DEFECTS", "severity": "ERROR"}).json()
    assert q["total"] > 0 and all(i["severity"] == "ERROR" for i in q["items"])
    csv = loaded.get("/api/quality/issues.csv", params={"rule_id": "DQ_LATE_EVENT"})
    assert csv.headers["content-type"].startswith("text/csv") and csv.text.count("\n") == 16  # header + 15 late events
    qr = loaded.get("/api/quarantine", params={"run_id": "RUN-002-DQ-DEFECTS"}).json()
    assert qr["total"] == 8 and "DQ_UNEXPECTED_UNIT" in qr["by_rule"]
    issue = next(i for i in loaded.get("/api/quality/issues", params={"rule_id": "DQ_NON_NUMERIC_VALUE"}).json()["items"])
    assert loaded.get(f"/api/quality/issues/{issue['issue_id']}").json()["raw_payload"] is not None


def test_findings_review_flow_and_validation(loaded):
    f = loaded.get("/api/findings", params={"run_id": "RUN-003-SUSPICIOUS", "rule_id": "AN_FLATLINE"}).json()["items"][0]
    d = loaded.get(f"/api/findings/{f['finding_id']}").json()
    assert d["evidence_events"] and d["limitations"] and d["threshold"]
    r = loaded.patch(f"/api/findings/{f['finding_id']}/review", json={"status": "FALSE_POSITIVE", "reviewer": "qa", "note": "n"}).json()
    assert r["review_status"] == "FALSE_POSITIVE" and r["reviewed_at"] and r["reviewer"] == "qa"
    assert loaded.patch(f"/api/findings/{f['finding_id']}/review", json={"status": "BOGUS"}).status_code == 422
    assert loaded.patch("/api/findings/NOPE/review", json={"status": "OPEN"}).status_code == 404


def test_compare_endpoint(loaded):
    assert loaded.get("/api/compare", params={"run_ids": "RUN-001-CLEAN"}).status_code == 422
    assert loaded.get("/api/compare", params={"run_ids": "RUN-001-CLEAN,NOPE"}).status_code == 404
    c = loaded.get("/api/compare", params={"run_ids": "RUN-001-CLEAN,RUN-003-SUSPICIOUS"}).json()
    assert c["anomaly_counts_by_rule"]["AN_SPIKE"]["RUN-003-SUSPICIOUS"] >= 1


def test_reports(loaded):
    for fmt, ct in (("json", "application/json"), ("csv", "text/csv"), ("html", "text/html")):
        r = loaded.get("/api/report", params={"run_ids": "RUN-002-DQ-DEFECTS", "format": fmt})
        assert r.status_code == 200 and r.headers["content-type"].startswith(ct)
    assert loaded.get("/api/report", params={"run_ids": "NOPE"}).status_code == 404
    assert loaded.get("/api/campaigns/SYNTH-CAMPAIGN-001/report").status_code == 200


def test_csv_upload_flow(client):
    good = b"time,run,channel,value\n2026-02-01T00:00:00Z,U1,temperature,25.5\n2026-02-01T00:00:01Z,U1,temperature,26\n"
    pv = client.post("/api/ingest/csv/preview", files={"file": ("u.csv", good)}).json()
    assert pv["ready"] and pv["row_count"] == 2
    r = client.post("/api/ingest/csv", files={"file": ("u.csv", good)}, data={"default_campaign": "UP"})
    assert r.status_code == 201 and r.json()["imported"] == 2
    assert client.post("/api/ingest/csv", files={"file": ("u.csv", good)}).status_code == 409
    assert client.post("/api/ingest/csv", files={"file": ("u.txt", good)}).status_code == 415
    assert client.post("/api/ingest/csv", files={"file": ("e.csv", b"")}).status_code == 422
    bad = client.post("/api/ingest/csv", files={"file": ("x.csv", b"a,b\n1,2\n")})
    assert bad.status_code == 422 and "Missing required" in bad.json()["detail"]["message"]
    assert client.post("/api/ingest/csv", files={"file": ("m.csv", good)}, data={"mapping": "[1]"}).status_code == 422


def test_upload_size_limit(client, monkeypatch):
    from orbitalpulse.api import main
    monkeypatch.setattr(main, "_limits", lambda: {"max_upload_mb": 0, "default_page_size": 100, "max_page_size": 1000, "max_chart_points": 1500})
    assert client.post("/api/ingest/csv", files={"file": ("big.csv", b"a,b\n1,2\n")}).status_code == 413


def test_config_roundtrip_and_validation(loaded):
    cfg = loaded.get("/api/config/sensors").json()
    cfg["sensors"][0]["operating_max"] = 1.0  # makes chamber pressure plateau exceed range
    r = loaded.put("/api/config/sensors", json=cfg)
    assert r.status_code == 200 and len(r.json()["reanalyzed"]) == 4
    assert loaded.get("/api/findings", params={"run_id": "RUN-001-CLEAN", "rule_id": "AN_RANGE_EXCEEDED"}).json()["total"] >= 1
    cfg["sensors"][0]["validation_min"], cfg["sensors"][0]["validation_max"] = 10, 5
    assert loaded.put("/api/config/sensors", json=cfg).status_code == 422
    assert loaded.post("/api/config/reset").json()["reset_to_defaults"]
    assert loaded.get("/api/findings", params={"run_id": "RUN-001-CLEAN"}).json()["total"] == 0
    assert "config_version" in loaded.get("/api/config/export").json()


def test_errors_do_not_leak_stack_traces(tmp_path, monkeypatch):
    monkeypatch.setenv("ORBITALPULSE_DATA_DIR", str(tmp_path))
    from fastapi.testclient import TestClient

    from orbitalpulse.api import main
    client = TestClient(main.app, raise_server_exceptions=False)
    monkeypatch.setattr(main, "load_sensor_config", lambda: (_ for _ in ()).throw(RuntimeError("secret internals")))
    r = client.get("/api/runs")
    assert r.status_code == 500 and "secret" not in r.text and "Traceback" not in r.text


def test_demo_reset(loaded):
    assert loaded.post("/api/demo/reset").json()["reset"]
    assert loaded.get("/api/overview").json()["events_valid"] == 0


def test_frontend_served_and_openapi(client):
    assert client.get("/").status_code == 200 and "OrbitalPulse" in client.get("/").text
    assert "/api/compare" in client.get("/openapi.json").json()["paths"]
