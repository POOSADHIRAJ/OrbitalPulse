import json

import pytest

from orbitalpulse.ingestion.core import finish_job, ingest_records, start_job
from orbitalpulse.ingestion.csv_ingest import IngestError, ingest_csv, preview_csv
from tests.conftest import rec


def run_ingest(conn, records, **kw):
    job_id, arch = start_job(conn, "TEST", "t")
    counts = ingest_records(conn, [(f"ref{i}", r) for i, r in enumerate(records)], job_id=job_id, archive=arch, **kw)
    finish_job(conn, job_id, counts, arch)
    return job_id, counts


def test_idempotent_storage_and_duplicate_delivery(conn):
    _, c1 = run_ingest(conn, [rec(), rec()])
    assert (c1.imported, c1.duplicates, c1.quarantined) == (1, 1, 0)
    _, c2 = run_ingest(conn, [rec()])  # redelivery in a later job
    assert (c2.imported, c2.duplicates) == (0, 1)
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM quality_issues WHERE rule_id='DQ_DUPLICATE_DELIVERY'").fetchone()[0] >= 1


def test_conflicting_duplicate_id_is_quarantined_not_overwritten(conn):
    run_ingest(conn, [rec(sensor_value="10")])
    _, c = run_ingest(conn, [rec(sensor_value="11")])
    assert c.quarantined == 1
    assert conn.execute("SELECT value FROM events WHERE event_id='E1'").fetchone()[0] == 10.0
    assert conn.execute("SELECT rule_id FROM quarantine").fetchone()[0] == "DQ_DUPLICATE_EVENT_ID_CONFLICT"


def test_invalid_records_quarantined_with_payload_and_counts_reconcile(conn):
    bad = [rec(event_id="B1", sensor_value="x"), rec(event_id="B2", event_timestamp="nope"), rec(event_id="G1")]
    job, c = run_ingest(conn, bad)
    assert c.total == 3 and c.imported == 1 and c.quarantined == 2
    payload = json.loads(conn.execute("SELECT raw_payload FROM quarantine WHERE event_id='B1'").fetchone()[0])
    assert payload["sensor_value"] == "x"  # original preserved
    j = conn.execute("SELECT * FROM ingestion_jobs WHERE job_id=?", (job,)).fetchone()
    assert j["rows_total"] == j["imported"] + j["duplicates"] + j["quarantined"]


def test_raw_archive_written_before_validation(conn, tmp_path):
    job, _ = run_ingest(conn, [rec(sensor_value="garbage")])
    name = conn.execute("SELECT raw_archive FROM ingestion_jobs WHERE job_id=?", (job,)).fetchone()[0]
    lines = (tmp_path / "raw" / name).read_text().splitlines()
    assert json.loads(lines[0])["payload"]["sensor_value"] == "garbage"


def test_quarantine_replay_is_idempotent(conn):
    bad = [rec(event_id="B1", sensor_value="x")]
    _, c1 = run_ingest(conn, bad)
    _, c2 = run_ingest(conn, bad)  # same source_ref + same payload = replay of the same message
    assert c1.quarantined == 1 and c2.quarantined == 0 and c2.duplicates == 1
    assert conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0] == 1


CSV = b"time,run,channel,value,unit\n2026-02-01T00:00:00Z,X1,temperature,25.5,degC\n2026-02-01T00:00:01Z,X1,temperature,,degC\n2026-02-01T00:00:02Z,X1,temperature,abc,degC\nshort,row\n"


def test_csv_ingest_with_alias_mapping_and_malformed_rows(conn):
    pv = preview_csv("a.csv", CSV)
    assert pv["ready"] and pv["suggested_mapping"]["event_timestamp"] == "time"
    res = ingest_csv(conn, "a.csv", CSV, default_campaign="CX")
    assert (res["rows_total"], res["imported"], res["quarantined"]) == (4, 2, 2)
    rules = {r[0] for r in conn.execute("SELECT rule_id FROM quarantine")}
    assert rules == {"DQ_NON_NUMERIC_VALUE", "DQ_MALFORMED_ROW"}
    assert conn.execute("SELECT COUNT(*) FROM events WHERE value IS NULL").fetchone()[0] == 1


def test_duplicate_file_import_refused(conn):
    ingest_csv(conn, "a.csv", CSV)
    with pytest.raises(IngestError) as e:
        ingest_csv(conn, "b.csv", CSV)
    assert e.value.status == 409


@pytest.mark.parametrize("name,content,status", [("a.csv", b"", 422), ("a.txt", b"x", 415), ("a.csv", b"a,b\n", 422),
                                                  ("a.csv", b"a,b\n1,2\n", 422), ("a.csv", b"\xff\xfe\x00bad", 415)])
def test_bad_csv_inputs(conn, name, content, status):
    with pytest.raises(IngestError) as e:
        ingest_csv(conn, name, content)
    assert e.value.status == status


def test_explicit_mapping_and_default_run(conn):
    data = b"ts,ch,v\n2026-02-01T00:00:00Z,temperature,20\n"
    res = ingest_csv(conn, "m.csv", data, mapping={"event_timestamp": "ts", "sensor_id": "ch", "sensor_value": "v"}, default_run_id="DEF")
    assert res["runs"] == ["DEF"] and res["imported"] == 1
