"""Rule behaviour on small controlled series stored in SQLite."""

from orbitalpulse.analytics.anomalies import analyze
from orbitalpulse.ingestion.core import finish_job, ingest_records, start_job
from orbitalpulse.pipeline import analyze_run
from orbitalpulse.quality.rules import check_run
from tests.conftest import rec


def load(conn, values, sensor="chamber_pressure", start=0.0, step=0.1, run="R1", phase="TEST_WINDOW", seq0=0, drop=()):
    recs = []
    for i, v in enumerate(values):
        if i in drop:
            continue
        ts = f"2026-01-01T00:{int((start + i * step) // 60):02d}:{(start + i * step) % 60:06.3f}Z"
        recs.append(rec(event_id=f"{run}-{sensor}-{phase}-{start}-{i}", test_run_id=run, sensor_id=sensor, event_timestamp=ts,
                        ingestion_timestamp=ts, sequence_number=str(seq0 + i), sensor_unit="", sensor_value="" if v is None else str(v),
                        test_phase=phase))
    job, arch = start_job(conn, "T", "t")
    c = ingest_records(conn, [(f"r{i}", r) for i, r in enumerate(recs)], job_id=job, archive=arch)
    finish_job(conn, job, c, arch)
    return c


def rules(conn, sensors, settings, run="R1"):
    return {f.rule_id for f in analyze(conn, run, sensors, settings)}


FLAT = [20.0 + 0.01 * ((i * 7) % 5) for i in range(60)]


def test_spike_detected_and_isolated(conn, sensors, settings):
    vals = list(FLAT); vals[40] = 55.0
    load(conn, vals)
    f = [x for x in analyze(conn, "R1", sensors, settings) if x.rule_id == "AN_SPIKE"]
    assert len(f) == 1 and f[0].evidence["observed_extreme"] == 55.0 and "rolling median" in f[0].explanation
    assert f[0].limitations and "confirmed" not in f[0].explanation.lower() and f[0].category == "UNUSUAL_SENSOR_PATTERN"


def test_drop_detected(conn, sensors, settings):
    vals = list(FLAT); vals[40] = 1.0
    load(conn, vals)
    assert "AN_DROP" in rules(conn, sensors, settings)


def test_level_shift_is_not_a_spike(conn, sensors, settings):
    load(conn, [20.0 + 0.01 * (i % 3) for i in range(40)] + [40.0 + 0.01 * (i % 3) for i in range(40)])
    assert "AN_SPIKE" not in rules(conn, sensors, settings)


def test_flatline_detected_and_constant_noise_free_ok(conn, sensors, settings):
    vals = list(FLAT[:20]) + [23.0] * 25 + list(FLAT[:15])
    load(conn, vals)
    f = [x for x in analyze(conn, "R1", sensors, settings) if x.rule_id == "AN_FLATLINE"]
    assert len(f) == 1 and f[0].evidence["identical_samples"] == 25


def test_zero_variance_series_does_not_crash(conn, sensors, settings):
    load(conn, [5.0] * 60)
    analyze(conn, "R1", sensors, settings)  # must not raise


def test_range_violation_uses_operating_range(conn, sensors, settings):
    vals = list(FLAT); vals[30:33] = [70.0, 71.0, 72.0]  # operating_max=60, validation_max=120
    load(conn, vals)
    f = [x for x in analyze(conn, "R1", sensors, settings) if x.rule_id == "AN_RANGE_EXCEEDED"]
    assert len(f) == 1 and f[0].evidence["samples_outside_range"] == 3 and f[0].category == "THRESHOLD_EXCEEDED"


def test_missing_data_burst(conn, sensors, settings):
    load(conn, FLAT, drop=set(range(20, 35)))
    f = [x for x in analyze(conn, "R1", sensors, settings) if x.rule_id == "AN_MISSING_BURST"]
    assert f and "estimate" in f[0].evidence["basis"]
    issues = {i.rule_id for i in check_run(conn, "R1", sensors, settings)}
    assert {"DQ_TIME_GAP", "DQ_MISSING_SEQUENCE"} <= issues


def test_null_value_burst(conn, sensors, settings):
    vals = list(FLAT); vals[10:16] = [None] * 6
    load(conn, vals)
    assert "AN_MISSING_BURST" in rules(conn, sensors, settings)


def test_drift_in_test_window(conn, sensors, settings):
    load(conn, [20.0 + 0.2 * i for i in range(100)])
    f = [x for x in analyze(conn, "R1", sensors, settings) if x.rule_id == "AN_DRIFT"]
    assert f and f[0].evidence["relative_change_over_window_pct"] > 25


def test_insufficient_data_is_explicit_not_a_diagnosis(conn, sensors, settings):
    load(conn, [1.0, 2.0, 3.0])
    f = analyze(conn, "R1", sensors, settings)
    assert [x.rule_id for x in f] == ["AN_INSUFFICIENT_DATA"] and f[0].category == "INSUFFICIENT_EVIDENCE"
    assert "not a clean bill of health" in f[0].explanation


def test_empty_run_returns_insufficient_evidence(conn, sensors, settings):
    f = analyze(conn, "NOPE", sensors, settings)
    assert f[0].rule_id == "AN_INSUFFICIENT_DATA"


def test_phase_transition_out_of_order(conn, sensors, settings):
    load(conn, FLAT[:20], phase="STARTUP", start=0)
    load(conn, FLAT[:20], phase="TEST_WINDOW", start=2.0, seq0=20)
    # a backwards label: TEST_WINDOW -> STARTUP
    recs = [rec(event_id=f"b{i}", sensor_id="chamber_pressure", event_timestamp=f"2026-01-01T00:00:{4 + i * 0.1:06.3f}Z",
                ingestion_timestamp=f"2026-01-01T00:00:{4 + i * 0.1:06.3f}Z", sequence_number=str(40 + i), test_phase="STARTUP", sensor_value="20", sensor_unit="")
            for i in range(5)]
    job, arch = start_job(conn, "T", "t"); ingest_records(conn, [(f"b{i}", r) for i, r in enumerate(recs)], job_id=job, archive=arch)
    assert "AN_PHASE_TRANSITION" in rules(conn, sensors, settings)


def test_out_of_order_arrival_and_duplicates_flagged(conn, sensors, settings):
    recs = [rec(event_id=f"o{i}", event_timestamp=f"2026-01-01T00:00:0{t}Z", sequence_number=str(i), sensor_value="10")
            for i, t in enumerate([1, 3, 2, 4])]
    job, arch = start_job(conn, "T", "t"); ingest_records(conn, [(f"o{i}", r) for i, r in enumerate(recs)], job_id=job, archive=arch)
    assert "DQ_OUT_OF_ORDER" in {i.rule_id for i in check_run(conn, "R1", sensors, settings)}


def test_cross_sensor_mismatch(conn, sensors, settings):
    for s in ("temperature", "thrust_measurement", "vibration_rms"):
        load(conn, FLAT, sensor=s, start=0)
    load(conn, FLAT, sensor="load_cell_measurement", start=10.0)  # starts 10s late
    f = [x for x in analyze(conn, "R1", sensors, settings) if x.rule_id == "AN_CROSS_SENSOR_TIME_MISMATCH"]
    assert [x.sensor_id for x in f] == ["load_cell_measurement"]


def test_findings_review_survives_reanalysis(conn):
    vals = list(FLAT); vals[40] = 55.0
    load(conn, vals)
    analyze_run(conn, "R1")
    fid = conn.execute("SELECT finding_id FROM findings WHERE rule_id='AN_SPIKE'").fetchone()[0]
    conn.execute("UPDATE findings SET review_status='FALSE_POSITIVE', reviewer='me', reviewed_at='x' WHERE finding_id=?", (fid,)); conn.commit()
    analyze_run(conn, "R1")
    row = conn.execute("SELECT review_status, reviewer FROM findings WHERE finding_id=?", (fid,)).fetchone()
    assert tuple(row) == ("FALSE_POSITIVE", "me")
