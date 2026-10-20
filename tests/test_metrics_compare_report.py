import json

import pytest

from orbitalpulse.analytics.compare import compare_runs
from orbitalpulse.analytics.metrics import sensor_metrics
from orbitalpulse.analytics.timeseries import downsample
from orbitalpulse.demo import load_demo
from orbitalpulse.pipeline import analyze_run
from orbitalpulse.reporting.report import build_report, report_csv, report_html
from tests.test_quality_and_anomalies import FLAT, load


def test_sensor_metrics_values_and_estimates(conn, sensors):
    load(conn, [1.0, 2.0, 3.0, 4.0, None], drop=())
    m = sensor_metrics(conn, "R1", sensors.by_id())[0]
    assert (m["record_count"], m["valid_count"], m["missing_value_count"]) == (5, 4, 1)
    assert (m["min"], m["max"], m["mean"], m["median"]) == (1.0, 4.0, 2.5, 2.5)
    assert m["std"] == pytest.approx(1.118, abs=1e-3) and m["duration_s"] == pytest.approx(0.4)
    assert "estimated" in m["completeness_basis"] and m["observed_interval_median_s"] == pytest.approx(0.1)


def test_empty_run_metrics_do_not_crash(conn, sensors):
    assert sensor_metrics(conn, "NONE", sensors.by_id()) == []


def test_filters(conn, sensors):
    load(conn, FLAT, phase="STARTUP")
    assert sensor_metrics(conn, "R1", sensors.by_id(), phase="POST_TEST") == []
    assert sensor_metrics(conn, "R1", sensors.by_id(), sensor_id="chamber_pressure")[0]["record_count"] == 60


def test_downsample_keeps_spike():
    pts = [(i, 1.0 if i != 500 else 99.0) for i in range(2000)]
    out, reduced = downsample(pts, 100)
    assert reduced and len(out) <= 110 and any(p[1] == 99.0 for p in out)


@pytest.fixture()
def demo(conn):
    load_demo(conn)
    return conn


def test_demo_scenarios_behave_as_designed(demo):
    n = lambda run, t: demo.execute(f"SELECT COUNT(*) FROM {t} WHERE run_id=?", (run,)).fetchone()[0]  # noqa: E731
    assert n("RUN-001-CLEAN", "findings") == 0 and n("RUN-001-CLEAN", "quarantine") == 0
    assert n("RUN-002-DQ-DEFECTS", "quarantine") == 8
    assert n("RUN-002-DQ-DEFECTS", "quality_issues") > 10
    sus = {r[0] for r in demo.execute("SELECT rule_id FROM findings WHERE run_id='RUN-003-SUSPICIOUS'")}
    assert {"AN_SPIKE", "AN_DROP", "AN_FLATLINE", "AN_DRIFT", "AN_RANGE_EXCEEDED", "AN_PHASE_TRANSITION", "AN_CROSS_SENSOR_TIME_MISMATCH"} <= sus


def test_demo_reload_is_idempotent(demo):
    before = demo.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    res = load_demo(demo)
    assert res["imported_runs"] == [] and len(res["already_loaded_runs"]) == 4
    assert demo.execute("SELECT COUNT(*) FROM events").fetchone()[0] == before


def test_comparison_metrics_and_rate_warning(demo, sensors):
    c = compare_runs(demo, ["RUN-001-CLEAN", "RUN-004-CLEAN-B"], sensors.by_id())
    assert any("Sampling intervals differ" in w["message"] for w in c["warnings"])
    s = next(x for x in c["sensors"] if x["sensor_id"] == "thrust_measurement")
    d = s["diff_vs_baseline"]["RUN-004-CLEAN-B"]["mean"]
    assert d["abs"] == pytest.approx(s["per_run"]["RUN-004-CLEAN-B"]["mean"] - s["per_run"]["RUN-001-CLEAN"]["mean"])
    assert d["pct"] == pytest.approx(100 * d["abs"] / s["per_run"]["RUN-001-CLEAN"]["mean"])
    assert "does not prove" in c["method_note"]


def test_unit_mismatch_is_not_compared(conn, sensors):
    load(conn, FLAT, run="A"); load(conn, FLAT, run="B")
    conn.execute("UPDATE events SET unit='psi' WHERE run_id='B'"); conn.commit()
    c = compare_runs(conn, ["A", "B"], sensors.by_id())
    s = c["sensors"][0]
    assert s["status"] == "NOT_COMPARABLE" and s["diff_vs_baseline"] == {}
    assert any("Units differ" in w["message"] for w in c["warnings"])


def test_compare_needs_two_runs(conn, sensors):
    with pytest.raises(ValueError):
        compare_runs(conn, ["A"], sensors.by_id())


def test_report_contents_and_determinism(demo):
    r = build_report(demo, ["RUN-002-DQ-DEFECTS", "RUN-001-CLEAN"])
    assert r["disclaimer"].startswith("SYNTHETIC DATA") and "comparison" in r and r["limitations"]
    run = next(x for x in r["runs"] if x["run_id"] == "RUN-002-DQ-DEFECTS")
    assert run["ingestion_jobs"] and run["sensor_metrics"] and run["quality_issue_counts"] and run["executive_summary"]
    assert any("quarantined" in line for line in run["executive_summary"])
    r2 = build_report(demo, ["RUN-001-CLEAN", "RUN-002-DQ-DEFECTS"])
    strip = lambda x: json.dumps({k: v for k, v in x.items() if k != "meta"}, sort_keys=True)  # noqa: E731
    assert strip(r) == strip(r2)  # only generated_at (in meta) may differ
    assert "RUN-001-CLEAN" in report_csv(r) and "<html" in report_html(r) and "SYNTHETIC DATA" in report_html(r)


def test_html_report_escapes(conn):
    load(conn, FLAT, run="<script>x</script>")
    analyze_run(conn, "<script>x</script>")
    html = report_html(build_report(conn, ["<script>x</script>"]))
    assert "<script>x" not in html
