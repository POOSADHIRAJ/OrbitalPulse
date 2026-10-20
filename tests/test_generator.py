from orbitalpulse.simulator.generator import DEMO_PLANS, RunPlan, generate_campaign, generate_run


def test_generator_is_deterministic(sensors):
    assert generate_campaign(sensors=sensors) == generate_campaign(sensors=sensors)


def test_different_seed_changes_output(sensors):
    a = generate_run(RunPlan("R", "clean", 20, seed=1), sensors)
    b = generate_run(RunPlan("R", "clean", 20, seed=2), sensors)
    assert a != b and len(a) == len(b)


def test_all_records_labelled_synthetic_with_unique_ids_in_clean_run(sensors):
    recs = generate_run(DEMO_PLANS[0], sensors)
    assert {r["source_type"] for r in recs} == {"SYNTHETIC"}
    assert len({r["event_id"] for r in recs}) == len(recs)


def test_defect_run_contains_each_injected_defect(sensors):
    recs = generate_run(DEMO_PLANS[1], sensors)
    ids = [r["event_id"] for r in recs]
    assert len(ids) != len(set(ids))                                  # duplicates
    assert any(r["sensor_value"] == "" for r in recs)                 # missing values
    assert any(":MALFORMED:" in i for i in ids)                       # malformed
    assert len(recs) < len(generate_run(DEMO_PLANS[0], sensors)) + 60  # samples dropped
