from cmapss_pipeline import build_quality_summary

common = {
    "source_file": "synthetic_test.txt",
    "examined_rows": 10,
    "blank_lines": 0,
    "parsed_rows": 10,
    "source_rejected_rows": 0,
    "published_rows": 10,
    "consumed_rows": 10,
    "valid_rows": 10,
    "invalid_rows": 0,
}

# Test 1: successful run with matching counts
success = build_quality_summary(
    **common,
    status="SUCCESS",
    error=None,
)
assert success["status"] == "SUCCESS"
assert success["checks"]["all_counts_reconcile"] is True
print("PASS: matching counts reconcile for a successful run.")

# Test 2: failed run even though counts match
failed = build_quality_summary(
    **common,
    status="FAILED",
    error="RuntimeError: Synthetic failure",
)
assert failed["status"] == "FAILED"
assert failed["error"] == "RuntimeError: Synthetic failure"
assert failed["checks"]["all_counts_reconcile"] is True
print("PASS: failure status is preserved even when counts reconcile.")

# Test 3: count mismatch is detected
mismatch = build_quality_summary(
    **{**common, "consumed_rows": 9},
    status="FAILED",
    error="Synthetic count mismatch",
)
assert mismatch["checks"]["published_equals_consumed"] is False
assert mismatch["checks"]["all_counts_reconcile"] is False
print("PASS: count mismatches are detected.")

print("PASS: all 3 quality-summary tests succeeded.")
