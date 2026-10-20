
import math

from cmapss_pipeline import validate_record


# These are synthetic test cases, not NASA dataset records.
EXPECTED_FIELDS = 3

tests = [
    {
        "name": "Valid numeric record",
        "record": {"values": [1.0, 2.0, 3.0]},
        "should_pass": True,
        "expected_error": None,
    },
    {
        "name": "Wrong number of fields",
        "record": {"values": [1.0, 2.0]},
        "should_pass": False,
        "expected_error": "Expected 3 fields, got 2",
    },
    {
        "name": "Values is not a list",
        "record": {"values": "1,2,3"},
        "should_pass": False,
        "expected_error": "'values' must be a list",
    },
    {
        "name": "NaN value",
        "record": {"values": [1.0, float("nan"), 3.0]},
        "should_pass": False,
        "expected_error": "must be finite",
    },
    {
        "name": "Infinite value",
        "record": {"values": [1.0, float("inf"), 3.0]},
        "should_pass": False,
        "expected_error": "must be finite",
    },
    {
        "name": "Boolean used as a number",
        "record": {"values": [1.0, True, 3.0]},
        "should_pass": False,
        "expected_error": "Field 1 must be numeric",
    },
    {
        "name": "Text used as a number",
        "record": {"values": [1.0, "bad", 3.0]},
        "should_pass": False,
        "expected_error": "Field 1 must be numeric",
    },
    {
        "name": "Record is not a dictionary",
        "record": [1.0, 2.0, 3.0],
        "should_pass": False,
        "expected_error": "Record must be a dictionary",
    },
]

failures = 0

print("OrbitalPulse validator tests")
print("Synthetic test records only")
print("-" * 45)

for test in tests:
    valid, errors = validate_record(
        test["record"],
        expected_field_count=EXPECTED_FIELDS,
    )

    passed = valid == test["should_pass"]

    if test["expected_error"] is not None:
        passed = passed and any(
            test["expected_error"] in error
            for error in errors
        )

    if passed:
        print(f"PASS: {test['name']}")
    else:
        failures += 1
        print(f"FAIL: {test['name']}")
        print(f"  Expected valid: {test['should_pass']}")
        print(f"  Actual valid:   {valid}")
        print(f"  Errors:         {errors}")

print("-" * 45)
print(f"Tests passed: {len(tests) - failures}/{len(tests)}")
print(f"Tests failed: {failures}")

if failures:
    raise SystemExit(1)

print("PASS: all validator tests succeeded.")
