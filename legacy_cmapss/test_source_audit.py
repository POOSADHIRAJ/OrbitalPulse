
import tempfile
from pathlib import Path
from unittest.mock import patch

from cmapss_pipeline import read_rows

# Synthetic test data only; this is not NASA data.
synthetic_input = (
    "1.0 2.0 3.0\n"
    "\n"
    "4.0 not_a_number 6.0\n"
    "7.0 8.0 9.0\n"
)

with tempfile.TemporaryDirectory() as temp_dir:
    test_file = Path(temp_dir) / "synthetic_input.txt"
    test_file.write_text(synthetic_input, encoding="utf-8")

    with (
        patch("cmapss_pipeline.DATASET", test_file),
        patch("cmapss_pipeline.LIMIT", 100),
    ):
        rows, rejected, blank_lines, examined = read_rows()

    assert examined == 3, f"Expected 3 examined rows, got {examined}"
    assert blank_lines == 1, f"Expected 1 blank line, got {blank_lines}"
    assert len(rows) == 2, f"Expected 2 valid rows, got {len(rows)}"
    assert len(rejected) == 1, f"Expected 1 rejected row, got {len(rejected)}"
    assert rejected[0]["source_line"] == 3
    assert "cannot be parsed" in rejected[0]["reason"]
    assert rejected[0]["raw_line"] == "4.0 not_a_number 6.0"

    print("PASS: valid synthetic rows were parsed.")
    print("PASS: blank lines were counted separately.")
    print("PASS: malformed row was rejected.")
    print("PASS: original line number and contents were preserved.")
    print("PASS: parsing reason was recorded.")
    print("PASS: all source-audit tests succeeded.")
