"""OrbitalPulse developer CLI.   python scripts/op.py <command>

  generate-demo   write the deterministic synthetic CSVs to data/demo/
  import-demo     generate (if needed) + import them through the real CSV pipeline (idempotent)
  reset           delete ALL stored data (DB rows, raw archive, demo CSVs); config overrides are kept
  analyze         re-run quality rules + anomaly rules on every stored run
  serve           start the API + dashboard on http://127.0.0.1:8000
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orbitalpulse.storage.db import session  # noqa: E402


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["generate-demo", "import-demo", "reset", "analyze", "serve"])
    p.add_argument("--reset", action="store_true", help="import-demo: wipe everything first")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--host", default="127.0.0.1")
    a = p.parse_args(argv)

    if a.command == "generate-demo":
        from orbitalpulse.demo import generate_demo_files
        for f in generate_demo_files():
            print(f)
    elif a.command == "import-demo":
        from orbitalpulse.demo import load_demo
        with session() as conn:
            print(json.dumps(load_demo(conn, reset=a.reset), indent=2))
    elif a.command == "reset":
        from orbitalpulse.demo import reset_demo
        with session() as conn:
            reset_demo(conn)
        print("All stored data deleted.")
    elif a.command == "analyze":
        from orbitalpulse.pipeline import analyze_runs
        with session() as conn:
            ids = [r["run_id"] for r in conn.execute("SELECT run_id FROM runs")]
            print(json.dumps(analyze_runs(conn, ids), indent=2))
    elif a.command == "serve":
        import uvicorn
        uvicorn.run("orbitalpulse.api.main:app", host=a.host, port=a.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
