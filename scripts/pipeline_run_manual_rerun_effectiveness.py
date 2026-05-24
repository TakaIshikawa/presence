#!/usr/bin/env python3
"""Report effectiveness of manual pipeline reruns."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.pipeline_run_manual_rerun_effectiveness import DEFAULT_LIMIT, build_pipeline_run_manual_rerun_effectiveness_report_from_db, format_pipeline_run_manual_rerun_effectiveness_json, format_pipeline_run_manual_rerun_effectiveness_text  # noqa: E402
from runner import script_context  # noqa: E402

def _positive_int(v: str) -> int:
    n = int(v)
    if n <= 0: raise argparse.ArgumentTypeError("value must be positive")
    return n

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format", choices=("json","text"), default="json"); p.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    try:
        a = p.parse_args(argv)
        if a.db:
            with sqlite3.connect(a.db) as conn: report = build_pipeline_run_manual_rerun_effectiveness_report_from_db(conn, limit=a.limit)
        else:
            with script_context() as (_c, db): report = build_pipeline_run_manual_rerun_effectiveness_report_from_db(db, limit=a.limit)
    except SystemExit as exc: return int(exc.code or 0)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr); return 1
    print(format_pipeline_run_manual_rerun_effectiveness_text(report) if a.format == "text" else format_pipeline_run_manual_rerun_effectiveness_json(report)); return 0
if __name__ == "__main__": raise SystemExit(main())

