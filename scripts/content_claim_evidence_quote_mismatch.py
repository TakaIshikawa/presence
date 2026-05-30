#!/usr/bin/env python3
"""Report content claim evidence quote mismatches."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.content_claim_evidence_quote_mismatch import DEFAULT_DISTANCE_THRESHOLD, DEFAULT_LIMIT, build_content_claim_evidence_quote_mismatch_report_from_db, format_content_claim_evidence_quote_mismatch_json, format_content_claim_evidence_quote_mismatch_text  # noqa: E402
from runner import script_context  # noqa: E402


def _positive_int(v: str) -> int:
    n = int(v)
    if n <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return n


def _distance(v: str) -> float:
    n = float(v)
    if not 0 <= n <= 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return n


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "text"), default="json")
    p.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    p.add_argument("--distance-threshold", type=_distance, default=DEFAULT_DISTANCE_THRESHOLD)
    try:
        a = p.parse_args(argv)
        kwargs = {"limit": a.limit, "distance_threshold": a.distance_threshold}
        if a.db:
            with sqlite3.connect(a.db) as conn:
                report = build_content_claim_evidence_quote_mismatch_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_c, db):
                report = build_content_claim_evidence_quote_mismatch_report_from_db(db, **kwargs)
    except SystemExit as exc:
        return int(exc.code or 0)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_content_claim_evidence_quote_mismatch_text(report) if a.format == "text" else format_content_claim_evidence_quote_mismatch_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
