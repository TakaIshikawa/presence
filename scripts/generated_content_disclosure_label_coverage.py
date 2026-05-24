#!/usr/bin/env python3
"""Report generated content missing required disclosure labels."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.generated_content_disclosure_label_coverage import DEFAULT_LIMIT, DEFAULT_REQUIRED_LABELS, build_generated_content_disclosure_label_coverage_report_from_db, format_generated_content_disclosure_label_coverage_json, format_generated_content_disclosure_label_coverage_text  # noqa: E402
from runner import script_context  # noqa: E402


def _positive_int(v: str) -> int:
    n = int(v)
    if n <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return n


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "text"), default="json")
    p.add_argument("--required-label", action="append", default=[])
    p.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    try:
        a = p.parse_args(argv)
        labels = a.required_label or list(DEFAULT_REQUIRED_LABELS)
        kwargs = {"required_labels": labels, "limit": a.limit}
        if a.db:
            with sqlite3.connect(a.db) as conn:
                report = build_generated_content_disclosure_label_coverage_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_c, db):
                report = build_generated_content_disclosure_label_coverage_report_from_db(db, **kwargs)
    except SystemExit as exc:
        return int(exc.code or 0)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_generated_content_disclosure_label_coverage_text(report) if a.format == "text" else format_generated_content_disclosure_label_coverage_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

