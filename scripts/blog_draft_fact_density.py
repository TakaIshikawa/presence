#!/usr/bin/env python3
"""Report blog draft fact density risks."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.blog_draft_fact_density import DEFAULT_LIMIT, DEFAULT_MIN_FACTS_PER_100_WORDS, build_blog_draft_fact_density_report_from_db, format_blog_draft_fact_density_json, format_blog_draft_fact_density_text  # noqa: E402
from runner import script_context  # noqa: E402


def _positive_int(v: str) -> int:
    n = int(v)
    if n <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return n


def _positive_float(v: str) -> float:
    n = float(v)
    if n <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return n


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "text"), default="json")
    p.add_argument("--min-facts-per-100-words", type=_positive_float, default=DEFAULT_MIN_FACTS_PER_100_WORDS)
    p.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    try:
        a = p.parse_args(argv)
        kwargs = {"min_facts_per_100_words": a.min_facts_per_100_words, "limit": a.limit}
        if a.db:
            with sqlite3.connect(a.db) as conn:
                report = build_blog_draft_fact_density_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_c, db):
                report = build_blog_draft_fact_density_report_from_db(db, **kwargs)
    except SystemExit as exc:
        return int(exc.code or 0)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_blog_draft_fact_density_text(report) if a.format == "text" else format_blog_draft_fact_density_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

