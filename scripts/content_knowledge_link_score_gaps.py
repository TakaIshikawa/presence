#!/usr/bin/env python3
"""Report content knowledge link score gaps."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from _focused_report_cli import non_negative_float, positive_int, run  # noqa: E402
from evaluation.content_knowledge_link_score_gaps import DEFAULT_LIMIT, DEFAULT_MIN_LINKS, DEFAULT_WEAK_THRESHOLD, build_content_knowledge_link_score_gaps_report_from_db, format_content_knowledge_link_score_gaps_json, format_content_knowledge_link_score_gaps_text  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "text"), default="json")
    p.add_argument("--weak-threshold", type=non_negative_float, default=DEFAULT_WEAK_THRESHOLD)
    p.add_argument("--min-links", type=positive_int, default=DEFAULT_MIN_LINKS)
    p.add_argument("--limit", type=positive_int, default=DEFAULT_LIMIT)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    return run(args, build_content_knowledge_link_score_gaps_report_from_db, format_content_knowledge_link_score_gaps_json, format_content_knowledge_link_score_gaps_text, {"weak_threshold": args.weak_threshold, "min_links": args.min_links, "limit": args.limit})


if __name__ == "__main__":
    raise SystemExit(main())
