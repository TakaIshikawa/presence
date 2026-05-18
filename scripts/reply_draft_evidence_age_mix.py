#!/usr/bin/env python3
"""Report reply drafts with stale or narrow evidence age mixes."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_draft_evidence_age_mix import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_SINGLE_BAND_MIN_EVIDENCE,
    DEFAULT_STALE_DAYS,
    build_reply_draft_evidence_age_mix_report_from_db,
    format_reply_draft_evidence_age_mix_json,
    format_reply_draft_evidence_age_mix_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stale-days", type=_positive_int, default=DEFAULT_STALE_DAYS)
    parser.add_argument("--single-band-min-evidence", type=_positive_int, default=DEFAULT_SINGLE_BAND_MIN_EVIDENCE)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_reply_draft_evidence_age_mix_report_from_db(
                db,
                stale_days=args.stale_days,
                single_band_min_evidence=args.single_band_min_evidence,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_reply_draft_evidence_age_mix_text(report) if args.table or args.format == "text" else format_reply_draft_evidence_age_mix_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
