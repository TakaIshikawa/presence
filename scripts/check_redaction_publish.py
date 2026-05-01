#!/usr/bin/env python3
"""Check queued generated content for redaction leaks before publishing."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.redaction_publish_guard import (  # noqa: E402
    VALID_PLATFORMS,
    build_redaction_publish_guard_report,
    export_to_json,
    format_text_report,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--queue-id",
        type=int,
        help="Only check one publish_queue row.",
    )
    parser.add_argument(
        "--platform",
        choices=VALID_PLATFORMS,
        help="Only check queued rows for one publish_queue platform.",
    )
    parser.add_argument(
        "--include-warnings",
        action="store_true",
        help="Include configured warning-severity redaction rules.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Exit non-zero when blocked queued items exist.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    with script_context() as (config, db):
        report = build_redaction_publish_guard_report(
            db,
            queue_id=args.queue_id,
            platform=args.platform,
            include_warnings=args.include_warnings,
            patterns=config.privacy.redaction_patterns,
        )

    if args.format == "json":
        print(export_to_json(report))
    else:
        print(format_text_report(report))

    if args.fail_on_blocked and report.blocked_count:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
