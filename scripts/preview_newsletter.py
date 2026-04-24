#!/usr/bin/env python3
"""Assemble newsletter preview artifacts without sending email."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_preview import (  # noqa: E402
    assemble_newsletter_preview,
    manual_subject_override,
    write_preview_artifact,
)
from runner import script_context  # noqa: E402

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--week-start", help="Start date as YYYY-MM-DD")
    parser.add_argument("--week-end", help="End date as YYYY-MM-DD")
    parser.add_argument("--output", type=Path, help="Write a single artifact to this path")
    parser.add_argument("--json", action="store_true", help="Hint JSON output when using --output")
    parser.add_argument("--markdown-out", type=Path, help="Write a Markdown preview artifact")
    parser.add_argument("--json-out", type=Path, help="Write a JSON preview artifact")
    parser.add_argument("--out", type=Path, help="Write one artifact, inferring format from extension")
    parser.add_argument("--subject", default="", help="Override the selected newsletter subject")
    parser.add_argument("--days", type=int, default=7, help="Trailing days to include when explicit dates are omitted")
    return parser.parse_args(argv)


def _parse_date(value: str, name: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"{name} must use YYYY-MM-DD") from exc
    return parsed.replace(tzinfo=timezone.utc)


def _default_week_range(days: int, now: datetime | None = None) -> tuple[datetime, datetime]:
    now = now or datetime.now(timezone.utc)
    week_end = datetime.combine(now.date(), time.min, tzinfo=timezone.utc)
    return week_end - timedelta(days=days), week_end


def _week_range(args: argparse.Namespace) -> tuple[datetime, datetime]:
    default_start, default_end = _default_week_range(args.days)
    week_start = _parse_date(args.week_start, "--week-start") if args.week_start else default_start
    week_end = _parse_date(args.week_end, "--week-end") if args.week_end else default_end
    if week_end <= week_start:
        raise SystemExit("--week-end must be after --week-start")
    return week_start, week_end


def _output_paths(args: argparse.Namespace) -> list[Path]:
    paths = [path for path in [args.output, args.out, args.markdown_out, args.json_out] if path]
    if not paths:
        raise SystemExit("Provide --output, --out, --markdown-out, or --json-out.")
    if args.json and args.output and args.output.suffix.lower() != ".json":
        return [args.output.with_suffix(".json")]
    return paths


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    week_start, week_end = _week_range(args)
    output_paths = _output_paths(args)

    with script_context() as (config, db):
        if not config.newsletter or not config.newsletter.enabled:
            logger.info("Newsletter not enabled, skipping preview")
            return 0

        payload = assemble_newsletter_preview(
            db,
            config,
            week_start,
            week_end,
            manual_subject=manual_subject_override(config, args.subject),
        )
        if not payload["body_markdown"].strip():
            logger.info("No content published in preview window")
            return 0

        for path in output_paths:
            write_preview_artifact(path, payload)
            logger.info("Newsletter preview written to %s", path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
