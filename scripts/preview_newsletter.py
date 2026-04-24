#!/usr/bin/env python3
"""Export reviewable newsletter draft artifacts without sending email."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
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
    parser.add_argument(
        "--markdown-out",
        type=Path,
        help="Write a Markdown preview artifact to this path.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        help="Write a JSON preview artifact to this path.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="Write one preview artifact, with format inferred from the extension.",
    )
    parser.add_argument(
        "--subject",
        default="",
        help="Override the selected newsletter subject for this preview.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of trailing days to include in the preview window.",
    )
    return parser.parse_args(argv)


def _output_paths(args: argparse.Namespace) -> list[Path]:
    paths = [path for path in [args.out, args.markdown_out, args.json_out] if path]
    if not paths:
        raise ValueError("Provide --out, --markdown-out, or --json-out.")
    return paths


def _week_range(days: int) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    week_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return week_end - timedelta(days=days), week_end


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        output_paths = _output_paths(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    with script_context() as (config, db):
        if not config.newsletter or not config.newsletter.enabled:
            logger.info("Newsletter not enabled, skipping preview")
            return 0

        week_start, week_end = _week_range(args.days)
        logger.info(
            "Assembling newsletter preview for %s to %s",
            week_start.date(),
            week_end.date(),
        )
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
