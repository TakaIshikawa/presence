#!/usr/bin/env python3
"""Export a local newsletter preview without sending through Buttondown."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_preview import (  # noqa: E402
    NewsletterPreviewOptions,
    assemble_newsletter_preview,
    build_newsletter_preview,
    format_preview_json,
    manual_subject_override,
    write_newsletter_preview,
    write_preview_artifact,
)
from runner import script_context  # noqa: E402


logger = logging.getLogger(__name__)


def _parse_date(value: str, name: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"{name} must use YYYY-MM-DD") from exc
    return parsed.replace(tzinfo=timezone.utc)


def _default_week_range(days: int = 7) -> tuple[datetime, datetime]:
    week_end = datetime.combine(
        datetime.now(timezone.utc).date(),
        time.min,
        tzinfo=timezone.utc,
    )
    return week_end - timedelta(days=days), week_end


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--week-start", help="Inclusive preview start date in YYYY-MM-DD format.")
    parser.add_argument("--week-end", help="Exclusive preview end date in YYYY-MM-DD format.")
    parser.add_argument("--output", help="Path to write the preview artifact.")
    parser.add_argument("--out", type=Path, help="Write one artifact, inferring format from extension.")
    parser.add_argument("--markdown-out", type=Path, help="Write a Markdown preview artifact.")
    parser.add_argument("--json-out", type=Path, help="Write a JSON preview artifact.")
    parser.add_argument("--json", action="store_true", help="Write structured JSON.")
    parser.add_argument("--include-metadata", action="store_true", help="Include assembler metadata.")
    parser.add_argument("--subject", default="", help="Override the selected newsletter subject.")
    parser.add_argument("--days", type=int, default=7, help="Trailing days to include when explicit dates are omitted.")
    return parser.parse_args(argv or [])


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    default_start, default_end = _default_week_range(args.days)
    week_start = _parse_date(args.week_start, "--week-start") if args.week_start else default_start
    week_end = _parse_date(args.week_end, "--week-end") if args.week_end else default_end
    if week_start >= week_end:
        raise SystemExit("--week-start must be earlier than --week-end")

    output_paths = [path for path in [args.out, args.markdown_out, args.json_out] if path]
    if args.output:
        output_paths.append(Path(args.output))
    if not output_paths:
        raise SystemExit("Provide --output, --out, --markdown-out, or --json-out.")

    with script_context() as (config, db):
        newsletter_config = getattr(config, "newsletter", None)
        preview = build_newsletter_preview(
            db,
            week_start,
            week_end,
            NewsletterPreviewOptions(
                site_url=getattr(newsletter_config, "site_url", "https://takaishikawa.com"),
                utm_source=getattr(newsletter_config, "utm_source", ""),
                utm_medium=getattr(newsletter_config, "utm_medium", ""),
                utm_campaign_template=getattr(newsletter_config, "utm_campaign_template", ""),
                manual_subject=manual_subject_override(config, args.subject),
                include_metadata=args.include_metadata,
            ),
        )
        compatibility_payload = assemble_newsletter_preview(
            db,
            config,
            week_start,
            week_end,
            manual_subject=manual_subject_override(config, args.subject),
        )

    for path in output_paths:
        suffix = path.suffix.lower()
        if path == args.json_out or (args.json and suffix == ".json"):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                format_preview_json(compatibility_payload),
                encoding="utf-8",
            )
        elif path == args.markdown_out or suffix in {".md", ".markdown"}:
            write_preview_artifact(path, compatibility_payload)
        else:
            write_newsletter_preview(
                preview,
                path,
                json_mode=args.json,
                include_metadata=args.include_metadata,
            )
        logger.info("Newsletter preview written to %s", path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
