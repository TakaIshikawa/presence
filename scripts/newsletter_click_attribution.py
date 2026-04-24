#!/usr/bin/env python3
"""Report Buttondown newsletter link clicks attributed to source content."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_click_attribution import (  # noqa: E402
    NewsletterClickAttribution,
    format_newsletter_click_attribution_json,
    format_newsletter_click_attribution_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Look back this many days by newsletter send time (default: 90)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output stable machine-readable JSON",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        summary = NewsletterClickAttribution(db).summarize(days=args.days)

    if args.json:
        print(format_newsletter_click_attribution_json(summary))
    else:
        print(format_newsletter_click_attribution_text(summary))


if __name__ == "__main__":
    main(sys.argv[1:])
