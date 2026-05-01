#!/usr/bin/env python3
"""Check newsletter links before Buttondown delivery."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_link_health import (  # noqa: E402
    DEFAULT_TIMEOUT,
    check_newsletter_links,
    format_newsletter_link_health_json,
    format_newsletter_link_health_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--body-file", help="Plaintext newsletter body file.")
    parser.add_argument("--html-file", help="HTML newsletter body file.")
    parser.add_argument("--subject", default="", help="Newsletter subject.")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout in seconds per link (default: {DEFAULT_TIMEOUT:g}).",
    )
    parser.add_argument(
        "--require-utm",
        action="store_true",
        help="Classify HTTP links missing utm_source, utm_medium, or utm_campaign.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        body = Path(args.body_file).read_text() if args.body_file else ""
        html = Path(args.html_file).read_text() if args.html_file else ""
        report = check_newsletter_links(
            subject=args.subject,
            body=body,
            html=html,
            timeout=args.timeout,
            require_utm=args.require_utm,
        )
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_link_health_json(report))
    else:
        print(format_newsletter_link_health_text(report))

    return 1 if report.broken_required_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
