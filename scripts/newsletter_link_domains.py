#!/usr/bin/env python3
"""Report newsletter outbound link domain mix before delivery."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_link_domains import (  # noqa: E402
    DEFAULT_DOMINANT_SHARE,
    build_newsletter_link_domain_report,
    format_newsletter_link_domain_json,
    format_newsletter_link_domain_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "draft",
        nargs="?",
        default="-",
        help="HTML or Markdown newsletter draft file, or '-' for stdin (default: stdin).",
    )
    parser.add_argument(
        "--text",
        help="HTML or Markdown newsletter draft text to report without reading a file.",
    )
    parser.add_argument(
        "--preferred-domain",
        action="append",
        default=[],
        help="Preferred/internal domain. May be supplied more than once.",
    )
    parser.add_argument(
        "--dominant-share-threshold",
        type=float,
        default=DEFAULT_DOMINANT_SHARE,
        help=(
            "Minimum share for a domain to be reported as dominant "
            f"(default: {DEFAULT_DOMINANT_SHARE:g})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="json",
        help="Output format (default: json).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.text is not None and args.draft != "-":
            raise ValueError("--text cannot be combined with a draft file")
        text = args.text if args.text is not None else _read_text_arg(args.draft)
        report = build_newsletter_link_domain_report(
            text,
            preferred_domains=args.preferred_domain,
            source="text" if args.text is not None else ("stdin" if args.draft == "-" else args.draft),
            dominant_share_threshold=args.dominant_share_threshold,
        )
    except (OSError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "text":
        print(format_newsletter_link_domain_text(report))
    else:
        print(format_newsletter_link_domain_json(report))
    return 0


def _read_text_arg(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    return Path(path).read_text()


if __name__ == "__main__":
    raise SystemExit(main())
