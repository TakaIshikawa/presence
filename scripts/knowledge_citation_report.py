#!/usr/bin/env python3
"""Report generated content that used knowledge without traceable citations."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context

logger = logging.getLogger(__name__)


def _bool_text(value: object) -> str:
    return "yes" if bool(value) else "no"


def _shorten(value: str | None, limit: int = 80) -> str:
    text = (value or "").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def build_report_payload(db, days: int, only_missing: bool) -> dict:
    rows = db.get_knowledge_citation_report(days=days, only_missing=only_missing)
    coverage = db.get_knowledge_citation_coverage(days=days)
    return {
        "days": days,
        "only_missing": only_missing,
        "coverage": coverage,
        "rows": rows,
    }


def format_json_report(payload: dict) -> str:
    return json.dumps(payload, indent=2, default=str)


def format_text_report(payload: dict) -> str:
    rows = payload["rows"]
    coverage = payload["coverage"]
    lines = [
        f"Knowledge Citation Report (last {payload['days']} days)",
        (
            "Coverage: "
            f"{coverage['knowledge_link_count']} knowledge links, "
            f"{coverage['external_link_count']} external, "
            f"{coverage['missing_traceable_link_count']} missing traceable links "
            f"across {coverage['content_with_missing_traceable_links']} generated rows"
        ),
    ]
    if payload["only_missing"]:
        lines.append("Filter: only missing traceable links")

    if not rows:
        lines.append("\nNo matching knowledge-linked generated content found.")
        return "\n".join(lines)

    current_content_id = None
    for row in rows:
        if row["content_id"] != current_content_id:
            current_content_id = row["content_id"]
            lines.append("")
            lines.append(
                f"Content #{row['content_id']} "
                f"[{row['content_type']}] @ {row['generated_at']}"
            )
            lines.append(f"  {_shorten(row['content'], 120)}")

        status = "MISSING" if row["missing_traceable_link"] else "ok"
        url = row["source_url"] or "-"
        license_value = row["license"] or "-"
        author = row["author"] or "-"
        lines.append(
            f"  - link #{row['link_id']} knowledge #{row['knowledge_id']} "
            f"[{status}] source={row['source_type']} author={author} "
            f"url={url} license={license_value} "
            f"attribution_required={_bool_text(row['attribution_required'])} "
            f"relevance={row['relevance_score']}"
        )

    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of generated-content days to inspect (default: 30)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Show only external knowledge links missing source_url",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.days < 1:
        raise SystemExit("--days must be at least 1")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        payload = build_report_payload(db, args.days, args.only_missing)

    if args.format == "json":
        print(format_json_report(payload))
    else:
        print(format_text_report(payload))


if __name__ == "__main__":
    main()
