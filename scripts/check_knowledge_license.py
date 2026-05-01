#!/usr/bin/env python3
"""Check generated content knowledge licenses before publishing."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.knowledge_license_blocker import check_knowledge_license  # noqa: E402
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--content-id",
        type=int,
        required=True,
        help="generated_content.id to inspect",
    )
    parser.add_argument(
        "--platform",
        default="unknown",
        help="Publication platform for report context",
    )
    parser.add_argument("--json", action="store_true", help="Print stable JSON output")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Block attribution-required knowledge when source_url is missing",
    )
    return parser.parse_args(argv)


def format_text_report(report: dict) -> str:
    lines = [
        (
            f"Knowledge license check: {report['status']} "
            f"(content #{report['content_id']} on {report['platform']})"
        ),
        f"Linked knowledge: {report['linked_knowledge_count']}",
    ]
    if report["findings"]:
        lines.append("")
        lines.append("Findings:")
        for finding in report["findings"]:
            source = finding.get("source_url") or finding.get("source_id") or "-"
            lines.append(
                f"- {finding['severity']}: {finding['kind']} "
                f"knowledge #{finding['knowledge_id']} "
                f"license={finding['license']} source={source}"
            )
            lines.append(f"  {finding['message']}")
    if report["attribution_groups"]:
        lines.append("")
        lines.append("Required attribution:")
        for group in report["attribution_groups"]:
            lines.append(f"- {group['source_url']}")
            for snippet in group["snippets"]:
                author = snippet.get("author") or "unknown author"
                lines.append(
                    f"  knowledge #{snippet['knowledge_id']} ({author}): "
                    f"{snippet['snippet']}"
                )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    with script_context() as (_config, db):
        report = check_knowledge_license(
            db,
            args.content_id,
            platform=args.platform,
            strict=args.strict,
        ).as_dict()

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(format_text_report(report))
    return 1 if report["blocked"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
