#!/usr/bin/env python3
"""Scan generated drafts for contradictions with linked knowledge."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.knowledge_contradictions import scan_content_id, scan_recent_unpublished


def build_scan_payload(db, *, content_id: int | None, recent_days: int) -> dict:
    """Return contradiction scan payload for one content item or recent drafts."""

    if content_id is not None:
        warnings = scan_content_id(db, content_id)
        return {
            "content_id": content_id,
            "recent_days": None,
            "warning_count": len(warnings),
            "warnings": [warning.to_dict() for warning in warnings],
        }

    results = scan_recent_unpublished(db, recent_days=recent_days)
    rows = [
        {
            "content_id": content_id,
            "warning_count": len(warnings),
            "warnings": [warning.to_dict() for warning in warnings],
        }
        for content_id, warnings in sorted(results.items())
    ]
    return {
        "content_id": None,
        "recent_days": recent_days,
        "warning_count": sum(row["warning_count"] for row in rows),
        "content_count": len(rows),
        "rows": rows,
    }


def format_json_scan(payload: dict) -> str:
    """Format scan results as stable JSON."""

    return json.dumps(payload, indent=2, sort_keys=True, default=str)


def format_text_scan(payload: dict) -> str:
    """Format scan results for humans."""

    lines = ["Knowledge contradiction scan"]
    if payload.get("content_id") is not None:
        lines.append(f"Content: #{payload['content_id']}")
        rows = [
            {
                "content_id": payload["content_id"],
                "warnings": payload.get("warnings", []),
            }
        ]
    else:
        lines.append(f"Recent unpublished: last {payload['recent_days']} days")
        rows = payload.get("rows", [])

    lines.append(f"Warnings: {payload['warning_count']}")
    if payload["warning_count"] == 0:
        lines.append("No obvious contradictions found.")
        return "\n".join(lines)

    for row in rows:
        if not row.get("warnings"):
            continue
        lines.append("")
        lines.append(f"Content #{row['content_id']}")
        for warning in row["warnings"]:
            lines.append(
                f"- {warning['kind']} knowledge #{warning['knowledge_id']}: "
                f"{warning['claim_value']} conflicts with {warning['evidence_value']}"
            )
            lines.append(f"  claim: {warning['claim']}")
            lines.append(f"  evidence: {warning['evidence']}")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--content-id", type=int, help="Scan one generated_content id")
    target.add_argument(
        "--recent-days",
        type=int,
        default=7,
        help="Scan recent unpublished content (default: 7)",
    )
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.content_id is not None and args.content_id < 1:
        raise SystemExit("--content-id must be at least 1")
    if args.recent_days < 1:
        raise SystemExit("--recent-days must be at least 1")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        try:
            payload = build_scan_payload(
                db,
                content_id=args.content_id,
                recent_days=args.recent_days,
            )
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

    if args.json:
        print(format_json_scan(payload))
    else:
        print(format_text_scan(payload))


if __name__ == "__main__":
    main()
