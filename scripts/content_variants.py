#!/usr/bin/env python3
"""Manage generated content platform variants without direct SQL edits."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context


def _parse_metadata_json(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"invalid metadata JSON: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("metadata JSON must be an object")
    return parsed


def _shorten(value: Any, width: int) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def format_variant_rows(rows: list[dict]) -> str:
    if not rows:
        return "No content variants found."

    columns = [
        ("id", "ID", 5),
        ("content_id", "CID", 5),
        ("platform", "PLATFORM", 10),
        ("variant_type", "TYPE", 12),
        ("selected", "SEL", 3),
        ("content", "CONTENT", 56),
    ]
    lines = [
        "  ".join(label.ljust(width) for _, label, width in columns),
        "  ".join("-" * width for _, _, width in columns),
    ]
    for row in rows:
        display = dict(row)
        display["selected"] = "yes" if row.get("selected") else "no"
        lines.append(
            "  ".join(
                _shorten(display.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def format_variant_detail(row: dict) -> str:
    metadata = json.dumps(row.get("metadata") or {}, sort_keys=True)
    lines = [
        f"id: {row['id']}",
        f"content_id: {row['content_id']}",
        f"platform: {row['platform']}",
        f"variant_type: {row['variant_type']}",
        f"selected: {'yes' if row.get('selected') else 'no'}",
        f"metadata: {metadata}",
        "content:",
        row["content"],
    ]
    return "\n".join(lines)


def _require_content(db, content_id: int) -> None:
    if not db.get_generated_content(content_id):
        raise ValueError(f"generated_content id {content_id} does not exist")


def _require_variant(db, content_id: int, platform: str, variant_type: str) -> dict:
    _require_content(db, content_id)
    variant = db.get_content_variant(content_id, platform, variant_type)
    if not variant:
        raise ValueError(
            "content variant does not exist for "
            f"content_id={content_id}, platform={platform}, variant_type={variant_type}"
        )
    return variant


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List variants for content")
    list_parser.add_argument("--content-id", type=int, required=True)
    list_parser.add_argument("--platform", help="Optional platform filter")

    show_parser = subparsers.add_parser("show", help="Show one content variant")
    show_parser.add_argument("--content-id", type=int, required=True)
    show_parser.add_argument("--platform", required=True)
    show_parser.add_argument("--variant-type", required=True)

    add_parser = subparsers.add_parser("add", help="Create or update a variant")
    add_parser.add_argument("--content-id", type=int, required=True)
    add_parser.add_argument("--platform", required=True)
    add_parser.add_argument("--variant-type", required=True)
    add_parser.add_argument("--text", required=True)
    add_parser.add_argument(
        "--metadata-json",
        type=_parse_metadata_json,
        default=None,
        help='Optional JSON object, for example {"source":"operator"}',
    )

    select_parser = subparsers.add_parser("select", help="Select a platform variant")
    select_parser.add_argument("--content-id", type=int, required=True)
    select_parser.add_argument("--platform", required=True)
    select_parser.add_argument("--variant-type", required=True)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            if args.command == "list":
                _require_content(db, args.content_id)
                rows = db.list_content_variants(args.content_id)
                if args.platform:
                    rows = [row for row in rows if row["platform"] == args.platform]
                print(format_variant_rows(rows))
            elif args.command == "show":
                row = _require_variant(
                    db, args.content_id, args.platform, args.variant_type
                )
                print(format_variant_detail(row))
            elif args.command == "add":
                variant_id = db.upsert_content_variant(
                    content_id=args.content_id,
                    platform=args.platform,
                    variant_type=args.variant_type,
                    content=args.text,
                    metadata=args.metadata_json or {},
                )
                row = db.get_content_variant(
                    args.content_id,
                    args.platform,
                    args.variant_type,
                )
                row["id"] = variant_id
                print(format_variant_detail(row))
            elif args.command == "select":
                row = db.select_content_variant(
                    args.content_id,
                    args.platform,
                    args.variant_type,
                )
                print(format_variant_detail(row))
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
