#!/usr/bin/env python3
"""Decorate outbound artifact links with UTM parameters."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.link_tracking import decorate_links  # noqa: E402
from storage.db import Database  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--content-id", type=int, help="generated_content id to decorate")
    parser.add_argument(
        "--db-path",
        type=Path,
        help="SQLite database path for --content-id; defaults to configured script context",
    )
    parser.add_argument("--utm-source", help="utm_source value to append")
    parser.add_argument("--utm-medium", help="utm_medium value to append")
    parser.add_argument("--utm-campaign", help="utm_campaign value to append")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace existing UTM parameter values instead of preserving them",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit transformed content and link diagnostics as JSON",
    )
    return parser.parse_args(argv)


def _load_content_from_db_path(db_path: Path, content_id: int) -> str:
    db = Database(str(db_path))
    db.connect()
    try:
        row = db.get_generated_content(content_id)
    finally:
        db.close()
    if row is None:
        raise ValueError(f"Content ID {content_id} not found")
    return str(row.get("content") or "")


def _load_content_from_configured_db(content_id: int) -> str:
    from runner import script_context

    with script_context() as (_config, db):
        row = db.get_generated_content(content_id)
    if row is None:
        raise ValueError(f"Content ID {content_id} not found")
    return str(row.get("content") or "")


def load_content(args: argparse.Namespace) -> str:
    if args.content_id is None:
        return sys.stdin.read()
    if args.db_path:
        return _load_content_from_db_path(args.db_path, args.content_id)
    return _load_content_from_configured_db(args.content_id)


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.WARNING)

    try:
        content = load_content(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    result = decorate_links(
        content,
        utm_source=args.utm_source,
        utm_medium=args.utm_medium,
        utm_campaign=args.utm_campaign,
        replace=args.replace,
    )

    if args.json:
        print(
            json.dumps(
                result.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                default=_json_default,
            )
        )
    else:
        print(result.content, end="" if result.content.endswith("\n") else "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
