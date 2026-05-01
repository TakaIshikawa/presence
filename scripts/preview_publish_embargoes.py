#!/usr/bin/env python3
"""Preview publish embargo windows and queued items they affect."""

from __future__ import annotations

import argparse
from datetime import datetime
import sqlite3
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config  # noqa: E402
from output.publish_embargo_preview import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    build_publish_embargo_preview,
    format_publish_embargo_preview_json,
    format_publish_embargo_preview_text,
)
from runner import script_context  # noqa: E402
from storage.db import Database  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="YAML config path. Defaults to configured config.")
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS_AHEAD,
        help=f"Days ahead to preview (default: {DEFAULT_DAYS_AHEAD}).",
    )
    parser.add_argument(
        "--now",
        help="Deterministic current time as an ISO timestamp. Naive values are treated as UTC.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        now = datetime.fromisoformat(args.now) if args.now else None
        if args.config:
            config = _load_yaml_config(args.config)
            db_path = args.db or _database_path_from_config(config)
            if not db_path:
                raise ValueError("--db is required when --config has no paths.database")
            with Database(db_path) as db:
                report = build_publish_embargo_preview(
                    db,
                    config,
                    days=args.days,
                    now=now,
                )
        elif args.db:
            config = load_config()
            with Database(args.db) as db:
                report = build_publish_embargo_preview(
                    db,
                    config,
                    days=args.days,
                    now=now,
                )
        else:
            with script_context() as (config, db):
                report = build_publish_embargo_preview(
                    db,
                    config,
                    days=args.days,
                    now=now,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publish_embargo_preview_json(report))
    else:
        print(format_publish_embargo_preview_text(report))
    return 0


def _load_yaml_config(path: str) -> dict:
    with open(path, "r") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("config file must contain a YAML mapping")
    return data


def _database_path_from_config(config: dict) -> str:
    paths = config.get("paths")
    if isinstance(paths, dict):
        return str(paths.get("database") or "")
    return ""


if __name__ == "__main__":
    raise SystemExit(main())
