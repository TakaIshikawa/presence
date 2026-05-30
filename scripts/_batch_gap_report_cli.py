"""Shared CLI plumbing for batch gap reports."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def probability(value: str) -> float:
    parsed = non_negative_float(value)
    if parsed > 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def run(argv: list[str] | None, *, description: str, builder: Callable[..., dict[str, Any]], json_formatter: Callable[[dict[str, Any]], str], text_formatter: Callable[[dict[str, Any]], str], options: list[tuple[str, Callable[[str], Any], Any]]) -> int:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    for name, value_type, default in options:
        parser.add_argument(f"--{name.replace('_', '-')}", type=value_type, default=default)
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {name: getattr(args, name) for name, _type, _default in options}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = builder(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = builder(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(text_formatter(report) if args.format == "text" else json_formatter(report))
    return 0
