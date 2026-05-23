"""Shared CLI runner for focused SQLite evaluation reports."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from typing import Any, Callable

from runner import script_context


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def run(
    args: argparse.Namespace,
    build_from_db: Callable[..., dict[str, Any]],
    format_json: Callable[[dict[str, Any]], str],
    format_text: Callable[[dict[str, Any]], str],
    kwargs: dict[str, Any],
) -> int:
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_text(report) if args.format == "text" else format_json(report))
    return 0
