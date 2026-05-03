#!/usr/bin/env python3
"""Report Claude Code working-directory drift from parsed session events."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.claude_session_cwd_drift import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_CWD_CHANGES,
    build_claude_session_cwd_drift_report,
    format_claude_session_cwd_drift_json,
    format_claude_session_cwd_drift_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--project-root",
        help="Configured project root. Defaults to the first paths.allowed_projects entry.",
    )
    parser.add_argument(
        "--min-cwd-changes",
        type=_positive_int,
        default=DEFAULT_MIN_CWD_CHANGES,
        help=(
            "Minimum adjacent cwd changes before in-project movement is reported "
            f"(default: {DEFAULT_MIN_CWD_CHANGES})."
        ),
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum session rows to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="json",
        help="Output format (default: json).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        if args.db:
            project_root = args.project_root
            if not project_root:
                raise ValueError("--project-root is required with --db")
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_claude_session_cwd_drift_report(
                    conn,
                    project_root=project_root,
                    min_cwd_changes=args.min_cwd_changes,
                    limit=args.limit,
                )
        else:
            with script_context() as (config, db):
                project_root = args.project_root or _configured_project_root(config)
                report = build_claude_session_cwd_drift_report(
                    db,
                    project_root=project_root,
                    min_cwd_changes=args.min_cwd_changes,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "text":
        print(format_claude_session_cwd_drift_text(report))
    else:
        print(format_claude_session_cwd_drift_json(report))
    return 0


def _configured_project_root(config: object) -> str:
    allowed = getattr(getattr(config, "paths", None), "allowed_projects", None)
    if allowed:
        return str(allowed[0])
    raise ValueError("--project-root is required when paths.allowed_projects is empty")


if __name__ == "__main__":
    raise SystemExit(main())
