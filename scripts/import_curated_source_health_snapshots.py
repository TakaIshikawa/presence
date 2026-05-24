#!/usr/bin/env python3
"""Import curated source health snapshots."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.curated_source_health_snapshot_import import format_curated_source_health_snapshot_import_json, format_curated_source_health_snapshot_import_text, import_curated_source_health_snapshots  # noqa: E402
from runner import script_context  # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--input", required=True)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--format", choices=("json", "text"), default="json")
    return p.parse_args(argv)


def main(argv=None) -> int:
    try:
        a = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if a.db:
            with sqlite3.connect(a.db) as conn:
                conn.row_factory = sqlite3.Row
                summary = import_curated_source_health_snapshots(conn, a.input, dry_run=a.dry_run)
        else:
            with script_context() as (_config, db):
                summary = import_curated_source_health_snapshots(db.conn, a.input, dry_run=a.dry_run)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_curated_source_health_snapshot_import_text(summary) if a.format == "text" else format_curated_source_health_snapshot_import_json(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
