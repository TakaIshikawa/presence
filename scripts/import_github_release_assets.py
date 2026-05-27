#!/usr/bin/env python3
"""Import GitHub release asset snapshots."""
from __future__ import annotations

import argparse, sqlite3, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from ingestion.github_release_asset_import import format_github_release_asset_import_json, format_github_release_asset_import_text, import_github_release_assets  # noqa:E402
from runner import script_context  # noqa:E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--input", required=True)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--format", choices=("json", "text"), default="json")
    return p.parse_args(argv)


def main(argv=None):
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                summary = import_github_release_assets(conn, args.input, dry_run=args.dry_run)
        else:
            with script_context() as (_config, db):
                summary = import_github_release_assets(db.conn, args.input, dry_run=args.dry_run)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_github_release_asset_import_text(summary) if args.format == "text" else format_github_release_asset_import_json(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
