#!/usr/bin/env python3
"""Export generated content as Threads review artifacts."""

from __future__ import annotations

import argparse
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.threads_export import (  # noqa: E402
    ThreadsExportError,
    build_threads_exports_from_db,
    threads_exports_to_json,
    write_threads_json,
)
from runner import SCHEMA_PATH, script_context  # noqa: E402
from storage.db import Database  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, help="SQLite database path")
    parser.add_argument("--content-id", type=int, help="generated_content id to export")
    parser.add_argument("--status", help="publish_queue status to export")
    parser.add_argument("--limit", type=int, default=20, help="Maximum artifacts to export")
    parser.add_argument("--output", type=Path, help="JSON artifact output path")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Write JSON to stdout instead of an output file",
    )
    return parser.parse_args(argv)


@contextmanager
def _db_context(db_path: Path | None) -> Iterator[tuple[object | None, object]]:
    if db_path is None:
        with script_context() as context:
            yield context
        return

    db = Database(str(db_path))
    db.connect()
    db.init_schema(SCHEMA_PATH)
    try:
        yield None, db
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.json and args.output:
        print("error: pass either --json or --output, not both", file=sys.stderr)
        return 1
    if not args.json and not args.output:
        args.json = True

    try:
        with _db_context(args.db) as (_config, db):
            exports = build_threads_exports_from_db(
                db,
                content_id=args.content_id,
                status=args.status,
                limit=args.limit,
            )
    except ThreadsExportError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(threads_exports_to_json(exports))
    else:
        write_threads_json(exports, args.output)
        print(f"Threads artifact: {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
