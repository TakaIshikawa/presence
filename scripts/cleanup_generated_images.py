#!/usr/bin/env python3
"""Clean unreferenced generated image files older than a configured age."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = PROJECT_ROOT / "schema.sql"
IMAGE_SUFFIXES = {".gif", ".jpeg", ".jpg", ".png", ".webp"}


@dataclass
class CleanupResult:
    image_dir: str
    cutoff: str
    days: float
    dry_run: bool
    scanned: int
    referenced: int
    old_unreferenced: list[str]
    deleted: list[str]
    errors: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=float,
        default=30,
        help="Delete candidates older than this many days (default: 30).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report files that would be deleted without deleting them.",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete old unreferenced files. Without this flag the command is a dry run.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON output.",
    )
    parser.add_argument(
        "--db-path",
        help="SQLite database path (default: paths.database from config).",
    )
    parser.add_argument(
        "--image-dir",
        help="Generated image directory (default: image_gen.output_dir from config).",
    )
    return parser.parse_args()


def _resolve_path(path: Path) -> Path:
    return path.expanduser().resolve(strict=False)


def _iter_image_files(image_dir: Path) -> Iterable[Path]:
    for path in image_dir.rglob("*"):
        if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES:
            yield path


def _referenced_image_paths(conn: sqlite3.Connection, image_dir: Path) -> set[Path]:
    rows = conn.execute(
        """SELECT image_path
           FROM generated_content
           WHERE image_path IS NOT NULL AND TRIM(image_path) != ''"""
    ).fetchall()

    referenced: set[Path] = set()
    for row in rows:
        raw = row["image_path"] if isinstance(row, sqlite3.Row) else row[0]
        raw_path = Path(str(raw)).expanduser()
        if raw_path.is_absolute():
            referenced.add(_resolve_path(raw_path))
        else:
            referenced.add(_resolve_path(PROJECT_ROOT / raw_path))
            referenced.add(_resolve_path(Path.cwd() / raw_path))
            referenced.add(_resolve_path(image_dir / raw_path))
    return referenced


def cleanup_generated_images(
    db_path: str | Path,
    image_dir: str | Path,
    days: float = 30,
    delete: bool = False,
    now: datetime | None = None,
) -> CleanupResult:
    if days < 0:
        raise ValueError("--days must be non-negative")

    image_dir_path = _resolve_path(Path(image_dir))
    if not image_dir_path.exists():
        raise FileNotFoundError(f"Image directory does not exist: {image_dir_path}")
    if not image_dir_path.is_dir():
        raise NotADirectoryError(f"Image path is not a directory: {image_dir_path}")

    current_time = now or datetime.now(timezone.utc)
    cutoff_timestamp = current_time.timestamp() - (days * 24 * 60 * 60)
    cutoff = datetime.fromtimestamp(cutoff_timestamp, tz=timezone.utc)

    db = Database(str(db_path))
    db.connect()
    try:
        db.init_schema(str(SCHEMA_PATH))
        referenced = _referenced_image_paths(db.conn, image_dir_path)
    finally:
        db.close()

    scanned = 0
    old_unreferenced: list[str] = []
    deleted: list[str] = []
    errors: list[str] = []

    for path in sorted(_iter_image_files(image_dir_path)):
        scanned += 1
        resolved = _resolve_path(path)
        if resolved in referenced:
            continue
        try:
            if path.stat().st_mtime >= cutoff_timestamp:
                continue
        except OSError as e:
            errors.append(f"{path}: {e}")
            continue

        old_unreferenced.append(str(path))
        if delete:
            try:
                path.unlink()
                deleted.append(str(path))
            except OSError as e:
                errors.append(f"{path}: {e}")

    return CleanupResult(
        image_dir=str(image_dir_path),
        cutoff=cutoff.isoformat(),
        days=days,
        dry_run=not delete,
        scanned=scanned,
        referenced=len(referenced),
        old_unreferenced=old_unreferenced,
        deleted=deleted,
        errors=errors,
    )


def _default_paths(args: argparse.Namespace) -> tuple[str, str]:
    if args.db_path and args.image_dir:
        return args.db_path, args.image_dir

    config = load_config()
    db_path = args.db_path or config.paths.database
    image_dir = args.image_dir
    if image_dir is None:
        image_dir = config.image_gen.output_dir if config.image_gen else "generated_images"
    return db_path, image_dir


def _print_text(result: CleanupResult) -> None:
    action = "Would delete" if result.dry_run else "Deleted"
    count = len(result.old_unreferenced) if result.dry_run else len(result.deleted)
    print(f"Scanned {result.scanned} image files in {result.image_dir}")
    print(f"{action} {count} old unreferenced image files")
    for path in result.old_unreferenced:
        marker = "DRY-RUN" if result.dry_run else "DELETED"
        print(f"{marker} {path}")
    for error in result.errors:
        print(f"ERROR {error}", file=sys.stderr)


def main() -> int:
    args = parse_args()
    if args.dry_run and args.delete:
        raise SystemExit("--dry-run and --delete cannot be used together")

    db_path, image_dir = _default_paths(args)
    result = cleanup_generated_images(
        db_path=db_path,
        image_dir=image_dir,
        days=args.days,
        delete=args.delete,
    )

    if args.json:
        print(json.dumps(asdict(result), indent=2, sort_keys=True))
    else:
        _print_text(result)

    return 1 if result.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
