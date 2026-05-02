#!/usr/bin/env python3
"""Report repeated blog excerpts across recent drafts or posts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_excerpt_duplication import (  # noqa: E402
    DEFAULT_SIMILARITY_THRESHOLD,
    build_blog_excerpt_duplication_report,
    format_blog_excerpt_duplication_json,
    format_blog_excerpt_duplication_text,
    load_blog_posts_from_paths,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Markdown blog draft/post files. Defaults to --draft-dir/*.md when omitted.",
    )
    parser.add_argument(
        "--draft-dir",
        type=Path,
        default=Path("drafts"),
        help="Directory of markdown drafts to scan when no paths are supplied (default: drafts).",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Optional draft manifest JSON containing entries with draft_path.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        help="Only compare posts published within this many days; undated posts are retained.",
    )
    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=DEFAULT_SIMILARITY_THRESHOLD,
        help=(
            "Minimum normalized excerpt similarity to cluster "
            f"(default: {DEFAULT_SIMILARITY_THRESHOLD:g})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Alias for --format json.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.json:
        args.format = "json"

    try:
        paths = _blog_paths(args.paths, args.draft_dir, args.manifest)
        records = load_blog_posts_from_paths(paths)
        source = _source_label(paths, args.draft_dir, args.manifest, explicit=bool(args.paths))
        report = build_blog_excerpt_duplication_report(
            records,
            similarity_threshold=args.similarity_threshold,
            lookback_days=args.lookback_days,
            source=source,
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_blog_excerpt_duplication_json(report))
    else:
        print(format_blog_excerpt_duplication_text(report))
    return 1 if report.cluster_count else 0


def _blog_paths(
    paths: list[Path],
    draft_dir: Path,
    manifest_path: Path | None,
) -> list[Path]:
    if paths:
        return sorted(paths)
    if manifest_path is not None:
        manifest = json.loads(manifest_path.read_text())
        base = (
            manifest_path.parent.parent
            if manifest_path.parent.name == "drafts"
            else draft_dir.parent
        )
        manifest_paths: list[Path] = []
        for entry in manifest.get("drafts", []):
            draft_path = entry.get("draft_path")
            if not draft_path:
                continue
            path = Path(draft_path)
            manifest_paths.append(path if path.is_absolute() else base / path)
        return sorted(manifest_paths)
    return sorted(draft_dir.glob("*.md"))


def _source_label(
    paths: list[Path],
    draft_dir: Path,
    manifest_path: Path | None,
    *,
    explicit: bool,
) -> str:
    if explicit:
        return "paths:" + ",".join(str(path) for path in paths)
    if manifest_path is not None:
        return str(manifest_path)
    return str(draft_dir)


if __name__ == "__main__":
    raise SystemExit(main())
