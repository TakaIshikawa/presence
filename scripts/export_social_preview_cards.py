#!/usr/bin/env python3
"""Export Open Graph and Twitter-card metadata for generated content."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.social_preview_cards import (  # noqa: E402
    DEFAULT_CONTENT_TYPES,
    SocialPreviewCardError,
    build_social_preview_cards,
    social_preview_cards_to_json,
    social_preview_cards_to_jsonl,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of published generated_content rows to export.",
    )
    parser.add_argument(
        "--content-type",
        choices=DEFAULT_CONTENT_TYPES,
        help="Only export one eligible generated_content content type.",
    )
    parser.add_argument(
        "--output",
        choices=["json", "jsonl"],
        default="json",
        help="Output format.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and render records without publishing or mutating the database.",
    )
    return parser.parse_args(argv)


def fetch_preview_rows(
    db: Any,
    *,
    limit: int,
    content_type: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch eligible generated_content rows for social preview export."""
    if limit <= 0:
        raise SocialPreviewCardError("limit must be positive")

    params: list[Any] = []
    if content_type:
        content_filter = "gc.content_type = ?"
        params.append(content_type)
    else:
        placeholders = ", ".join("?" for _ in DEFAULT_CONTENT_TYPES)
        content_filter = f"gc.content_type IN ({placeholders})"
        params.extend(DEFAULT_CONTENT_TYPES)

    params.append(limit)
    rows = db.conn.execute(
        f"""SELECT gc.id,
                  gc.content_type,
                  gc.content,
                  gc.published_url,
                  gc.published_at,
                  gc.image_path,
                  gc.image_alt_text,
                  gc.created_at
           FROM generated_content gc
           WHERE gc.published = 1
             AND {content_filter}
           ORDER BY datetime(gc.published_at) DESC,
                    datetime(gc.created_at) DESC,
                    gc.id DESC
           LIMIT ?""",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def render_preview_cards(args: argparse.Namespace) -> str:
    with script_context() as (_config, db):
        rows = fetch_preview_rows(
            db,
            limit=args.limit,
            content_type=args.content_type,
        )

    cards = build_social_preview_cards(rows)
    if args.dry_run:
        print(
            f"Dry run: exported {len(cards)} social preview card(s); database was not mutated.",
            file=sys.stderr,
        )

    if args.output == "jsonl":
        payload = social_preview_cards_to_jsonl(cards)
        return payload + ("\n" if payload else "")
    return social_preview_cards_to_json(cards) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        print(render_preview_cards(args), end="")
    except SocialPreviewCardError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
