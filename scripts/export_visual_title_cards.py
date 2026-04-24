#!/usr/bin/env python3
"""Export deterministic visual post title-card metadata."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.visual_title_cards import (  # noqa: E402
    DEFAULT_MAX_TITLE_CHARS,
    VisualTitleCardError,
    build_recent_visual_title_cards_from_db,
    build_visual_title_card_from_artifact,
    build_visual_title_card_from_db,
    visual_title_card_filename,
    visual_title_cards_to_json,
    write_visual_title_card_artifact,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--content-id", type=int, help="generated_content id to export")
    target.add_argument(
        "--preview-artifact",
        type=Path,
        help="Path to a visual post dry-run review artifact JSON",
    )
    parser.add_argument(
        "--recent",
        type=int,
        default=5,
        help="Number of recent visual posts to export when --content-id is omitted",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        help="Directory for per-content JSON artifacts. Defaults to stdout.",
    )
    parser.add_argument(
        "--max-title-chars",
        type=int,
        default=DEFAULT_MAX_TITLE_CHARS,
        help="Maximum title length before deterministic truncation",
    )
    parser.add_argument(
        "--max-subtitle-chars",
        type=int,
        default=120,
        help="Maximum subtitle length before deterministic truncation",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.WARNING)

    try:
        if args.preview_artifact:
            artifact = json.loads(args.preview_artifact.read_text(encoding="utf-8"))
            cards = [
                build_visual_title_card_from_artifact(
                    artifact,
                    max_title_chars=args.max_title_chars,
                    max_subtitle_chars=args.max_subtitle_chars,
                )
            ]
        else:
            with script_context() as (_config, db):
                if args.content_id is not None:
                    cards = [
                        build_visual_title_card_from_db(
                            db,
                            content_id=args.content_id,
                            max_title_chars=args.max_title_chars,
                            max_subtitle_chars=args.max_subtitle_chars,
                        )
                    ]
                else:
                    cards = build_recent_visual_title_cards_from_db(
                        db,
                        limit=args.recent,
                        max_title_chars=args.max_title_chars,
                        max_subtitle_chars=args.max_subtitle_chars,
                    )

        if args.out_dir:
            for card in cards:
                path = args.out_dir / visual_title_card_filename(card.content_id)
                write_visual_title_card_artifact(card, path)
                print(f"Visual title-card artifact: {path}", file=sys.stderr)
        else:
            payload = cards[0] if len(cards) == 1 else cards
            print(visual_title_cards_to_json(payload))
    except (VisualTitleCardError, OSError, json.JSONDecodeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
