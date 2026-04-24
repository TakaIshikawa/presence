#!/usr/bin/env python3
"""Generate a deterministic blog outline from Claude sessions and commits."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from storage.db import Database
from synthesis.blog_outline import build_blog_outline

logger = logging.getLogger(__name__)


def _format_text(outline: dict) -> str:
    lines = ["Blog Outline", ""]
    lines.append("Title candidates:")
    for title in outline["title_candidates"]:
        lines.append(f"- {title}")
    lines.append("")
    lines.append("Sections:")
    for section in outline["sections"]:
        lines.append(f"- {section['heading']}")
        for bullet in section["bullets"]:
            lines.append(f"  - {bullet}")
    if outline["warnings"]:
        lines.append("")
        lines.append("Warnings:")
        for warning in outline["warnings"]:
            lines.append(f"- {warning}")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="Path to the Presence SQLite database")
    parser.add_argument("--days", type=int, default=7, help="Number of days to inspect (default: 7)")
    parser.add_argument("--repo", help="Repo name or project directory basename to filter")
    parser.add_argument("--output", help="Write output to this file instead of stdout")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    return parser.parse_args(argv)


def _write_or_print(text: str, output: str | None) -> None:
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    else:
        print(text)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.db:
        db = Database(args.db)
        db.connect()
        try:
            outline = build_blog_outline(db, days=args.days, repo=args.repo)
        finally:
            db.close()
    else:
        with script_context() as (_config, db):
            outline = build_blog_outline(db, days=args.days, repo=args.repo)

    payload = outline.to_dict()
    text = json.dumps(payload, indent=2, sort_keys=True) if args.json else _format_text(payload)
    _write_or_print(text, args.output)


if __name__ == "__main__":
    main()
