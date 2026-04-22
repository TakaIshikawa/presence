#!/usr/bin/env python3
"""Manage the manual content idea inbox."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context


def cmd_add(
    db,
    note: str,
    topic: str | None = None,
    priority: str = "normal",
    source: str | None = None,
    force: bool = False,
) -> int:
    """Add a seed note to the content idea inbox."""
    duplicates = db.find_similar_content_ideas(
        note=note,
        topic=topic,
        source=source,
        statuses=("open", "promoted"),
        limit=1,
    )
    if duplicates and not force:
        duplicate = duplicates[0]
        print(
            f"Skipped duplicate content idea {duplicate['id']} "
            f"({duplicate.get('status')}; {', '.join(duplicate['duplicate_reasons'])}). "
            "Use --force to add anyway."
        )
        return duplicate["id"]
    if duplicates:
        duplicate = duplicates[0]
        print(
            f"Warning: similar content idea {duplicate['id']} exists "
            f"({duplicate.get('status')}; {', '.join(duplicate['duplicate_reasons'])})."
        )
    idea_id = db.add_content_idea(
        note=note,
        topic=topic,
        priority=priority,
        source=source,
    )
    print(f"Added content idea {idea_id}.")
    return idea_id


def cmd_list(
    db,
    status: str | None = "open",
    priority: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """List content ideas."""
    ideas = db.get_content_ideas(status=status, priority=priority, limit=limit)
    if not ideas:
        print("No content ideas.")
        return []

    print(f"{'ID':>4s}  {'Priority':8s}  {'Status':9s}  {'Topic':18s}  Note")
    print(f"{'-' * 4:>4s}  {'-' * 8:8s}  {'-' * 9:9s}  {'-' * 18:18s}  {'-' * 40}")
    for idea in ideas:
        note = " ".join(str(idea.get("note") or "").split())
        if len(note) > 80:
            note = note[:79].rstrip() + "..."
        print(
            f"{idea['id']:4d}  "
            f"{idea.get('priority') or '':8s}  "
            f"{idea.get('status') or '':9s}  "
            f"{idea.get('topic') or '':18s}  "
            f"{note}"
        )
    return ideas


def _validate_date(value: str) -> str:
    """Validate an ISO date or datetime string."""
    try:
        datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid target date '{value}'. Use YYYY-MM-DD.") from exc
    return value


def cmd_promote(
    db,
    idea_id: int,
    target_date: str,
    campaign_id: int | None = None,
    topic: str | None = None,
    angle: str | None = None,
    force: bool = False,
) -> int | None:
    """Promote an idea into planned_topics."""
    idea = db.get_content_idea(idea_id)
    if idea is None:
        raise ValueError(f"Content idea {idea_id} does not exist")
    source_metadata = idea.get("source_metadata")
    duplicates = db.find_similar_content_ideas(
        note=idea.get("note"),
        topic=topic or idea.get("topic"),
        source=idea.get("source"),
        source_metadata=source_metadata,
        statuses=("open", "promoted"),
        exclude_id=idea_id,
        limit=1,
    )
    if duplicates and not force:
        duplicate = duplicates[0]
        print(
            f"Skipped promoting content idea {idea_id}; similar content idea "
            f"{duplicate['id']} is {duplicate.get('status')} "
            f"({', '.join(duplicate['duplicate_reasons'])}). Use --force to promote anyway."
        )
        return None
    if duplicates:
        duplicate = duplicates[0]
        print(
            f"Warning: promoting despite similar content idea {duplicate['id']} "
            f"({duplicate.get('status')}; {', '.join(duplicate['duplicate_reasons'])})."
        )
    planned_topic_id = db.promote_content_idea(
        idea_id,
        target_date=_validate_date(target_date),
        campaign_id=campaign_id,
        topic=topic,
        angle=angle,
        force=force,
    )
    print(f"Promoted content idea {idea_id} to planned topic {planned_topic_id}.")
    return planned_topic_id


def cmd_dismiss(db, idea_id: int) -> None:
    """Mark an idea as dismissed."""
    db.dismiss_content_idea(idea_id)
    print(f"Dismissed content idea {idea_id}.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage manual content idea seeds")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser("add", help="Add a content idea")
    add_parser.add_argument("note", help="Seed note for future content")
    add_parser.add_argument("--topic", help="Optional topic label")
    add_parser.add_argument(
        "--priority",
        choices=("high", "normal", "low"),
        default="normal",
        help="Idea priority (default: normal)",
    )
    add_parser.add_argument("--source", help="Where this idea came from")
    add_parser.add_argument(
        "--force",
        action="store_true",
        help="Add even if a similar open or promoted idea exists",
    )

    list_parser = subparsers.add_parser("list", help="List content ideas")
    list_parser.add_argument(
        "--status",
        default="open",
        help="Filter by status: open, promoted, dismissed. Use 'all' for no status filter.",
    )
    list_parser.add_argument(
        "--priority",
        choices=("high", "normal", "low"),
        help="Filter by priority",
    )
    list_parser.add_argument("--limit", type=int, default=20, help="Maximum rows to show")

    promote_parser = subparsers.add_parser(
        "promote",
        help="Promote a content idea into planned_topics",
    )
    promote_parser.add_argument("idea_id", type=int, help="Content idea ID")
    promote_parser.add_argument(
        "--target-date",
        required=True,
        help="Target publication date for the planned topic (YYYY-MM-DD)",
    )
    promote_parser.add_argument(
        "--campaign-id",
        type=int,
        help="Optional campaign ID for the planned topic",
    )
    promote_parser.add_argument("--topic", help="Override the idea topic")
    promote_parser.add_argument("--angle", help="Override the planned topic angle")
    promote_parser.add_argument(
        "--force",
        action="store_true",
        help="Promote even if the idea is dismissed or already promoted",
    )

    dismiss_parser = subparsers.add_parser("dismiss", help="Mark an idea as dismissed")
    dismiss_parser.add_argument("idea_id", type=int, help="Content idea ID")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    with script_context() as (_config, db):
        try:
            if args.command == "add":
                cmd_add(
                    db,
                    note=args.note,
                    topic=args.topic,
                    priority=args.priority,
                    source=args.source,
                    force=args.force,
                )
            elif args.command == "list":
                status = None if args.status == "all" else args.status
                cmd_list(db, status=status, priority=args.priority, limit=args.limit)
            elif args.command == "promote":
                cmd_promote(
                    db,
                    args.idea_id,
                    target_date=args.target_date,
                    campaign_id=args.campaign_id,
                    topic=args.topic,
                    angle=args.angle,
                    force=args.force,
                )
            elif args.command == "dismiss":
                cmd_dismiss(db, args.idea_id)
        except ValueError as exc:
            parser.exit(1, f"error: {exc}\n")


if __name__ == "__main__":
    main()
