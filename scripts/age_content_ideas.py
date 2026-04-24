#!/usr/bin/env python3
"""Apply age-based escalation to open content ideas."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.content_idea_aging import age_content_ideas


def cmd_age(
    db,
    *,
    promote_after_days: int = 30,
    dismiss_low_after_days: int = 60,
    topic: str | None = None,
    dry_run: bool = False,
    json_output: bool = False,
) -> list[dict]:
    actions = age_content_ideas(
        db,
        promote_after_days=promote_after_days,
        dismiss_low_after_days=dismiss_low_after_days,
        topic=topic,
        dry_run=dry_run,
    )
    payload = [action.to_dict() for action in actions]

    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return payload

    if not actions:
        print("No content ideas matched the aging policy.")
        return payload

    prefix = "Would" if dry_run else "Did"
    for action in actions:
        verb = "promote" if action.action == "promote_priority" else "dismiss"
        note = " ".join(action.note.split())
        if len(note) > 70:
            note = note[:69].rstrip() + "..."
        print(
            f"{prefix} {verb} content idea {action.idea_id} "
            f"({action.age_days} days old): {action.reason}. {note}"
        )
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Escalate or dismiss stale open content ideas",
    )
    parser.add_argument(
        "--promote-after-days",
        type=int,
        default=30,
        help="Promote normal-priority ideas to high after this many days",
    )
    parser.add_argument(
        "--dismiss-low-after-days",
        type=int,
        default=60,
        help="Dismiss low-priority ideas after this many days",
    )
    parser.add_argument("--topic", help="Only age ideas with this topic")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List matching actions without updating the database",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print actions as JSON",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    with script_context() as (_config, db):
        try:
            cmd_age(
                db,
                promote_after_days=args.promote_after_days,
                dismiss_low_after_days=args.dismiss_low_after_days,
                topic=args.topic,
                dry_run=args.dry_run,
                json_output=args.json,
            )
        except ValueError as exc:
            parser.exit(1, f"error: {exc}\n")


if __name__ == "__main__":
    main()
