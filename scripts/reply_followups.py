#!/usr/bin/env python3
"""Manage follow-up reminders after high-value replies."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_followup import (  # noqa: E402
    ReplyFollowupPolicy,
    create_reply_followup_reminders,
    select_reply_followup_candidates,
)
from runner import script_context  # noqa: E402

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create and manage future follow-up reminders after approved or sent replies."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Print proposed reminders without inserting rows.",
    )
    mode.add_argument(
        "--mark-done",
        type=int,
        metavar="ID",
        help="Mark a pending follow-up reminder done.",
    )
    mode.add_argument(
        "--dismiss",
        type=int,
        metavar="ID",
        help="Dismiss a pending follow-up reminder.",
    )
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON.")
    parser.add_argument("--notes", help="Optional notes for inserted, completed, or dismissed reminders.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum candidates to propose or insert.")
    parser.add_argument("--due-in-days", type=int, default=7, help="Days from now for new reminder due_at.")
    parser.add_argument(
        "--cooldown-days",
        type=int,
        default=14,
        help="Skip targets with a recent pending or done follow-up.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=14,
        help="Only consider approved/posted reply sources this many days back.",
    )
    parser.add_argument(
        "--list",
        choices=["pending", "due", "upcoming", "done", "dismissed", "all"],
        help="List existing reminders instead of creating new ones.",
    )
    return parser


def _policy_from_args(args: argparse.Namespace) -> ReplyFollowupPolicy:
    if args.limit <= 0:
        raise ValueError("--limit must be positive")
    if args.due_in_days <= 0:
        raise ValueError("--due-in-days must be positive")
    if args.cooldown_days <= 0:
        raise ValueError("--cooldown-days must be positive")
    if args.lookback_days <= 0:
        raise ValueError("--lookback-days must be positive")
    return ReplyFollowupPolicy(
        source_lookback_days=args.lookback_days,
        target_cooldown_days=args.cooldown_days,
        due_in_days=args.due_in_days,
        limit=args.limit,
    )


def _with_notes(items: list[dict[str, Any]], notes: str | None) -> list[dict[str, Any]]:
    if not notes:
        return items
    return [{**item, "notes": item.get("notes") or notes} for item in items]


def _print_text(payload: dict[str, Any]) -> None:
    action = payload.get("action")
    if action in {"mark_done", "dismiss"}:
        status = "updated" if payload.get("updated") else "not updated"
        print(f"Reminder {payload.get('id')}: {status}")
        return

    reminders = payload.get("reminders") or []
    print(f"{len(reminders)} reply follow-up reminder{'s' if len(reminders) != 1 else ''}")
    for item in reminders:
        marker = ""
        if "inserted" in item:
            marker = " inserted" if item["inserted"] else " duplicate"
        print(
            f"  @{item['target_handle']} due {item['due_at']} "
            f"from {item['source_type']}#{item['source_id']}{marker}"
        )
        print(f"    {item['reason']}")


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING if args.json else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    with script_context() as (_config, db):
        if args.mark_done is not None:
            updated = db.mark_reply_followup_done(args.mark_done, notes=args.notes)
            payload = {"action": "mark_done", "id": args.mark_done, "updated": updated}
        elif args.dismiss is not None:
            updated = db.dismiss_reply_followup(args.dismiss, notes=args.notes)
            payload = {"action": "dismiss", "id": args.dismiss, "updated": updated}
        elif args.list:
            if args.list in {"due", "upcoming"}:
                reminders = db.list_reply_followup_reminders(status="pending", due=args.list)
            else:
                reminders = db.list_reply_followup_reminders(status=args.list)
            payload = {"action": "list", "reminders": reminders}
        else:
            policy = _policy_from_args(args)
            if args.dry_run:
                reminders = [item.to_dict() for item in select_reply_followup_candidates(db, policy=policy)]
                reminders = _with_notes(reminders, args.notes)
                payload = {"action": "dry_run", "reminders": reminders}
            else:
                reminders = create_reply_followup_reminders(
                    db,
                    policy=policy,
                    notes=args.notes,
                )
                payload = {"action": "create", "reminders": reminders}

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _print_text(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
