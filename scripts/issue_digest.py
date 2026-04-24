#!/usr/bin/env python3
"""Generate reviewable summaries from recent GitHub issue activity."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from storage.db import Database
from synthesis.issue_digest import IssueDigest, IssueSeedResult, build_issue_digest, seed_issue_ideas


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_digest_table(digest: IssueDigest, seed_results: list[IssueSeedResult] | None = None) -> str:
    total_issues = sum(len(group.issues) for group in digest.groups)
    high_signal = sum(group.high_signal_count for group in digest.groups)
    lines = [
        f"groups={len(digest.groups)} issues={total_issues} high_signal={high_signal}",
        f"{'Repo':20s}  {'Label':14s}  {'Issues':>6s}  {'Open':>4s}  {'Closed':>6s}  Summary",
        f"{'-' * 20:20s}  {'-' * 14:14s}  {'-' * 6:>6s}  {'-' * 4:>4s}  {'-' * 6:>6s}  {'-' * 42}",
    ]
    if not digest.groups:
        lines.append("none                  none                 0     0       0  no recent issue activity")
    for group in digest.groups:
        lines.append(
            f"{_shorten(group.repo_name, 20):20s}  "
            f"{_shorten(group.label, 14):14s}  "
            f"{len(group.issues):6d}  "
            f"{group.opened_count:4d}  "
            f"{group.closed_count:6d}  "
            f"{_shorten(group.summary, 78)}"
        )
        for issue in group.issues[:3]:
            marker = "*" if issue.score >= 38 else "-"
            lines.append(
                f"  {marker} #{issue.number:<5d} "
                f"{issue.state[:8]:8s} "
                f"score={issue.score:4.1f} "
                f"{_shorten(issue.title, 76)}"
            )

    if seed_results is not None:
        created = sum(1 for result in seed_results if result.status == "created")
        proposed = sum(1 for result in seed_results if result.status == "proposed")
        skipped = sum(1 for result in seed_results if result.status == "skipped")
        lines.append("")
        lines.append(f"seed_results created={created} proposed={proposed} skipped={skipped}")
        for result in seed_results:
            idea_id = result.idea_id if result.idea_id is not None else "-"
            lines.append(
                f"  {result.status:8s} id={idea_id} "
                f"{_shorten(result.repo_name, 18)}#{result.number}: {result.reason}"
            )
    return "\n".join(lines)


def format_digest_json(digest: IssueDigest, seed_results: list[IssueSeedResult] | None = None) -> str:
    payload = {"digest": digest.to_dict()}
    if seed_results is not None:
        payload["seed_results"] = [result.to_dict() for result in seed_results]
    return json.dumps(payload, indent=2, sort_keys=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="./presence.db", help="SQLite database path")
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Lookback window in days for issue activity (default: 7)",
    )
    parser.add_argument("--repo", help="Only include issues from this repo_name")
    parser.add_argument("--label", help="Only include issues with this label")
    parser.add_argument("--output", help="Write output to this file instead of stdout")
    parser.add_argument(
        "--seed-ideas",
        action="store_true",
        help="Create content ideas for high-signal issues",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with Database(args.db) as db:
        digest = build_issue_digest(
            db,
            days=args.days,
            repo=args.repo,
            label=args.label,
        )
        seed_results = seed_issue_ideas(db, digest) if args.seed_ideas else None

    output = (
        format_digest_json(digest, seed_results)
        if args.json
        else format_digest_table(digest, seed_results)
    )
    if args.output:
        Path(args.output).write_text(output + "\n")
    else:
        print(output)


if __name__ == "__main__":
    main()
