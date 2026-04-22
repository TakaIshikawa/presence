#!/usr/bin/env python3
"""Seed reviewable content ideas from recent GitHub releases."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context


SOURCE_NAME = "github_release_seed"
BODY_EXCERPT_CHARS = 360


@dataclass(frozen=True)
class ReleaseIdeaCandidate:
    repo_name: str
    release_id: int
    tag_name: str
    title: str
    url: str
    body_excerpt: str
    suggested_angle: str
    note: str
    topic: str
    source_metadata: dict[str, Any]


@dataclass(frozen=True)
class SeedResult:
    status: str
    repo_name: str
    tag_name: str
    idea_id: int | None
    reason: str
    note: str


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _body_excerpt(text: str | None, width: int = BODY_EXCERPT_CHARS) -> str:
    return _shorten(text, width) or "No release notes provided."


def _release_tag(row: dict) -> str:
    metadata = row.get("metadata") or {}
    return str(metadata.get("tag_name") or row.get("title") or row.get("number"))


def _suggest_angle(row: dict, tag_name: str) -> str:
    state = str(row.get("state") or "").lower()
    repo_name = row.get("repo_name") or "the repo"
    if state == "prerelease":
        return (
            f"Explain what {repo_name} is previewing in {tag_name}, what changed, "
            "and what to validate before adopting it."
        )
    return (
        f"Turn {repo_name} {tag_name} into a concise release note: what shipped, "
        "why it matters, and the upgrade or usage takeaway."
    )


def release_to_candidate(row: dict) -> ReleaseIdeaCandidate:
    metadata = row.get("metadata") or {}
    repo_name = str(row.get("repo_name") or "")
    release_id = int(row.get("number"))
    tag_name = _release_tag(row)
    title = str(row.get("title") or tag_name)
    url = str(row.get("url") or "")
    body_excerpt = _body_excerpt(row.get("body"))
    suggested_angle = _suggest_angle(row, tag_name)
    note = (
        f"Release {tag_name} in {repo_name}: {title}. "
        f"URL: {url or 'none'}. "
        f"Body excerpt: {body_excerpt} "
        f"Suggested angle: {suggested_angle}"
    )
    topic = f"{repo_name} {tag_name} release".strip()
    return ReleaseIdeaCandidate(
        repo_name=repo_name,
        release_id=release_id,
        tag_name=tag_name,
        title=title,
        url=url,
        body_excerpt=body_excerpt,
        suggested_angle=suggested_angle,
        note=note,
        topic=topic,
        source_metadata={
            "source": SOURCE_NAME,
            "activity_id": row.get("activity_id"),
            "release_id": release_id,
            "repo_name": repo_name,
            "tag_name": tag_name,
            "title": title,
            "url": url,
            "state": row.get("state"),
            "updated_at": row.get("updated_at"),
            "published_at": metadata.get("published_at"),
            "body_excerpt": body_excerpt,
            "suggested_angle": suggested_angle,
        },
    )


def seed_release_ideas(
    db,
    *,
    days: int = 14,
    repo: str | None = None,
    dry_run: bool = False,
    limit: int | None = None,
    now: datetime | None = None,
) -> list[SeedResult]:
    if limit is not None and limit <= 0:
        return []
    if days <= 0:
        return []
    now = now or datetime.now(timezone.utc)
    releases = db.get_recent_github_releases(
        days=days,
        repo_name=repo,
        limit=limit,
        now=now,
    )

    results: list[SeedResult] = []
    for row in releases:
        candidate = release_to_candidate(row)
        existing = db.find_active_content_idea_for_source_metadata(
            note=candidate.note,
            topic=candidate.topic,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        if existing:
            results.append(
                SeedResult(
                    status="skipped",
                    repo_name=candidate.repo_name,
                    tag_name=candidate.tag_name,
                    idea_id=existing["id"],
                    reason=f"{existing['status']} duplicate",
                    note=candidate.note,
                )
            )
            continue

        if dry_run:
            results.append(
                SeedResult(
                    status="skipped",
                    repo_name=candidate.repo_name,
                    tag_name=candidate.tag_name,
                    idea_id=None,
                    reason="dry run",
                    note=candidate.note,
                )
            )
            continue

        idea_id = db.add_content_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority="normal",
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(
            SeedResult(
                status="created",
                repo_name=candidate.repo_name,
                tag_name=candidate.tag_name,
                idea_id=idea_id,
                reason="created",
                note=candidate.note,
            )
        )

    return results


def format_results_table(results: list[SeedResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} skipped={skipped}"]
    lines.append(f"{'Status':8s}  {'ID':>4s}  {'Repo':20s}  {'Tag':14s}  Reason")
    lines.append(
        f"{'-' * 8:8s}  {'-' * 4:>4s}  "
        f"{'-' * 20:20s}  {'-' * 14:14s}  {'-' * 32}"
    )
    if not results:
        lines.append("none      ----  --------------------  --------------  no eligible releases")
        return "\n".join(lines)

    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        lines.append(
            f"{result.status:8s}  "
            f"{idea_id:>4s}  "
            f"{_shorten(result.repo_name, 20):20s}  "
            f"{_shorten(result.tag_name, 14):14s}  "
            f"{result.reason}"
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Lookback window in days for release activity (default: 14)",
    )
    parser.add_argument("--repo", help="Only seed releases for this repo name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show ideas that would be created without writing to the database",
    )
    parser.add_argument("--limit", type=int, help="Maximum releases to process")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        results = seed_release_ideas(
            db,
            days=args.days,
            repo=args.repo,
            dry_run=args.dry_run,
            limit=args.limit,
        )
    print(format_results_table(results))


if __name__ == "__main__":
    main()
