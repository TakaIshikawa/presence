"""Build newsletter/blog source digests from GitHub release activity."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


SOURCE_NAME = "github_release_digest"
MAX_HIGHLIGHTS = 5
HIGHLIGHT_MAX_CHARS = 220

_REF_RE = re.compile(
    r"(?:\b(?:PR|pull request|issue)\s*)?#(?P<number>\d+)\b|"
    r"https://github\.com/(?P<repo>[\w.-]+/[\w.-]+)/(?:pull|issues)/(?P<url_number>\d+)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ReleaseDigestEntry:
    repo_name: str
    title: str
    tag_name: str
    url: str
    released_at: str
    state: str
    source_activity_ids: list[str]
    highlights: list[str] = field(default_factory=list)
    references: list[str] = field(default_factory=list)
    source_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "repo_name": self.repo_name,
            "title": self.title,
            "tag_name": self.tag_name,
            "url": self.url,
            "released_at": self.released_at,
            "state": self.state,
            "source_activity_ids": self.source_activity_ids,
            "highlights": self.highlights,
            "references": self.references,
            "source_metadata": self.source_metadata,
        }


@dataclass(frozen=True)
class ReleaseDigestRepository:
    repo_name: str
    releases: list[ReleaseDigestEntry]
    source_activity_ids: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "repo_name": self.repo_name,
            "source_activity_ids": self.source_activity_ids,
            "releases": [release.to_dict() for release in self.releases],
        }


@dataclass(frozen=True)
class ReleaseDigest:
    generated_at: str
    start: str
    end: str
    repositories: list[ReleaseDigestRepository]
    source_activity_ids: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "start": self.start,
            "end": self.end,
            "source_activity_ids": self.source_activity_ids,
            "repositories": [repo.to_dict() for repo in self.repositories],
        }


@dataclass(frozen=True)
class SeedDigestIdeaResult:
    status: str
    repo_name: str
    tag_name: str
    idea_id: int | None
    reason: str


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _activity_id(row: dict[str, Any]) -> str:
    if row.get("activity_id"):
        return str(row["activity_id"])
    return f"{row.get('repo_name', '')}#{row.get('number', '')}:{row.get('activity_type', 'release')}"


def _release_timestamp(row: dict[str, Any]) -> str:
    metadata = row.get("metadata") or {}
    return str(
        metadata.get("published_at")
        or row.get("created_at_github")
        or row.get("updated_at")
        or ""
    )


def _tag_name(row: dict[str, Any]) -> str:
    metadata = row.get("metadata") or {}
    return str(metadata.get("tag_name") or row.get("title") or row.get("number") or "")


def _clean_highlight(line: str) -> str:
    line = re.sub(r"^\s{0,3}#{1,6}\s*", "", line)
    line = re.sub(r"^\s*[-*+]\s+", "", line)
    line = re.sub(r"^\s*\d+[.)]\s+", "", line)
    line = re.sub(r"\s+", " ", line).strip()
    if len(line) > HIGHLIGHT_MAX_CHARS:
        return line[: HIGHLIGHT_MAX_CHARS - 3].rstrip() + "..."
    return line


def parse_release_body(body: str | None) -> tuple[list[str], list[str]]:
    """Extract stable highlights and GitHub PR/issue references from release notes."""
    text = str(body or "")
    highlights: list[str] = []
    references: list[str] = []
    seen_refs: set[str] = set()

    in_code_block = False
    for raw_line in text.splitlines():
        if raw_line.strip().startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            continue
        for match in _REF_RE.finditer(raw_line):
            repo = match.group("repo")
            number = match.group("number") or match.group("url_number")
            ref = f"{repo}#{number}" if repo else f"#{number}"
            if ref not in seen_refs:
                seen_refs.add(ref)
                references.append(ref)
        line = _clean_highlight(raw_line)
        if not line:
            continue
        if line.lower() in {"what's changed", "whats changed", "changes", "changelog"}:
            continue
        highlights.append(line)
        if len(highlights) >= MAX_HIGHLIGHTS:
            break

    return highlights, references


def release_row_to_digest_entry(row: dict[str, Any]) -> ReleaseDigestEntry:
    metadata = row.get("metadata") or {}
    highlights, references = parse_release_body(row.get("body"))
    activity_id = _activity_id(row)
    released_at = _release_timestamp(row)
    return ReleaseDigestEntry(
        repo_name=str(row.get("repo_name") or ""),
        title=str(row.get("title") or _tag_name(row)),
        tag_name=_tag_name(row),
        url=str(row.get("url") or metadata.get("html_url") or ""),
        released_at=released_at,
        state=str(row.get("state") or ""),
        source_activity_ids=[activity_id],
        highlights=highlights,
        references=references,
        source_metadata={
            "activity_id": activity_id,
            "release_id": row.get("number"),
            "repo_name": row.get("repo_name"),
            "tag_name": _tag_name(row),
            "published_at": metadata.get("published_at"),
            "updated_at": row.get("updated_at"),
        },
    )


def build_release_digest(
    db,
    *,
    days: int = 14,
    repo: str | None = None,
    now: datetime | None = None,
) -> ReleaseDigest:
    """Return a release digest grouped by repository for the requested lookback."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    start = now - timedelta(days=max(days, 0))
    rows = []
    if days > 0:
        rows = db.get_recent_github_releases(days=days, repo_name=repo, limit=None, now=now)

    entries = [release_row_to_digest_entry(row) for row in rows]
    entries.sort(key=lambda item: (item.repo_name, item.released_at, item.tag_name), reverse=False)

    repos: list[ReleaseDigestRepository] = []
    for repo_name in sorted({entry.repo_name for entry in entries}):
        releases = [entry for entry in entries if entry.repo_name == repo_name]
        source_ids = [
            source_id
            for release in releases
            for source_id in release.source_activity_ids
        ]
        repos.append(
            ReleaseDigestRepository(
                repo_name=repo_name,
                releases=releases,
                source_activity_ids=source_ids,
            )
        )

    all_source_ids = [
        source_id
        for repository in repos
        for source_id in repository.source_activity_ids
    ]
    return ReleaseDigest(
        generated_at=_iso(now),
        start=_iso(start),
        end=_iso(now),
        repositories=repos,
        source_activity_ids=all_source_ids,
    )


def _entry_idea_payload(entry: ReleaseDigestEntry) -> tuple[str, str, dict[str, Any]]:
    title = f"{entry.repo_name} {entry.tag_name} release".strip()
    highlights = "; ".join(entry.highlights) if entry.highlights else "No release notes provided."
    references = ", ".join(entry.references) if entry.references else "none"
    note = (
        f"Release {entry.tag_name} in {entry.repo_name}: {entry.title}. "
        f"Highlights: {highlights} "
        f"References: {references}. "
        f"URL: {entry.url or 'none'}."
    )
    metadata = {
        "source": SOURCE_NAME,
        "activity_id": entry.source_activity_ids[0] if entry.source_activity_ids else None,
        "source_activity_ids": entry.source_activity_ids,
        "release_id": entry.source_metadata.get("release_id"),
        "repo_name": entry.repo_name,
        "tag_name": entry.tag_name,
        "title": entry.title,
        "url": entry.url,
    }
    return title, note, metadata


def seed_digest_content_ideas(db, digest: ReleaseDigest) -> list[SeedDigestIdeaResult]:
    """Create idempotent content ideas for digest releases without active ideas."""
    results: list[SeedDigestIdeaResult] = []
    find_existing = getattr(db, "find_active_content_idea_for_source_metadata", None)
    add_idea = getattr(db, "add_content_idea", None)
    if not callable(add_idea):
        return results

    for repository in digest.repositories:
        for entry in repository.releases:
            topic, note, metadata = _entry_idea_payload(entry)
            existing = None
            if callable(find_existing):
                existing = find_existing(
                    note=note,
                    topic=topic,
                    source=None,
                    source_metadata=metadata,
                )
            if existing:
                results.append(
                    SeedDigestIdeaResult(
                        status="skipped",
                        repo_name=entry.repo_name,
                        tag_name=entry.tag_name,
                        idea_id=existing["id"],
                        reason=f"{existing['status']} duplicate",
                    )
                )
                continue

            idea_id = add_idea(
                note=note,
                topic=topic,
                priority="normal",
                source=SOURCE_NAME,
                source_metadata=metadata,
            )
            results.append(
                SeedDigestIdeaResult(
                    status="created",
                    repo_name=entry.repo_name,
                    tag_name=entry.tag_name,
                    idea_id=idea_id,
                    reason="created",
                )
            )
    return results


def format_release_digest_json(digest: ReleaseDigest) -> str:
    return json.dumps(digest.to_dict(), indent=2, sort_keys=True)


def format_release_digest_text(digest: ReleaseDigest) -> str:
    lines = [
        f"Release digest: {digest.start} to {digest.end}",
        f"Repositories: {len(digest.repositories)}",
    ]
    if not digest.repositories:
        lines.append("No releases found.")
        return "\n".join(lines)

    for repository in digest.repositories:
        lines.append("")
        lines.append(f"## {repository.repo_name}")
        for release in repository.releases:
            label = f"{release.tag_name} - {release.title}" if release.tag_name else release.title
            lines.append(f"- {label}")
            if release.released_at:
                lines.append(f"  Date: {release.released_at}")
            if release.url:
                lines.append(f"  Link: {release.url}")
            lines.append(f"  Source activity IDs: {', '.join(release.source_activity_ids)}")
            if release.references:
                lines.append(f"  References: {', '.join(release.references)}")
            for highlight in release.highlights:
                lines.append(f"  - {highlight}")
    return "\n".join(lines)
