"""Build deterministic blog outlines from Claude sessions and commits."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


PROBLEM_TERMS = (
    "bug",
    "broken",
    "error",
    "fail",
    "flaky",
    "issue",
    "missing",
    "problem",
    "regression",
)
APPROACH_TERMS = (
    "add",
    "build",
    "change",
    "create",
    "implement",
    "refactor",
    "reuse",
    "update",
)
RESULT_TERMS = (
    "fix",
    "fixed",
    "pass",
    "passing",
    "resolve",
    "resolved",
    "ship",
    "shipped",
    "verify",
    "verified",
)
DECISION_TERMS = (
    "chose",
    "decide",
    "decided",
    "decision",
    "instead",
    "tradeoff",
    "why",
)


@dataclass(frozen=True)
class SourceReference:
    """A compact pointer back to source material used by an outline section."""

    source_type: str
    identifier: str
    timestamp: str
    label: str
    text: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OutlineSection:
    """One deterministic section candidate for a blog draft."""

    heading: str
    angle: str
    bullets: list[str]
    source_references: list[SourceReference]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["source_references"] = [ref.to_dict() for ref in self.source_references]
        return data


@dataclass(frozen=True)
class BlogOutline:
    """Structured artifact reviewed before prose blog generation."""

    title_candidates: list[str]
    sections: list[OutlineSection]
    source_references: list[SourceReference]
    warnings: list[str]
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "title_candidates": self.title_candidates,
            "sections": [section.to_dict() for section in self.sections],
            "source_references": [ref.to_dict() for ref in self.source_references],
            "warnings": self.warnings,
            "metadata": self.metadata,
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)


def _parse_timestamp(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _day_key(value: str | datetime) -> str:
    return _parse_timestamp(value).date().isoformat()


def _project_label(project_path: str | None) -> str:
    if not project_path:
        return "unknown-project"
    return Path(project_path).expanduser().name or str(project_path)


def _repo_label(repo_name: str | None) -> str:
    if not repo_name:
        return "unknown-repo"
    return str(repo_name).split("/")[-1] or str(repo_name)


def _repo_matches(row: dict[str, Any], repo: str | None) -> bool:
    if not repo:
        return True
    needle = repo.strip().lower()
    repo_name = str(row.get("repo_name") or "").lower()
    project = str(row.get("project_path") or "").lower()
    return (
        repo_name == needle
        or repo_name.endswith(f"/{needle}")
        or _repo_label(repo_name) == needle
        or Path(project).name.lower() == needle
        or project == needle
    )


def _text_matches_terms(text: str, terms: Iterable[str]) -> bool:
    lower = text.lower()
    return any(term in lower for term in terms)


def _clean_text(text: str, limit: int = 160) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def _source_ref_from_message(row: dict[str, Any]) -> SourceReference:
    return SourceReference(
        source_type="claude_message",
        identifier=str(row.get("message_uuid") or ""),
        timestamp=str(row.get("timestamp") or ""),
        label=str(row.get("session_id") or ""),
        text=_clean_text(str(row.get("prompt_text") or "")),
    )


def _source_ref_from_commit(row: dict[str, Any]) -> SourceReference:
    sha = str(row.get("commit_sha") or "")
    return SourceReference(
        source_type="github_commit",
        identifier=sha,
        timestamp=str(row.get("timestamp") or ""),
        label=str(row.get("repo_name") or ""),
        text=_clean_text(str(row.get("commit_message") or "")),
    )


def _fetch_prompt_links(db: Any, commit_shas: list[str]) -> dict[str, list[str]]:
    """Return linked Claude message UUIDs by commit SHA when link table exists."""

    if not commit_shas or not getattr(db, "conn", None):
        return {}

    placeholders = ",".join("?" for _ in commit_shas)
    try:
        rows = db.conn.execute(
            f"""SELECT gc.commit_sha, cm.message_uuid
                FROM commit_prompt_links cpl
                JOIN github_commits gc ON gc.id = cpl.commit_id
                JOIN claude_messages cm ON cm.id = cpl.message_id
                WHERE gc.commit_sha IN ({placeholders})
                ORDER BY gc.timestamp, cpl.confidence DESC""",
            tuple(commit_shas),
        ).fetchall()
    except Exception:
        return {}

    links: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        links[str(row["commit_sha"])].append(str(row["message_uuid"]))
    return dict(links)


def load_source_events(
    db: Any,
    *,
    days: int,
    repo: str | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Load normalized Claude message and GitHub commit events from storage."""

    if days <= 0:
        raise ValueError("days must be greater than zero")

    end = now or datetime.now(timezone.utc)
    end = _parse_timestamp(end)
    start = end - timedelta(days=days)

    messages = [
        row for row in db.get_messages_in_range(start, end)
        if _repo_matches(row, repo)
    ]
    commits = [
        row for row in db.get_commits_in_range(start, end)
        if _repo_matches(row, repo)
    ]
    commit_links = _fetch_prompt_links(
        db,
        [str(row.get("commit_sha") or "") for row in commits if row.get("commit_sha")],
    )

    events: list[dict[str, Any]] = []
    for row in messages:
        events.append(
            {
                "kind": "message",
                "day": _day_key(row["timestamp"]),
                "project": _project_label(row.get("project_path")),
                "timestamp": row["timestamp"],
                "text": row.get("prompt_text") or "",
                "row": row,
                "source_reference": _source_ref_from_message(row),
            }
        )
    for row in commits:
        sha = str(row.get("commit_sha") or "")
        events.append(
            {
                "kind": "commit",
                "day": _day_key(row["timestamp"]),
                "project": _repo_label(row.get("repo_name")),
                "timestamp": row["timestamp"],
                "text": row.get("commit_message") or "",
                "row": row,
                "linked_messages": commit_links.get(sha, []),
                "source_reference": _source_ref_from_commit(row),
            }
        )
    return sorted(events, key=lambda event: (event["day"], event["project"], event["timestamp"], event["kind"]))


def group_source_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group normalized events by UTC day and project/repo label."""

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[(event["day"], event["project"])].append(event)

    groups = []
    for (day, project), group_events in sorted(grouped.items()):
        group_events = sorted(group_events, key=lambda event: (event["timestamp"], event["kind"]))
        groups.append(
            {
                "day": day,
                "project": project,
                "events": group_events,
                "messages": [event for event in group_events if event["kind"] == "message"],
                "commits": [event for event in group_events if event["kind"] == "commit"],
            }
        )
    return groups


def _pick_bullets(events: list[dict[str, Any]], terms: Iterable[str], fallback: int = 3) -> list[str]:
    matches = [
        _clean_text(event["text"], limit=140)
        for event in events
        if _text_matches_terms(event["text"], terms)
    ]
    if not matches:
        matches = [_clean_text(event["text"], limit=140) for event in events[:fallback]]
    deduped = []
    seen = set()
    for item in matches:
        key = item.lower()
        if item and key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped[:fallback]


def extract_outline_sections(group: dict[str, Any]) -> list[OutlineSection]:
    """Extract problem, approach, and result sections for one grouped source set."""

    events = group["events"]
    refs = [event["source_reference"] for event in events]
    prefix = f"{group['project']} on {group['day']}"
    return [
        OutlineSection(
            heading=f"The problem: {prefix}",
            angle="What prompted the work and what was missing or broken.",
            bullets=_pick_bullets(events, PROBLEM_TERMS),
            source_references=refs,
        ),
        OutlineSection(
            heading=f"The approach: {group['project']}",
            angle="How the implementation moved from prompt to code.",
            bullets=_pick_bullets(events, APPROACH_TERMS),
            source_references=refs,
        ),
        OutlineSection(
            heading=f"The result: {group['project']}",
            angle="What changed, shipped, or became verifiable.",
            bullets=_pick_bullets(events, RESULT_TERMS),
            source_references=refs,
        ),
    ]


def _title_candidates(groups: list[dict[str, Any]]) -> list[str]:
    if not groups:
        return ["Untitled Claude Code Session Outline"]
    projects = sorted({group["project"] for group in groups})
    date_span = groups[0]["day"] if groups[0]["day"] == groups[-1]["day"] else f"{groups[0]['day']} to {groups[-1]['day']}"
    primary = projects[0] if len(projects) == 1 else f"{projects[0]} and {len(projects) - 1} more"
    return [
        f"What Changed in {primary}",
        f"Claude Code Notes: {date_span}",
        f"From Session Logs to Shipped Work in {primary}",
    ]


def _warnings(groups: list[dict[str, Any]], events: list[dict[str, Any]]) -> list[str]:
    warnings = []
    if not events:
        return ["No Claude messages or GitHub commits were found for the selected window."]

    if not any(_text_matches_terms(event["text"], RESULT_TERMS) for event in events):
        warnings.append("Source material lacks clear results or verification evidence.")
    if not any(_text_matches_terms(event["text"], DECISION_TERMS) for event in events):
        warnings.append("Source material lacks explicit decisions or tradeoffs.")

    commits = [event for event in events if event["kind"] == "commit"]
    if not commits:
        warnings.append("Source material lacks linked commit references.")
    elif not any(event.get("linked_messages") for event in commits):
        warnings.append("GitHub commits are present but no Claude-to-commit links were found.")

    for group in groups:
        if not group["messages"]:
            warnings.append(f"{group['day']} {group['project']} has commits but no Claude session context.")
        if not group["commits"]:
            warnings.append(f"{group['day']} {group['project']} has Claude session context but no commits.")
    return warnings


def build_blog_outline(
    db: Any,
    *,
    days: int = 7,
    repo: str | None = None,
    now: datetime | None = None,
) -> BlogOutline:
    """Build a deterministic outline object from recent stored source material."""

    events = load_source_events(db, days=days, repo=repo, now=now)
    groups = group_source_events(events)
    sections = [
        section
        for group in groups
        for section in extract_outline_sections(group)
    ]
    refs = [event["source_reference"] for event in events]
    return BlogOutline(
        title_candidates=_title_candidates(groups),
        sections=sections,
        source_references=refs,
        warnings=_warnings(groups, events),
        metadata={
            "days": days,
            "repo": repo,
            "group_count": len(groups),
            "message_count": len([event for event in events if event["kind"] == "message"]),
            "commit_count": len([event for event in events if event["kind"] == "commit"]),
            "generated_at": _parse_timestamp(now or datetime.now(timezone.utc)).isoformat(),
        },
    )


__all__ = [
    "BlogOutline",
    "OutlineSection",
    "SourceReference",
    "build_blog_outline",
    "extract_outline_sections",
    "group_source_events",
    "load_source_events",
]
