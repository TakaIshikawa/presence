"""Seed content ideas from GitHub release activity."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import sqlite3
from typing import Any


SOURCE_NAME = "github_release_seed"
ACTIVITY_TYPE = "release"
DEFAULT_DAYS = 30
DEFAULT_LIMIT = 10
DEFAULT_MIN_SCORE = 15

_SEMVER_RE = re.compile(
    r"v?(\d+)\.(\d+)\.(\d+)(?:[-.]?(alpha|beta|rc|dev|pre)[\d.]*)?",
    re.IGNORECASE,
)
_BREAKING_RE = re.compile(
    r"\b(?:breaking\s*change|BREAKING|incompatible|backward.incompatible|migration\s*required)\b",
    re.IGNORECASE,
)
_CONTRIBUTOR_RE = re.compile(r"@[\w-]+")


@dataclass(frozen=True)
class ReleaseIdeaCandidate:
    activity_id: str
    repo_name: str
    tag: str
    title: str
    body_excerpt: str
    prerelease: bool
    draft: bool
    score: int
    signals: tuple[str, ...]
    topic: str
    note: str
    priority: str
    release_fingerprint: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["signals"] = list(self.signals)
        return data


@dataclass(frozen=True)
class ReleaseIdeaSeedResult:
    status: str
    activity_id: str
    repo_name: str
    tag: str
    score: int
    signals: tuple[str, ...]
    idea_id: int | None
    reason: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["signals"] = list(self.signals)
        return data


@dataclass(frozen=True)
class ReleaseIdeaSeedReport:
    generated_at: str
    filters: dict[str, Any]
    summary: dict[str, int]
    results: tuple[ReleaseIdeaSeedResult, ...]
    missing_required_tables: tuple[str, ...]
    missing_required_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "summary": dict(self.summary),
            "results": [result.to_dict() for result in self.results],
            "missing_required_tables": list(self.missing_required_tables),
            "missing_required_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_required_columns.items())
            },
        }


def seed_release_ideas(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_score: int = DEFAULT_MIN_SCORE,
    dry_run: bool = True,
    now: datetime | None = None,
) -> ReleaseIdeaSeedReport:
    """Preview or insert deduplicated content ideas from GitHub releases."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_score < 0:
        raise ValueError("min_score must be non-negative")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)

    missing_tables = tuple(
        table for table in ("github_activity",) if table not in schema
    )
    missing_columns = _missing_required_columns(schema)

    filters = {
        "days": days,
        "limit": limit,
        "min_score": min_score,
        "dry_run": dry_run,
        "cutoff": cutoff.isoformat(),
    }

    if missing_tables or missing_columns:
        return ReleaseIdeaSeedReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            summary={"candidates": 0, "created": 0, "dry_run": 0, "skipped": 0},
            results=(),
            missing_required_tables=missing_tables,
            missing_required_columns=missing_columns,
        )

    candidates = _build_candidates(conn, cutoff=cutoff, now=generated_at, min_score=min_score, limit=limit)

    results: list[ReleaseIdeaSeedResult] = []
    for candidate in candidates:
        existing = _existing_open_idea(conn, candidate)
        if existing:
            results.append(_result("skipped", candidate, existing.get("id"), "active duplicate"))
            continue
        if dry_run:
            results.append(_result("dry-run", candidate, None, "dry run"))
            continue
        idea_id = _insert_content_idea(db_or_conn, candidate)
        results.append(_result("created", candidate, idea_id, "created"))

    return ReleaseIdeaSeedReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        summary={
            "candidates": len(candidates),
            "created": sum(1 for r in results if r.status == "created"),
            "dry_run": sum(1 for r in results if r.status == "dry-run"),
            "skipped": sum(1 for r in results if r.status == "skipped"),
        },
        results=tuple(results),
        missing_required_tables=missing_tables,
        missing_required_columns=missing_columns,
    )


def _build_candidates(
    conn: sqlite3.Connection,
    *,
    cutoff: datetime,
    now: datetime,
    min_score: int,
    limit: int,
) -> list[ReleaseIdeaCandidate]:
    rows = _load_release_rows(conn, cutoff=cutoff, now=now)
    candidates = []
    for row in rows:
        candidate = _row_to_candidate(row)
        if candidate is not None and candidate.score >= min_score:
            candidates.append(candidate)
    candidates.sort(key=lambda c: (-c.score, c.repo_name.lower(), c.tag))
    return candidates[:limit]


def _load_release_rows(
    conn: sqlite3.Connection,
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """SELECT *
           FROM github_activity
           WHERE activity_type = ?
             AND datetime(updated_at) >= datetime(?)
             AND datetime(updated_at) <= datetime(?)
           ORDER BY updated_at DESC, id DESC""",
        (ACTIVITY_TYPE, cutoff.isoformat(), now.isoformat()),
    )
    return [_row_dict(row) for row in cursor.fetchall()]


def _row_to_candidate(row: dict[str, Any]) -> ReleaseIdeaCandidate | None:
    metadata = _metadata(row)
    tag = str(metadata.get("tag_name") or row.get("number") or "").strip()
    if not tag:
        return None

    repo_name = str(row.get("repo_name") or "").strip()
    title = str(row.get("title") or tag).strip()
    body = str(row.get("body") or "").strip()
    prerelease = bool(metadata.get("prerelease", False))
    draft = bool(metadata.get("draft", False))
    activity_id = str(
        metadata.get("activity_id")
        or row.get("activity_id")
        or f"{repo_name}#{tag}:release"
    )

    score, signals = _score_release(
        tag=tag,
        body=body,
        prerelease=prerelease,
        draft=draft,
    )

    fingerprint = _release_fingerprint(repo_name=repo_name, tag=tag)
    priority = "high" if score >= 35 else "normal" if score >= 15 else "low"
    topic = _build_topic(repo_name, tag, signals)
    note = _build_note(repo_name, tag, title, signals)

    source_metadata = {
        "source": SOURCE_NAME,
        "source_type": "github_release",
        "source_id": fingerprint,
        "release_fingerprint": fingerprint,
        "activity_id": activity_id,
        "github_activity_id": row.get("id"),
        "repo_name": repo_name,
        "tag": tag,
        "prerelease": prerelease,
        "draft": draft,
        "score": score,
        "signals": list(signals),
        "updated_at": str(row.get("updated_at") or ""),
    }

    return ReleaseIdeaCandidate(
        activity_id=activity_id,
        repo_name=repo_name,
        tag=tag,
        title=title,
        body_excerpt=body[:500],
        prerelease=prerelease,
        draft=draft,
        score=score,
        signals=signals,
        topic=topic,
        note=note,
        priority=priority,
        release_fingerprint=fingerprint,
        source_metadata={k: v for k, v in source_metadata.items() if v not in (None, "", [])},
    )


def _score_release(
    *,
    tag: str,
    body: str,
    prerelease: bool,
    draft: bool,
) -> tuple[int, tuple[str, ...]]:
    """Score a release by signal strength, returning (score, signal_names)."""
    score = 0
    signals: list[str] = []

    # Semver parsing
    match = _SEMVER_RE.search(tag)
    if match:
        major, minor, patch = int(match.group(1)), int(match.group(2)), int(match.group(3))
        pre_tag = match.group(4)
        if pre_tag:
            # Pre-release tag in semver
            score += 5
            signals.append("semver_prerelease_tag")
        elif major > 0 and minor == 0 and patch == 0:
            score += 40
            signals.append("major_version")
        elif patch == 0:
            score += 25
            signals.append("minor_version")
        else:
            score += 10
            signals.append("patch_version")
    else:
        score += 10
        signals.append("non_semver_tag")

    # Breaking changes in body
    if body and _BREAKING_RE.search(body):
        score += 20
        signals.append("breaking_changes")

    # Changelog detail (body length)
    if len(body) >= 500:
        score += 15
        signals.append("detailed_changelog")
    elif len(body) >= 200:
        score += 8
        signals.append("moderate_changelog")

    # Contributors mentioned
    contributors = set(_CONTRIBUTOR_RE.findall(body)) if body else set()
    if len(contributors) >= 5:
        score += 10
        signals.append("many_contributors")
    elif len(contributors) >= 2:
        score += 5
        signals.append("some_contributors")

    # Pre-release penalty
    if prerelease:
        score = max(0, score - 15)
        signals.append("prerelease")

    # Draft penalty
    if draft:
        score = max(0, score - 20)
        signals.append("draft")

    return (score, tuple(signals))


def _build_topic(repo_name: str, tag: str, signals: tuple[str, ...]) -> str:
    kind = "release"
    if "major_version" in signals:
        kind = "major release"
    elif "minor_version" in signals:
        kind = "minor release"
    elif "breaking_changes" in signals:
        kind = "breaking release"
    return f"{repo_name}: {kind} {tag}"


def _build_note(repo_name: str, tag: str, title: str, signals: tuple[str, ...]) -> str:
    signal_text = ", ".join(signals) if signals else "release"
    return (
        f"{repo_name} published release {tag} ({title}). "
        f"Signals: {signal_text}. "
        "Suggested angle: write about what changed, migration steps if breaking, "
        "and practical impact for downstream users."
    )


def _release_fingerprint(*, repo_name: str, tag: str) -> str:
    raw = f"{repo_name.lower()}|{tag.lower()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _existing_open_idea(
    conn: sqlite3.Connection,
    candidate: ReleaseIdeaCandidate,
) -> dict[str, Any] | None:
    schema = _schema(conn)
    if "content_ideas" not in schema or "source_metadata" not in schema.get("content_ideas", set()):
        return None
    rows = conn.execute(
        """SELECT * FROM content_ideas
           WHERE status IN ('open', 'promoted')
             AND source_metadata IS NOT NULL
           ORDER BY created_at ASC, id ASC"""
    ).fetchall()
    for row in rows:
        item = _row_dict(row)
        metadata = _decode_json_object(item.get("source_metadata"))
        if metadata.get("source") != SOURCE_NAME:
            continue
        if (
            metadata.get("release_fingerprint") == candidate.release_fingerprint
            or metadata.get("source_id") == candidate.release_fingerprint
        ):
            return item
    return None


def _insert_content_idea(db_or_conn: Any, candidate: ReleaseIdeaCandidate) -> int:
    if hasattr(db_or_conn, "add_content_idea"):
        return int(
            db_or_conn.add_content_idea(
                note=candidate.note,
                topic=candidate.topic,
                priority=candidate.priority,
                source=SOURCE_NAME,
                source_metadata=candidate.source_metadata,
            )
        )
    conn = _connection(db_or_conn)
    cursor = conn.execute(
        """INSERT INTO content_ideas
           (note, topic, priority, status, source, source_metadata)
           VALUES (?, ?, ?, 'open', ?, ?)""",
        (
            candidate.note,
            candidate.topic,
            candidate.priority,
            SOURCE_NAME,
            json.dumps(candidate.source_metadata, sort_keys=True),
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _result(
    status: str,
    candidate: ReleaseIdeaCandidate,
    idea_id: int | None,
    reason: str,
) -> ReleaseIdeaSeedResult:
    return ReleaseIdeaSeedResult(
        status=status,
        activity_id=candidate.activity_id,
        repo_name=candidate.repo_name,
        tag=candidate.tag,
        score=candidate.score,
        signals=candidate.signals,
        idea_id=int(idea_id) if idea_id is not None else None,
        reason=reason,
        topic=candidate.topic,
        note=candidate.note,
        priority=candidate.priority,
        source_metadata=candidate.source_metadata,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {
            info["name"] if isinstance(info, sqlite3.Row) else info[1]
            for info in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _missing_required_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    required = {
        "github_activity": {"id", "repo_name", "activity_type", "number", "title", "updated_at", "metadata"},
    }
    result: dict[str, tuple[str, ...]] = {}
    for table, columns in required.items():
        if table not in schema:
            continue
        missing = tuple(sorted(columns - schema[table]))
        if missing:
            result[table] = missing
    return result


def _metadata(row: dict[str, Any]) -> dict[str, Any]:
    return _decode_json_object(row.get("metadata"))


def _decode_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _row_dict(row: Any) -> dict[str, Any]:
    item = dict(row)
    if isinstance(item.get("metadata"), str):
        item["metadata"] = _decode_json_object(item["metadata"])
    return item


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
