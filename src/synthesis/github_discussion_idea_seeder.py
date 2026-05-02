"""Seed content ideas from stored GitHub Discussion activity."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


SOURCE_NAME = "github_discussion_idea_seeder"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 10
DEFAULT_MIN_BODY_LENGTH = 80
DISCUSSION_ACTIVITY_TYPES = (
    "discussion",
    "discussion_comment",
    "github_discussion",
    "github_discussion_comment",
)

_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
_MARKDOWN_PREFIX_RE = re.compile(r"^\s*(?:#{1,6}\s*|[-*+]\s+|\d+[.)]\s*|>\s*)")
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class GitHubDiscussionIdeaCandidate:
    rank: int
    repo_name: str
    activity_type: str
    number: str
    title: str
    body_excerpt: str
    url: str
    updated_at: str
    source_activity_id: str
    body_length: int
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GitHubDiscussionIdeaResult:
    status: str
    rank: int
    repo_name: str
    activity_type: str
    number: str
    title: str
    source_activity_id: str
    idea_id: int | None
    reason: str
    topic: str
    note: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GitHubDiscussionIdeaSeedReport:
    artifact_type: str
    generated_at: str
    dry_run: bool
    filters: dict[str, Any]
    totals: dict[str, int]
    skipped_reasons: dict[str, int]
    results: tuple[GitHubDiscussionIdeaResult, ...]
    availability: dict[str, bool]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "availability": dict(sorted(self.availability.items())),
            "dry_run": self.dry_run,
            "filters": self.filters,
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "results": [result.to_dict() for result in self.results],
            "skipped_reasons": dict(sorted(self.skipped_reasons.items())),
            "totals": self.totals,
        }


def seed_github_discussion_ideas(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    repo: str | None = None,
    limit: int | None = DEFAULT_LIMIT,
    min_body_length: int = DEFAULT_MIN_BODY_LENGTH,
    dry_run: bool = False,
    now: datetime | None = None,
) -> GitHubDiscussionIdeaSeedReport:
    """Create draft content ideas from recent stored discussion activity."""

    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")
    if min_body_length < 0:
        raise ValueError("min_body_length must be zero or positive")
    if repo is not None and not repo.strip():
        raise ValueError("repo must not be blank")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}
    skipped_reasons: dict[str, int] = {}

    rows = _load_discussion_rows(
        conn,
        schema,
        cutoff=cutoff,
        repo=repo,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    candidates: list[GitHubDiscussionIdeaCandidate] = []
    for row in rows:
        candidate = _row_to_candidate(row)
        if candidate.body_length < min_body_length:
            _count(skipped_reasons, "body_too_short")
            continue
        candidates.append(candidate)

    ranked = tuple(
        _with_rank(candidate, rank)
        for rank, candidate in enumerate(
            sorted(
                candidates,
                key=lambda item: (
                    -_sort_timestamp(item.updated_at),
                    item.repo_name,
                    item.activity_type,
                    item.number,
                    item.source_activity_id,
                ),
            )[:limit],
            start=1,
        )
    )

    results: list[GitHubDiscussionIdeaResult] = []
    for candidate in ranked:
        existing = _find_equivalent_idea(conn, candidate)
        if existing is not None:
            reason = _duplicate_reason(existing)
            _count(skipped_reasons, reason)
            results.append(_result(candidate, "skipped", _row_value(existing, "id"), reason))
            continue

        if dry_run:
            results.append(_result(candidate, "proposed", None, "dry_run"))
            continue

        idea_id = _insert_content_idea(db_or_conn, candidate)
        results.append(_result(candidate, "created", idea_id, "created"))

    if missing_tables:
        _count(skipped_reasons, "missing_table", len(missing_tables))
    if missing_columns:
        _count(skipped_reasons, "missing_columns", len(missing_columns))

    return GitHubDiscussionIdeaSeedReport(
        artifact_type="github_discussion_idea_seed",
        generated_at=generated_at.isoformat(),
        dry_run=dry_run,
        filters={
            "days": days,
            "repo": repo,
            "limit": limit,
            "min_body_length": min_body_length,
            "activity_types": list(DISCUSSION_ACTIVITY_TYPES),
        },
        totals={
            "scanned": len(rows),
            "eligible": len(candidates),
            "processed": len(results),
            "created": sum(1 for result in results if result.status == "created"),
            "proposed": sum(1 for result in results if result.status == "proposed"),
            "skipped": sum(1 for result in results if result.status == "skipped"),
        },
        skipped_reasons=skipped_reasons,
        results=tuple(results),
        availability={
            "content_ideas": "content_ideas" in schema,
            "github_activity": "github_activity" in schema,
        },
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_github_discussion_idea_seed_json(report: GitHubDiscussionIdeaSeedReport) -> str:
    """Serialize a seed report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_github_discussion_idea_seed_text(report: GitHubDiscussionIdeaSeedReport) -> str:
    """Format a seed report for terminal review."""

    lines = [
        "GitHub Discussion Idea Seeder",
        f"Generated: {report.generated_at}",
        (
            f"Filters: days={report.filters['days']} "
            f"repo={report.filters['repo'] or 'all'} "
            f"limit={report.filters['limit'] or 'none'} "
            f"min_body_length={report.filters['min_body_length']} "
            f"dry_run={str(report.dry_run).lower()}"
        ),
        (
            f"Counts: scanned={report.totals['scanned']} "
            f"eligible={report.totals['eligible']} "
            f"processed={report.totals['processed']} "
            f"created={report.totals['created']} "
            f"proposed={report.totals['proposed']} "
            f"skipped={report.totals['skipped']}"
        ),
    ]
    if report.skipped_reasons:
        reasons = ", ".join(
            f"{reason}={count}" for reason, count in sorted(report.skipped_reasons.items())
        )
        lines.append("Skipped reasons: " + reasons)
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if not report.results:
        lines.append("No GitHub discussion idea candidates found.")
        return "\n".join(lines)

    lines.append("Results:")
    for result in report.results:
        idea_id = result.idea_id if result.idea_id is not None else "-"
        lines.append(
            f"- {result.rank}. {result.status} idea={idea_id} "
            f"{result.repo_name}#{result.number} {result.activity_type} "
            f"reason={result.reason}"
        )
        lines.append(f"  title: {result.title}")
        lines.append(f"  source: {result.source_activity_id}")
        lines.append(f"  topic: {result.topic}")
    return "\n".join(lines)


def _load_discussion_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    repo: str | None,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    if "github_activity" not in schema:
        missing_tables.add("github_activity")
        return []
    if "content_ideas" not in schema:
        missing_tables.add("content_ideas")
        return []

    required = {
        "id",
        "repo_name",
        "activity_type",
        "number",
        "title",
        "body",
        "state",
        "url",
        "updated_at",
        "created_at_github",
        "labels",
        "metadata",
    }
    missing = tuple(sorted(required - schema["github_activity"]))
    if missing:
        missing_columns["github_activity"] = missing
        return []

    params: list[Any] = [cutoff.isoformat(), *DISCUSSION_ACTIVITY_TYPES]
    repo_clause = ""
    if repo:
        repo_clause = " AND repo_name = ?"
        params.append(repo)
    placeholders = ", ".join("?" for _ in DISCUSSION_ACTIVITY_TYPES)
    return _fetch_dicts(
        conn,
        f"""SELECT id, repo_name, activity_type, number, title, body, state, url,
                  updated_at, created_at_github, labels, metadata
           FROM github_activity
           WHERE updated_at >= ?
             AND activity_type IN ({placeholders}){repo_clause}
           ORDER BY updated_at DESC, id DESC""",
        params,
    )


def _row_to_candidate(row: dict[str, Any]) -> GitHubDiscussionIdeaCandidate:
    metadata = _metadata(row.get("metadata"))
    repo_name = _text(row.get("repo_name"))
    activity_type = _text(row.get("activity_type") or "discussion")
    number = _text(row.get("number"))
    title = _text(row.get("title") or f"GitHub Discussion #{number}")
    body = _text(row.get("body"))
    normalized_body = _normalize_text(body)
    body_length = len(normalized_body)
    excerpt = _excerpt(body, title)
    url = _text(row.get("url") or metadata.get("html_url"))
    updated_at = _text(row.get("updated_at") or row.get("created_at_github"))
    source_activity_id = _text(
        metadata.get("activity_id")
        or metadata.get("source_activity_id")
        or f"{repo_name}#{number}:{activity_type}"
    )
    labels = _labels(row.get("labels"))
    evidence_urls = tuple(url_item for url_item in (url, metadata.get("discussion_url")) if url_item)
    topic = f"{repo_name}: {title}"
    note = (
        f"Draft idea from {activity_type.replace('_', ' ')} activity in {repo_name} "
        f"#{number}: {title}. Evidence: {', '.join(evidence_urls) or 'stored GitHub activity only'}. "
        f"Repo context: {repo_name}. Body excerpt: {excerpt}"
    )
    source_metadata = {
        "source": SOURCE_NAME,
        "activity_id": source_activity_id,
        "source_activity_id": source_activity_id,
        "github_activity_id": row.get("id"),
        "repo_name": repo_name,
        "activity_type": activity_type,
        "number": number,
        "title": title,
        "url": url,
        "evidence_urls": list(evidence_urls),
        "state": row.get("state"),
        "labels": labels,
        "updated_at": updated_at,
        "body_length": body_length,
        "body_excerpt": excerpt,
        "normalized_title": _normalize_text(title),
        "normalized_body": normalized_body,
    }
    return GitHubDiscussionIdeaCandidate(
        rank=0,
        repo_name=repo_name,
        activity_type=activity_type,
        number=number,
        title=title,
        body_excerpt=excerpt,
        url=url,
        updated_at=updated_at,
        source_activity_id=source_activity_id,
        body_length=body_length,
        topic=topic,
        note=note,
        priority="normal",
        source_metadata=source_metadata,
    )


def _with_rank(
    candidate: GitHubDiscussionIdeaCandidate,
    rank: int,
) -> GitHubDiscussionIdeaCandidate:
    return GitHubDiscussionIdeaCandidate(
        rank=rank,
        repo_name=candidate.repo_name,
        activity_type=candidate.activity_type,
        number=candidate.number,
        title=candidate.title,
        body_excerpt=candidate.body_excerpt,
        url=candidate.url,
        updated_at=candidate.updated_at,
        source_activity_id=candidate.source_activity_id,
        body_length=candidate.body_length,
        topic=candidate.topic,
        note=candidate.note,
        priority=candidate.priority,
        source_metadata=candidate.source_metadata,
    )


def _find_equivalent_idea(
    conn: sqlite3.Connection,
    candidate: GitHubDiscussionIdeaCandidate,
) -> dict[str, Any] | None:
    if not _has_table(conn, "content_ideas"):
        return None
    cursor = conn.execute(
        """SELECT *
           FROM content_ideas
           WHERE status IN ('open', 'promoted')
           ORDER BY created_at ASC, id ASC"""
    )
    candidate_topic = _normalize_text(candidate.topic)
    candidate_note = _normalize_text(candidate.note)
    candidate_title = candidate.source_metadata["normalized_title"]
    candidate_body = candidate.source_metadata["normalized_body"]
    candidate_ids = {
        "activity_id": _normalize_text(candidate.source_activity_id),
        "source_activity_id": _normalize_text(candidate.source_activity_id),
        "github_activity_id": _normalize_text(candidate.source_metadata.get("github_activity_id")),
    }
    for row in cursor.fetchall():
        item = dict(row)
        metadata = _metadata(item.get("source_metadata"))
        existing_ids = {
            "activity_id": _normalize_text(metadata.get("activity_id")),
            "source_activity_id": _normalize_text(metadata.get("source_activity_id")),
            "github_activity_id": _normalize_text(metadata.get("github_activity_id")),
        }
        if any(value and value == existing_ids.get(key) for key, value in candidate_ids.items()):
            item["duplicate_reason"] = f"{item.get('status') or 'active'} duplicate"
            return item
        if candidate_topic and candidate_topic == _normalize_text(item.get("topic")):
            item["duplicate_reason"] = "normalized topic duplicate"
            return item
        if candidate_note and candidate_note == _normalize_text(item.get("note")):
            item["duplicate_reason"] = "normalized note duplicate"
            return item
        if (
            candidate_title
            and candidate_body
            and candidate_title == _normalize_text(metadata.get("normalized_title"))
            and candidate_body == _normalize_text(metadata.get("normalized_body"))
        ):
            item["duplicate_reason"] = "normalized title/body duplicate"
            return item
    return None


def _insert_content_idea(db_or_conn: Any, candidate: GitHubDiscussionIdeaCandidate) -> int:
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
    candidate: GitHubDiscussionIdeaCandidate,
    status: str,
    idea_id: int | None,
    reason: str,
) -> GitHubDiscussionIdeaResult:
    return GitHubDiscussionIdeaResult(
        status=status,
        rank=candidate.rank,
        repo_name=candidate.repo_name,
        activity_type=candidate.activity_type,
        number=candidate.number,
        title=candidate.title,
        source_activity_id=candidate.source_activity_id,
        idea_id=idea_id,
        reason=reason,
        topic=candidate.topic,
        note=candidate.note,
        source_metadata=candidate.source_metadata,
    )


def _duplicate_reason(existing: dict[str, Any]) -> str:
    return _text(existing.get("duplicate_reason") or f"{existing.get('status') or 'active'} duplicate")


def _excerpt(body: str, fallback: str, width: int = 360) -> str:
    lines: list[str] = []
    in_code_block = False
    for raw_line in body.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            continue
        line = _MARKDOWN_PREFIX_RE.sub("", stripped)
        line = _MARKDOWN_LINK_RE.sub(r"\1", line)
        line = _WHITESPACE_RE.sub(" ", line).strip()
        if line:
            lines.append(line)
    excerpt = " ".join(lines) or fallback
    if len(excerpt) > width:
        return excerpt[: width - 3].rstrip() + "..."
    return excerpt


def _labels(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return [value]
    if isinstance(decoded, list):
        return [str(item) for item in decoded if str(item).strip()]
    return []


def _metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalize_text(value: Any) -> str:
    return _WHITESPACE_RE.sub(" ", _text(value).strip().lower())


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _sort_timestamp(value: str) -> float:
    parsed = _parse_datetime(value)
    return parsed.timestamp() if parsed is not None else 0.0


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _row_value(row: dict[str, Any], key: str) -> Any:
    return row.get(key)


def _count(counter: dict[str, int], key: str, amount: int = 1) -> None:
    counter[key] = counter.get(key, 0) + amount


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tables = [row["name"] if hasattr(row, "keys") else row[0] for row in rows]
    return {
        table: {
            column["name"] if hasattr(column, "keys") else column[1]
            for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for table in tables
    }


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _fetch_dicts(
    conn: sqlite3.Connection,
    query: str,
    params: list[Any],
) -> list[dict[str, Any]]:
    cursor = conn.execute(query, tuple(params))
    columns = [column[0] for column in cursor.description or []]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
