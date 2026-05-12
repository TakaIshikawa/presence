"""Evaluate generated blog post candidates for publication readiness."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_MAX_SOURCE_AGE_DAYS = 120


@dataclass(frozen=True)
class BlogPublicationReadinessCandidate:
    """Readiness result for one blog candidate."""

    content_id: int
    title: str | None
    slug: str | None
    published: bool
    readiness_status: str
    blocker_codes: tuple[str, ...]
    warning_codes: tuple[str, ...]
    source_count: int
    newest_source_age_days: int | None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["blocker_codes"] = list(self.blocker_codes)
        payload["warning_codes"] = list(self.warning_codes)
        return payload


@dataclass(frozen=True)
class BlogPublicationReadinessReport:
    """Blog publication readiness report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    candidates: tuple[BlogPublicationReadinessCandidate, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "blog_publication_readiness",
            "candidate_count": len(self.candidates),
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "totals": dict(self.totals),
        }


def build_blog_publication_readiness_report(
    db_or_conn: Any,
    *,
    ready_only: bool = False,
    max_source_age_days: int = DEFAULT_MAX_SOURCE_AGE_DAYS,
    now: datetime | None = None,
) -> BlogPublicationReadinessReport:
    """Evaluate generated blog_post rows for publication readiness."""
    if max_source_age_days <= 0:
        raise ValueError("max_source_age_days must be positive")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    rows = _load_blog_rows(conn)
    candidates = [
        _candidate(conn, row, generated_at=generated_at, max_source_age_days=max_source_age_days)
        for row in rows
    ]
    if ready_only:
        candidates = [candidate for candidate in candidates if candidate.readiness_status == "ready"]
    candidates.sort(key=lambda item: (item.readiness_status != "blocked", item.content_id))
    return BlogPublicationReadinessReport(
        generated_at=generated_at.isoformat(),
        filters={"ready_only": ready_only, "max_source_age_days": max_source_age_days},
        totals={
            "blog_candidate_count": len(rows),
            "reported_count": len(candidates),
            "ready_count": sum(1 for candidate in candidates if candidate.readiness_status == "ready"),
            "blocked_count": sum(1 for candidate in candidates if candidate.readiness_status == "blocked"),
        },
        candidates=tuple(candidates),
    )


def format_blog_publication_readiness_json(report: BlogPublicationReadinessReport) -> str:
    """Serialize as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_blog_publication_readiness_text(report: BlogPublicationReadinessReport) -> str:
    """Render blog publication readiness."""
    lines = [
        "Blog Publication Readiness",
        f"Generated: {report.generated_at}",
        f"Filters: ready_only={int(report.filters['ready_only'])} max_source_age_days={report.filters['max_source_age_days']}",
        (
            f"Totals: candidates={report.totals['blog_candidate_count']} "
            f"reported={report.totals['reported_count']} ready={report.totals['ready_count']} "
            f"blocked={report.totals['blocked_count']}"
        ),
    ]
    if not report.candidates:
        lines.extend(["", "No blog publication candidates found."])
        return "\n".join(lines)
    lines.extend(["", "Candidates:"])
    for candidate in report.candidates:
        lines.append(
            f"- content_id={candidate.content_id} status={candidate.readiness_status} "
            f"title={candidate.title or '-'} slug={candidate.slug or '-'} "
            f"blockers={','.join(candidate.blocker_codes) or '-'} "
            f"warnings={','.join(candidate.warning_codes) or '-'}"
        )
    return "\n".join(lines)


def _load_blog_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    if not _has_table(conn, "generated_content"):
        return []
    return conn.execute(
        """SELECT id, content, source_commits, source_messages, source_activity_ids,
                  published, created_at
             FROM generated_content
             WHERE content_type = 'blog_post'
             ORDER BY id ASC"""
    ).fetchall()


def _candidate(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    generated_at: datetime,
    max_source_age_days: int,
) -> BlogPublicationReadinessCandidate:
    content = str(row["content"] or "")
    title = _title(content)
    slug = _slug(content)
    source_dates = _source_dates(conn, row)
    source_count = len(source_dates)
    ages = [(generated_at - date).days for date in source_dates]
    newest_age = min(ages) if ages else None
    blockers: list[str] = []
    warnings: list[str] = []
    if not title:
        blockers.append("missing_title")
    if not slug:
        blockers.append("missing_slug")
    if source_count < 2:
        blockers.append("weak_source_grounding")
    if not _has_summary(content):
        warnings.append("absent_summary")
    if ages and newest_age is not None and newest_age > max_source_age_days:
        warnings.append("stale_source_material")
    if not bool(row["published"]) and not blockers:
        warnings.append("unpublished_ready_candidate")
    status = "blocked" if blockers else "ready"
    return BlogPublicationReadinessCandidate(
        content_id=int(row["id"]),
        title=title,
        slug=slug,
        published=bool(row["published"]),
        readiness_status=status,
        blocker_codes=tuple(blockers),
        warning_codes=tuple(warnings),
        source_count=source_count,
        newest_source_age_days=newest_age,
    )


def _source_dates(conn: sqlite3.Connection, row: sqlite3.Row) -> list[datetime]:
    dates: list[datetime] = []
    if _has_table(conn, "github_commits"):
        for sha in _json_list(row["source_commits"]):
            found = conn.execute("SELECT timestamp FROM github_commits WHERE commit_sha = ?", (str(sha),)).fetchone()
            parsed = _parse_dt(found["timestamp"]) if found else None
            if parsed:
                dates.append(parsed)
    if _has_table(conn, "claude_messages"):
        for uuid in _json_list(row["source_messages"]):
            found = conn.execute("SELECT timestamp FROM claude_messages WHERE message_uuid = ?", (str(uuid),)).fetchone()
            parsed = _parse_dt(found["timestamp"]) if found else None
            if parsed:
                dates.append(parsed)
    if _has_table(conn, "content_knowledge_links") and _has_table(conn, "knowledge"):
        rows = conn.execute(
            """SELECT COALESCE(k.published_at, k.ingested_at, k.created_at) AS source_at
                 FROM content_knowledge_links ckl
                 INNER JOIN knowledge k ON k.id = ckl.knowledge_id
                 WHERE ckl.content_id = ?""",
            (int(row["id"]),),
        ).fetchall()
        for found in rows:
            parsed = _parse_dt(found["source_at"])
            if parsed:
                dates.append(parsed)
    return dates


def _title(content: str) -> str | None:
    for line in content.splitlines():
        match = re.match(r"^#\s+(.+)$", line.strip())
        if match:
            return match.group(1).strip()
        if line.lower().startswith("title:"):
            return line.split(":", 1)[1].strip() or None
    return None


def _slug(content: str) -> str | None:
    for line in content.splitlines():
        if line.lower().startswith("slug:"):
            value = line.split(":", 1)[1].strip()
            return value or None
    return None


def _has_summary(content: str) -> bool:
    return any(line.lower().startswith("summary:") and line.split(":", 1)[1].strip() for line in content.splitlines())


def _json_list(value: Any) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone() is not None


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
