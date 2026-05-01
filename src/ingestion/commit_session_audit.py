"""Audit commit-to-Claude-message link quality."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_MIN_CONFIDENCE = 0.5
DEFAULT_MAX_GAP_HOURS = 2.0


@dataclass(frozen=True)
class AuditCommit:
    """Recent GitHub commit included in the link audit."""

    id: int
    repo_name: str
    commit_sha: str
    commit_message: str
    timestamp: str
    author: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AuditClaudeMessage:
    """Recent Claude message included in the link audit."""

    id: int
    session_id: str
    message_uuid: str
    project_path: str | None
    timestamp: str
    prompt_text: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AuditLinkIssue:
    """One commit_prompt_links row with one or more quality concerns."""

    id: int
    commit_id: int
    message_id: int
    commit_sha: str | None
    message_uuid: str | None
    confidence: float | None
    gap_hours: float | None
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["reasons"] = list(self.reasons)
        return data


@dataclass(frozen=True)
class CommitSessionAuditReport:
    """Read-only audit report for commit_prompt_links quality."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    orphan_commits: tuple[AuditCommit, ...]
    orphan_messages: tuple[AuditClaudeMessage, ...]
    low_confidence_links: tuple[AuditLinkIssue, ...]
    duplicate_links: tuple[AuditLinkIssue, ...]
    large_gap_links: tuple[AuditLinkIssue, ...]
    flagged_links: tuple[AuditLinkIssue, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "orphan_commits": [item.to_dict() for item in self.orphan_commits],
            "orphan_messages": [item.to_dict() for item in self.orphan_messages],
            "low_confidence_links": [
                item.to_dict() for item in self.low_confidence_links
            ],
            "duplicate_links": [item.to_dict() for item in self.duplicate_links],
            "large_gap_links": [item.to_dict() for item in self.large_gap_links],
            "flagged_links": [item.to_dict() for item in self.flagged_links],
        }


def build_commit_session_audit_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    max_gap_hours: float = DEFAULT_MAX_GAP_HOURS,
    now: datetime | None = None,
) -> CommitSessionAuditReport:
    """Return a bounded, read-only audit of commit_prompt_links quality."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_confidence < 0 or min_confidence > 1:
        raise ValueError("min-confidence must be between 0 and 1")
    if max_gap_hours < 0:
        raise ValueError("max-gap-hours must be non-negative")

    conn = _connection(db_or_conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    since = generated_at - timedelta(days=days)

    commits = _load_commits(conn, since)
    messages = _load_messages(conn, since)
    commit_by_id = {commit["id"]: commit for commit in commits}
    message_by_id = {message["id"]: message for message in messages}
    links = _load_links(conn, set(commit_by_id), set(message_by_id))

    linked_commit_ids = {link["commit_id"] for link in links if link["commit_id"]}
    linked_message_ids = {link["message_id"] for link in links if link["message_id"]}
    orphan_commits = tuple(
        AuditCommit(**_commit_item(commit))
        for commit in commits
        if commit["id"] not in linked_commit_ids
    )
    orphan_messages = tuple(
        AuditClaudeMessage(**_message_item(message))
        for message in messages
        if message["id"] not in linked_message_ids
    )

    pair_counts = Counter((link["commit_id"], link["message_id"]) for link in links)
    issues = [
        _link_issue(
            link,
            commit_by_id.get(link["commit_id"]),
            message_by_id.get(link["message_id"]),
            min_confidence=min_confidence,
            max_gap_hours=max_gap_hours,
            duplicate_count=pair_counts[(link["commit_id"], link["message_id"])],
        )
        for link in links
    ]
    flagged_links = tuple(issue for issue in issues if issue.reasons)
    low_confidence_links = tuple(
        issue
        for issue in flagged_links
        if any(reason.startswith("confidence ") for reason in issue.reasons)
    )
    duplicate_links = tuple(
        issue for issue in flagged_links if "duplicate commit/message pair" in issue.reasons
    )
    large_gap_links = tuple(
        issue
        for issue in flagged_links
        if any(reason.startswith("time gap ") for reason in issue.reasons)
    )

    return CommitSessionAuditReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "window_start": since.isoformat(),
            "window_end": generated_at.isoformat(),
            "min_confidence": min_confidence,
            "max_gap_hours": max_gap_hours,
        },
        totals={
            "commits": len(commits),
            "claude_messages": len(messages),
            "links": len(links),
            "orphan_commits": len(orphan_commits),
            "orphan_messages": len(orphan_messages),
            "low_confidence_links": len(low_confidence_links),
            "duplicate_links": len(duplicate_links),
            "large_gap_links": len(large_gap_links),
            "flagged_links": len(flagged_links),
        },
        orphan_commits=orphan_commits,
        orphan_messages=orphan_messages,
        low_confidence_links=low_confidence_links,
        duplicate_links=duplicate_links,
        large_gap_links=large_gap_links,
        flagged_links=flagged_links,
    )


def format_commit_session_audit_json(report: CommitSessionAuditReport) -> str:
    """Serialize a commit session audit report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_commit_session_audit_text(report: CommitSessionAuditReport) -> str:
    """Format a commit session audit report for terminal review."""
    lines = [
        "Commit Session Link Audit",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['window_start']} to "
            f"{report.filters['window_end']}"
        ),
        (
            "Thresholds: "
            f"min_confidence={report.filters['min_confidence']:.2f} "
            f"max_gap_hours={report.filters['max_gap_hours']:.2f}"
        ),
        (
            "Summary: "
            f"commits={report.totals['commits']} "
            f"claude_messages={report.totals['claude_messages']} "
            f"links={report.totals['links']} "
            f"flagged={report.totals['flagged_links']}"
        ),
    ]

    _append_commit_section(lines, "Orphan commits:", report.orphan_commits)
    _append_message_section(lines, "Orphan Claude messages:", report.orphan_messages)
    _append_link_section(lines, "Low-confidence links:", report.low_confidence_links)
    _append_link_section(lines, "Duplicate links:", report.duplicate_links)
    _append_link_section(lines, "Large time-gap links:", report.large_gap_links)
    return "\n".join(lines)


def _load_commits(conn: sqlite3.Connection, since: datetime) -> list[dict[str, Any]]:
    return _rows(
        conn.execute(
            """SELECT id, repo_name, commit_sha, commit_message, timestamp, author
               FROM github_commits
               WHERE timestamp >= ?
               ORDER BY timestamp ASC, id ASC""",
            (_db_time(since),),
        )
    )


def _load_messages(conn: sqlite3.Connection, since: datetime) -> list[dict[str, Any]]:
    return _rows(
        conn.execute(
            """SELECT id, session_id, message_uuid, project_path, timestamp, prompt_text
               FROM claude_messages
               WHERE timestamp >= ?
               ORDER BY timestamp ASC, id ASC""",
            (_db_time(since),),
        )
    )


def _load_links(
    conn: sqlite3.Connection,
    commit_ids: set[int],
    message_ids: set[int],
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if commit_ids:
        clauses.append(f"commit_id IN ({','.join('?' for _ in commit_ids)})")
        params.extend(sorted(commit_ids))
    if message_ids:
        clauses.append(f"message_id IN ({','.join('?' for _ in message_ids)})")
        params.extend(sorted(message_ids))
    if not clauses:
        return []
    return _rows(
        conn.execute(
            f"""SELECT id, commit_id, message_id, confidence
                FROM commit_prompt_links
                WHERE {' OR '.join(clauses)}
                ORDER BY commit_id ASC, message_id ASC, id ASC""",
            tuple(params),
        )
    )


def _link_issue(
    link: dict[str, Any],
    commit: dict[str, Any] | None,
    message: dict[str, Any] | None,
    *,
    min_confidence: float,
    max_gap_hours: float,
    duplicate_count: int,
) -> AuditLinkIssue:
    confidence = _optional_float(link.get("confidence"))
    gap_hours = _gap_hours(commit, message)
    reasons: list[str] = []
    if confidence is None:
        reasons.append("missing confidence")
    elif confidence < min_confidence:
        reasons.append(
            f"confidence {confidence:.2f} below threshold {min_confidence:.2f}"
        )
    if duplicate_count > 1:
        reasons.append("duplicate commit/message pair")
    if gap_hours is None:
        reasons.append("missing commit or message timestamp for gap check")
    elif gap_hours > max_gap_hours:
        reasons.append(f"time gap {gap_hours:.2f}h exceeds {max_gap_hours:.2f}h")

    return AuditLinkIssue(
        id=int(link["id"]),
        commit_id=int(link["commit_id"]),
        message_id=int(link["message_id"]),
        commit_sha=str(commit["commit_sha"]) if commit else None,
        message_uuid=str(message["message_uuid"]) if message else None,
        confidence=confidence,
        gap_hours=gap_hours,
        reasons=tuple(reasons),
    )


def _append_commit_section(
    lines: list[str],
    title: str,
    commits: tuple[AuditCommit, ...],
) -> None:
    lines.append("")
    lines.append(title)
    if not commits:
        lines.append("  - none")
        return
    for commit in commits:
        lines.append(
            f"  - {commit.timestamp} {commit.repo_name} {commit.commit_sha}: "
            f"{_shorten(commit.commit_message)}"
        )


def _append_message_section(
    lines: list[str],
    title: str,
    messages: tuple[AuditClaudeMessage, ...],
) -> None:
    lines.append("")
    lines.append(title)
    if not messages:
        lines.append("  - none")
        return
    for message in messages:
        lines.append(
            f"  - {message.timestamp} {message.session_id} "
            f"{message.message_uuid}: {_shorten(message.prompt_text)}"
        )


def _append_link_section(
    lines: list[str],
    title: str,
    links: tuple[AuditLinkIssue, ...],
) -> None:
    lines.append("")
    lines.append(title)
    if not links:
        lines.append("  - none")
        return
    for link in links:
        confidence = "missing" if link.confidence is None else f"{link.confidence:.2f}"
        gap = "missing" if link.gap_hours is None else f"{link.gap_hours:.2f}h"
        lines.append(
            f"  - link {link.id} commit={link.commit_sha or link.commit_id} "
            f"message={link.message_uuid or link.message_id} "
            f"confidence={confidence} gap={gap}; "
            f"{'; '.join(link.reasons)}"
        )


def _commit_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "repo_name": str(row["repo_name"]),
        "commit_sha": str(row["commit_sha"]),
        "commit_message": str(row["commit_message"]),
        "timestamp": str(row["timestamp"]),
        "author": row.get("author"),
    }


def _message_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "session_id": str(row["session_id"]),
        "message_uuid": str(row["message_uuid"]),
        "project_path": row.get("project_path"),
        "timestamp": str(row["timestamp"]),
        "prompt_text": str(row["prompt_text"]),
    }


def _gap_hours(
    commit: dict[str, Any] | None,
    message: dict[str, Any] | None,
) -> float | None:
    if not commit or not message:
        return None
    try:
        commit_at = _parse_timestamp(str(commit["timestamp"]))
        message_at = _parse_timestamp(str(message["timestamp"]))
    except ValueError:
        return None
    return round(abs((commit_at - message_at).total_seconds()) / 3600, 4)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _as_utc(parsed)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _shorten(value: Any, width: int = 96) -> str:
    text = str(value or "").replace("\n", " ").strip()
    return text if len(text) <= width else text[: max(0, width - 3)] + "..."


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if conn is None:
        raise ValueError("database connection is not available")
    return conn


def _rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    names = [description[0] for description in cursor.description or ()]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _db_time(value: datetime) -> str:
    return _as_utc(value).isoformat()
