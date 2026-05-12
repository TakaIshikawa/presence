"""Identify days/projects where Claude session ingestion appears incomplete."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_MIN_COMMITS = 1


@dataclass(frozen=True)
class SessionIngestionGap:
    """One day/project activity mismatch."""

    date: str
    repo_or_project: str
    commit_count: int
    session_count: int
    gap_reason_code: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SessionIngestionGapsReport:
    """Claude session ingestion gaps report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    gaps: tuple[SessionIngestionGap, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_ingestion_gaps",
            "filters": dict(self.filters),
            "gap_count": len(self.gaps),
            "gaps": [gap.to_dict() for gap in self.gaps],
            "generated_at": self.generated_at,
            "totals": dict(self.totals),
        }


def build_session_ingestion_gaps_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_commits: int = DEFAULT_MIN_COMMITS,
    now: datetime | None = None,
) -> SessionIngestionGapsReport:
    """Compare recent commit and Claude session activity by day/project."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if min_commits <= 0:
        raise ValueError("min_commits must be positive")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    conn = _connection(db_or_conn)
    commits = _commit_counts(conn, cutoff)
    sessions = _session_counts(conn, cutoff)
    keys = set(commits) | set(sessions)
    gaps: list[SessionIngestionGap] = []
    for key in keys:
        commit_count = commits.get(key, 0)
        session_count = sessions.get(key, 0)
        reason = None
        if commit_count >= min_commits and session_count == 0:
            reason = "commit_activity_without_sessions"
        elif commit_count == 0 and session_count > 0:
            reason = "session_activity_without_commits"
        elif commit_count >= min_commits and session_count * 3 < commit_count:
            reason = "sessions_under_ingested"
        if reason:
            gaps.append(SessionIngestionGap(key[0], key[1], commit_count, session_count, reason))
    gaps.sort(key=lambda gap: (gap.date, gap.repo_or_project, gap.gap_reason_code))
    return SessionIngestionGapsReport(
        generated_at=generated_at.isoformat(),
        filters={
            "lookback_days": lookback_days,
            "cutoff": cutoff.isoformat(),
            "min_commits": min_commits,
        },
        totals={
            "commit_bucket_count": len(commits),
            "session_bucket_count": len(sessions),
            "gap_count": len(gaps),
        },
        gaps=tuple(gaps),
    )


def format_session_ingestion_gaps_json(report: SessionIngestionGapsReport) -> str:
    """Serialize as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_session_ingestion_gaps_text(report: SessionIngestionGapsReport) -> str:
    """Render Claude session ingestion gaps."""
    lines = [
        "Claude Session Ingestion Gaps",
        f"Generated: {report.generated_at}",
        (
            f"Window: lookback_days={report.filters['lookback_days']} "
            f"min_commits={report.filters['min_commits']}"
        ),
        f"Totals: gaps={report.totals['gap_count']}",
    ]
    if not report.gaps:
        lines.extend(["", "No Claude session ingestion gaps found."])
        return "\n".join(lines)
    lines.extend(["", "Gaps:"])
    for gap in report.gaps:
        lines.append(
            f"- date={gap.date} project={gap.repo_or_project} commits={gap.commit_count} "
            f"sessions={gap.session_count} reason={gap.gap_reason_code}"
        )
    return "\n".join(lines)


def _commit_counts(conn: sqlite3.Connection, cutoff: datetime) -> dict[tuple[str, str], int]:
    if not _has_table(conn, "github_commits"):
        return {}
    counts: dict[tuple[str, str], int] = {}
    for row in conn.execute(
        "SELECT repo_name, timestamp FROM github_commits WHERE datetime(timestamp) >= datetime(?)",
        (cutoff.isoformat(),),
    ).fetchall():
        date = _date(row["timestamp"])
        if date:
            key = (date, _project(row["repo_name"]))
            counts[key] = counts.get(key, 0) + 1
    return counts


def _session_counts(conn: sqlite3.Connection, cutoff: datetime) -> dict[tuple[str, str], int]:
    if not _has_table(conn, "claude_messages"):
        return {}
    buckets: dict[tuple[str, str], set[str]] = {}
    for row in conn.execute(
        "SELECT session_id, project_path, timestamp FROM claude_messages WHERE datetime(timestamp) >= datetime(?)",
        (cutoff.isoformat(),),
    ).fetchall():
        date = _date(row["timestamp"])
        if date:
            key = (date, _project(row["project_path"]))
            buckets.setdefault(key, set()).add(str(row["session_id"]))
    return {key: len(session_ids) for key, session_ids in buckets.items()}


def _project(value: Any) -> str:
    text = str(value or "unknown").rstrip("/")
    if "/" in text:
        return text.split("/")[-1]
    return text


def _date(value: Any) -> str | None:
    parsed = _parse_dt(value)
    return parsed.date().isoformat() if parsed else None


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    ).fetchone() is not None


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
