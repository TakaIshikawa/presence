"""Seed content ideas from recent high-signal hotfix commits."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


SOURCE_NAME = "github_hotfix_commit_seed"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 20
DEFAULT_MIN_SCORE = 20

_SIGNALS: tuple[tuple[str, re.Pattern[str], int], ...] = (
    ("revert", re.compile(r"\b(?:revert|rollback|back\s*out)\b", re.I), 40),
    ("security", re.compile(r"\b(?:security|cve-\d+|vulnerab|xss|csrf|auth bypass|secret leak)\b", re.I), 38),
    ("hotfix", re.compile(r"\b(?:hotfix|patch release|emergency fix)\b", re.I), 34),
    ("regression", re.compile(r"\b(?:regression|regressed|breakage|broke|broken)\b", re.I), 32),
    ("production", re.compile(r"\b(?:prod(?:uction)?|incident|outage|sev[0-3]|firefight)\b", re.I), 28),
    ("repair", re.compile(r"\b(?:repair|recover|restore|mitigate|stabilize|stop crash)\b", re.I), 24),
    ("fix", re.compile(r"^(?:fix|bugfix)(?:\(.+\))?!?:|\b(?:fix|bugfix|crash|panic|timeout|race)\b", re.I), 22),
)


@dataclass(frozen=True)
class HotfixCommitIdeaCandidate:
    repo_name: str
    commit_sha: str
    commit_message: str
    timestamp: str
    author: str | None
    score: int
    signals: tuple[str, ...]
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["signals"] = list(self.signals)
        return data


@dataclass(frozen=True)
class HotfixCommitIdeaSeedResult:
    status: str
    commit_sha: str
    repo_name: str
    score: int
    signals: tuple[str, ...]
    idea_id: int | None
    reason: str
    topic: str
    note: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["signals"] = list(self.signals)
        return data


@dataclass(frozen=True)
class HotfixCommitIdeaSeedReport:
    generated_at: str
    filters: dict[str, Any]
    summary: dict[str, int]
    results: tuple[HotfixCommitIdeaSeedResult, ...]
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


def build_hotfix_commit_idea_candidates(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_score: int = DEFAULT_MIN_SCORE,
    now: datetime | None = None,
) -> list[HotfixCommitIdeaCandidate]:
    """Return deterministic high-signal commit idea candidates without writing."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_score < 0:
        raise ValueError("min_score must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if _missing_required_columns(schema) or "github_commits" not in schema:
        return []

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    rows = _load_recent_commits(conn, schema, cutoff=cutoff, now=generated_at)
    candidates = [
        candidate
        for row in rows
        if (candidate := _candidate_from_row(row, days=days)) is not None
        and candidate.score >= min_score
    ]
    return sorted(
        candidates,
        key=lambda item: (-item.score, item.repo_name.lower(), item.timestamp, item.commit_sha),
    )[:limit]


def seed_hotfix_commit_ideas(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_score: int = DEFAULT_MIN_SCORE,
    dry_run: bool = False,
    now: datetime | None = None,
) -> HotfixCommitIdeaSeedReport:
    """Create deduplicated content ideas for high-signal hotfix-like commits."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_score < 0:
        raise ValueError("min_score must be non-negative")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(table for table in ("github_commits",) if table not in schema)
    missing_columns = _missing_required_columns(schema)
    candidates = build_hotfix_commit_idea_candidates(
        db_or_conn,
        days=days,
        limit=limit,
        min_score=min_score,
        now=generated_at,
    )

    results: list[HotfixCommitIdeaSeedResult] = []
    for candidate in candidates:
        existing = _existing_open_idea(conn, candidate)
        if existing:
            results.append(_result("skipped", candidate, existing.get("id"), "open duplicate"))
            continue
        if dry_run:
            results.append(_result("proposed", candidate, None, "dry run"))
            continue
        idea_id = _insert_content_idea(db_or_conn, candidate)
        results.append(_result("created", candidate, idea_id, "created"))

    return HotfixCommitIdeaSeedReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "limit": limit,
            "min_score": min_score,
            "dry_run": dry_run,
            "cutoff": (generated_at - timedelta(days=days)).isoformat(),
        },
        summary={
            "candidates": len(candidates),
            "created": sum(1 for result in results if result.status == "created"),
            "proposed": sum(1 for result in results if result.status == "proposed"),
            "skipped": sum(1 for result in results if result.status == "skipped"),
        },
        results=tuple(results),
        missing_required_tables=missing_tables,
        missing_required_columns=missing_columns,
    )


def format_hotfix_commit_idea_seed_json(report: HotfixCommitIdeaSeedReport) -> str:
    """Serialize a seed report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_hotfix_commit_idea_seed_text(report: HotfixCommitIdeaSeedReport) -> str:
    """Render a compact stable seed report."""
    summary = report.summary
    filters = report.filters
    lines = [
        "Hotfix commit idea seed report",
        f"Generated: {report.generated_at}",
        (
            f"Filters: days={filters['days']} limit={filters['limit']} "
            f"min_score={filters['min_score']} dry_run={filters['dry_run']}"
        ),
        (
            "Totals: "
            f"candidates={summary['candidates']} "
            f"created={summary['created']} "
            f"proposed={summary['proposed']} "
            f"skipped={summary['skipped']}"
        ),
    ]
    if report.missing_required_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_required_tables))
    if report.missing_required_columns:
        columns = [
            f"{table}({', '.join(names)})"
            for table, names in sorted(report.missing_required_columns.items())
        ]
        lines.append("Missing columns: " + "; ".join(columns))
    if not report.results:
        lines.append("No high-signal hotfix commits found.")
        return "\n".join(lines)

    lines.append(
        f"{'Status':8s}  {'ID':>4s}  {'Score':>5s}  {'Repo':20s}  {'Commit':12s}  Signals"
    )
    lines.append(
        f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 5:>5s}  "
        f"{'-' * 20:20s}  {'-' * 12:12s}  {'-' * 28}"
    )
    for result in report.results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        lines.append(
            f"{result.status:8s}  "
            f"{idea_id:>4s}  "
            f"{result.score:5d}  "
            f"{_shorten(result.repo_name, 20):20s}  "
            f"{_short_sha(result.commit_sha):12s}  "
            f"{','.join(result.signals)}"
        )
    return "\n".join(lines)


def _candidate_from_row(row: dict[str, Any], *, days: int) -> HotfixCommitIdeaCandidate | None:
    message = str(row.get("commit_message") or "").strip()
    if not message:
        return None
    matched = [(name, weight) for name, pattern, weight in _SIGNALS if pattern.search(message)]
    if not matched:
        return None
    signals = tuple(name for name, _weight in matched)
    score = min(100, sum(weight for _name, weight in matched))
    repo_name = str(row.get("repo_name") or "unknown").strip() or "unknown"
    commit_sha = str(row.get("commit_sha") or "").strip()
    timestamp = str(row.get("timestamp") or "")
    topic = f"{repo_name}: {signals[0]} story from {_short_sha(commit_sha)}"
    note = (
        f"Commit {_short_sha(commit_sha)} in {repo_name} looks like {', '.join(signals)} work: "
        f"{_single_line(message)}. Use it as a concrete developer story about diagnosing, "
        "repairing, and preventing production regressions."
    )
    metadata = {
        "source": SOURCE_NAME,
        "source_type": "github_commit",
        "source_id": commit_sha,
        "commit_sha": commit_sha,
        "repo_name": repo_name,
        "commit_message": message,
        "timestamp": timestamp,
        "author": row.get("author"),
        "score": score,
        "signals": list(signals),
        "days": days,
    }
    return HotfixCommitIdeaCandidate(
        repo_name=repo_name,
        commit_sha=commit_sha,
        commit_message=message,
        timestamp=timestamp,
        author=row.get("author"),
        score=score,
        signals=signals,
        topic=topic,
        note=note,
        priority="high" if score >= 50 else "normal",
        source_metadata=metadata,
    )


def _load_recent_commits(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    columns = schema.get("github_commits", set())
    select_columns = [
        name
        for name in ("id", "repo_name", "commit_sha", "commit_message", "timestamp", "author")
        if name in columns
    ]
    if not {"repo_name", "commit_sha", "commit_message", "timestamp"}.issubset(columns):
        return []
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM github_commits
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC, commit_sha ASC""",
        (cutoff.isoformat(), now.isoformat()),
    ).fetchall()
    normalized = []
    for row in rows:
        item = dict(row)
        timestamp = _parse_time(item.get("timestamp"))
        if timestamp is None or timestamp < cutoff or timestamp > now:
            continue
        item["timestamp"] = timestamp.isoformat()
        if "author" not in item:
            item["author"] = None
        normalized.append(item)
    return normalized


def _existing_open_idea(
    conn: sqlite3.Connection,
    candidate: HotfixCommitIdeaCandidate,
) -> dict[str, Any] | None:
    schema = _schema(conn)
    if "content_ideas" not in schema or "source_metadata" not in schema["content_ideas"]:
        return None
    rows = conn.execute(
        """SELECT * FROM content_ideas
           WHERE status = 'open'
             AND source_metadata IS NOT NULL
           ORDER BY created_at ASC, id ASC"""
    ).fetchall()
    for row in rows:
        item = dict(row)
        metadata = _decode_json_object(item.get("source_metadata"))
        if (
            (item.get("source") == SOURCE_NAME or metadata.get("source") == SOURCE_NAME)
            and metadata.get("source_type") == "github_commit"
            and str(metadata.get("source_id")) == candidate.commit_sha
        ):
            return item
        if (
            (item.get("source") == SOURCE_NAME or metadata.get("source") == SOURCE_NAME)
            and str(metadata.get("commit_sha")) == candidate.commit_sha
        ):
            return item
    return None


def _insert_content_idea(db_or_conn: Any, candidate: HotfixCommitIdeaCandidate) -> int:
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
    candidate: HotfixCommitIdeaCandidate,
    idea_id: int | None,
    reason: str,
) -> HotfixCommitIdeaSeedResult:
    return HotfixCommitIdeaSeedResult(
        status=status,
        commit_sha=candidate.commit_sha,
        repo_name=candidate.repo_name,
        score=candidate.score,
        signals=candidate.signals,
        idea_id=int(idea_id) if idea_id is not None else None,
        reason=reason,
        topic=candidate.topic,
        note=candidate.note,
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
    required = {"repo_name", "commit_sha", "commit_message", "timestamp"}
    if "github_commits" not in schema:
        return {}
    missing = tuple(sorted(required - schema["github_commits"]))
    return {"github_commits": missing} if missing else {}


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _decode_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _single_line(value: str) -> str:
    return " ".join(value.split())


def _shorten(value: str | None, width: int) -> str:
    text = _single_line(value or "")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)].rstrip() + "..."


def _short_sha(value: str) -> str:
    return value[:12] if value else "unknown"
