"""Report Claude session linkage coverage for real work artifacts."""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_MIN_CLAUDE_MESSAGES = 3


def build_claude_work_coverage_report(
    db_or_conn: Any,
    *,
    days: int = 14,
    repo: str | None = None,
    min_commits: int = 3,
    limit: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Summarize whether recent commits have enough Claude session context."""
    if days <= 0:
        raise ValueError("days must be greater than zero")
    if min_commits <= 0:
        raise ValueError("min_commits must be greater than zero")
    if limit < 0:
        raise ValueError("limit must be zero or greater")

    conn = _connection(db_or_conn)
    generated_at = _aware(now or datetime.now(timezone.utc))
    since = generated_at - timedelta(days=days)

    commits = _load_commits(conn, since, repo)
    messages = _load_messages(conn, since, repo)
    linked_commit_ids = _linked_commit_ids(conn, commits)
    linked_message_ids = _linked_message_ids(conn, commits)
    loaded_message_ids = {message["id"] for message in messages}
    linked_loaded_message_ids = linked_message_ids & loaded_message_ids

    daily = _daily_rows(commits, messages, linked_commit_ids, linked_message_ids)
    unlinked_commit_heavy_days = [
        row
        for row in daily
        if row["commit_count"] >= min_commits
        and row["unlinked_commit_count"] > 0
    ]
    claude_heavy_days_without_commits = [
        row
        for row in daily
        if row["claude_message_count"] >= DEFAULT_MIN_CLAUDE_MESSAGES
        and row["linked_commit_count"] == 0
    ]

    unlinked_commits = [
        _commit_item(commit)
        for commit in sorted(
            commits,
            key=lambda row: (row["timestamp"], row["id"]),
            reverse=True,
        )
        if commit["id"] not in linked_commit_ids
    ][:limit]
    unlinked_messages = [
        _message_item(message)
        for message in sorted(
            messages,
            key=lambda row: (row["timestamp"], row["id"]),
            reverse=True,
        )
        if message["id"] not in linked_message_ids
    ][:limit]

    return {
        "generated_at": generated_at.isoformat(),
        "window": {
            "days": days,
            "since": since.isoformat(),
            "repo": repo,
            "min_commits": min_commits,
            "limit": limit,
            "min_claude_messages": DEFAULT_MIN_CLAUDE_MESSAGES,
        },
        "summary": {
            "commit_count": len(commits),
            "claude_message_count": len(messages),
            "claude_session_count": len(
                {
                    message["session_id"] or "unknown"
                    for message in messages
                }
            ),
            "linked_commit_count": len(linked_commit_ids),
            "unlinked_commit_count": len(commits) - len(linked_commit_ids),
            "linked_claude_message_count": len(linked_loaded_message_ids),
            "unlinked_commit_heavy_day_count": len(unlinked_commit_heavy_days),
            "claude_heavy_day_without_commits_count": len(
                claude_heavy_days_without_commits
            ),
        },
        "daily": daily,
        "unlinked_commit_heavy_days": unlinked_commit_heavy_days,
        "claude_heavy_days_without_commits": claude_heavy_days_without_commits,
        "top_unlinked_commits": unlinked_commits,
        "top_unlinked_messages": unlinked_messages,
    }


def format_claude_work_coverage_text(report: dict[str, Any]) -> str:
    """Format a Claude work coverage report for terminal output."""
    window = report["window"]
    summary = report["summary"]
    lines = [
        "Claude Work Coverage",
        f"Window: last {window['days']} days since {window['since']}",
    ]
    if window.get("repo"):
        lines.append(f"Repository: {window['repo']}")
    lines.extend(
        [
            (
                "Summary: "
                f"{summary['linked_commit_count']}/{summary['commit_count']} "
                "commits linked, "
                f"{summary['claude_message_count']} Claude messages across "
                f"{summary['claude_session_count']} sessions"
            ),
            "",
            "Daily:",
        ]
    )
    if not report["daily"]:
        lines.append("  - none")
    for row in report["daily"]:
        lines.append(
            f"  - {row['day']}: commits={row['commit_count']} "
            f"linked={row['linked_commit_count']} "
            f"unlinked={row['unlinked_commit_count']} "
            f"claude_messages={row['claude_message_count']} "
            f"sessions={row['claude_session_count']}"
        )

    _append_day_section(
        lines,
        "Commit-heavy days with unlinked commits:",
        report["unlinked_commit_heavy_days"],
    )
    _append_day_section(
        lines,
        "Claude-heavy days with no linked commits:",
        report["claude_heavy_days_without_commits"],
    )

    lines.append("")
    lines.append("Top unlinked commits:")
    if not report["top_unlinked_commits"]:
        lines.append("  - none")
    for item in report["top_unlinked_commits"]:
        lines.append(
            f"  - {item['timestamp']} {item['repo_name']} "
            f"{item['commit_sha']}: {_shorten(item['commit_message'])}"
        )

    lines.append("")
    lines.append("Top unlinked Claude messages:")
    if not report["top_unlinked_messages"]:
        lines.append("  - none")
    for item in report["top_unlinked_messages"]:
        lines.append(
            f"  - {item['timestamp']} {item['session_id']} "
            f"{item['message_uuid']}: {_shorten(item['prompt_text'])}"
        )

    return "\n".join(lines)


def _append_day_section(
    lines: list[str],
    title: str,
    days: list[dict[str, Any]],
) -> None:
    lines.append("")
    lines.append(title)
    if not days:
        lines.append("  - none")
        return
    for row in days:
        lines.append(
            f"  - {row['day']}: commits={row['commit_count']} "
            f"linked={row['linked_commit_count']} "
            f"claude_messages={row['claude_message_count']}"
        )


def _load_commits(
    conn: sqlite3.Connection,
    since: datetime,
    repo: str | None,
) -> list[dict[str, Any]]:
    where = ["timestamp >= ?"]
    params: list[Any] = [_db_time(since)]
    if repo:
        where.append("repo_name = ?")
        params.append(repo)
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT id, repo_name, commit_sha, commit_message, timestamp, author
                FROM github_commits
                WHERE {' AND '.join(where)}
                ORDER BY timestamp ASC, id ASC""",
            tuple(params),
        ).fetchall()
    ]


def _load_messages(
    conn: sqlite3.Connection,
    since: datetime,
    repo: str | None,
) -> list[dict[str, Any]]:
    params: list[Any] = [_db_time(since)]
    repo_clause = ""
    if repo:
        repo_slug = repo.rsplit("/", 1)[-1]
        repo_clause = """
             AND (
                 project_path LIKE ?
                 OR id IN (
                     SELECT cpl.message_id
                     FROM commit_prompt_links cpl
                     JOIN github_commits gc ON gc.id = cpl.commit_id
                     WHERE gc.repo_name = ?
                 )
             )"""
        params.extend([f"%{repo_slug}%", repo])

    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT id, session_id, message_uuid, project_path, timestamp, prompt_text
                FROM claude_messages
                WHERE timestamp >= ?
                {repo_clause}
                ORDER BY timestamp ASC, id ASC""",
            tuple(params),
        ).fetchall()
    ]


def _linked_commit_ids(
    conn: sqlite3.Connection,
    commits: list[dict[str, Any]],
) -> set[int]:
    commit_ids = [commit["id"] for commit in commits]
    if not commit_ids:
        return set()
    placeholders = ",".join("?" for _ in commit_ids)
    rows = conn.execute(
        f"""SELECT DISTINCT commit_id
            FROM commit_prompt_links
            WHERE commit_id IN ({placeholders})""",
        tuple(commit_ids),
    ).fetchall()
    return {int(row["commit_id"]) for row in rows}


def _linked_message_ids(
    conn: sqlite3.Connection,
    commits: list[dict[str, Any]],
) -> set[int]:
    commit_ids = [commit["id"] for commit in commits]
    if not commit_ids:
        return set()
    placeholders = ",".join("?" for _ in commit_ids)
    rows = conn.execute(
        f"""SELECT DISTINCT message_id
            FROM commit_prompt_links
            WHERE commit_id IN ({placeholders})""",
        tuple(commit_ids),
    ).fetchall()
    return {int(row["message_id"]) for row in rows}


def _daily_rows(
    commits: list[dict[str, Any]],
    messages: list[dict[str, Any]],
    linked_commit_ids: set[int],
    linked_message_ids: set[int],
) -> list[dict[str, Any]]:
    by_day: dict[str, dict[str, Any]] = defaultdict(_empty_day)
    for commit in commits:
        day = _day(commit["timestamp"])
        bucket = by_day[day]
        bucket["day"] = day
        bucket["commit_count"] += 1
        if commit["id"] in linked_commit_ids:
            bucket["linked_commit_count"] += 1

    sessions_by_day: dict[str, set[str]] = defaultdict(set)
    for message in messages:
        day = _day(message["timestamp"])
        bucket = by_day[day]
        bucket["day"] = day
        bucket["claude_message_count"] += 1
        if message["id"] in linked_message_ids:
            bucket["linked_claude_message_count"] += 1
        sessions_by_day[day].add(message["session_id"] or "unknown")

    rows = []
    for day in sorted(by_day):
        row = by_day[day]
        row["claude_session_count"] = len(sessions_by_day.get(day, set()))
        row["unlinked_commit_count"] = (
            row["commit_count"] - row["linked_commit_count"]
        )
        row["unlinked_claude_message_count"] = (
            row["claude_message_count"] - row["linked_claude_message_count"]
        )
        rows.append(row)
    return rows


def _empty_day() -> dict[str, Any]:
    return {
        "day": "",
        "commit_count": 0,
        "linked_commit_count": 0,
        "unlinked_commit_count": 0,
        "claude_message_count": 0,
        "linked_claude_message_count": 0,
        "unlinked_claude_message_count": 0,
        "claude_session_count": 0,
    }


def _commit_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "repo_name": row["repo_name"],
        "commit_sha": row["commit_sha"],
        "commit_message": row["commit_message"],
        "timestamp": row["timestamp"],
        "author": row["author"],
    }


def _message_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "session_id": row["session_id"],
        "message_uuid": row["message_uuid"],
        "project_path": row["project_path"],
        "timestamp": row["timestamp"],
        "prompt_text": row["prompt_text"],
    }


def _shorten(value: Any, width: int = 96) -> str:
    text = str(value or "").replace("\n", " ").strip()
    return text if len(text) <= width else text[: max(0, width - 3)] + "..."


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if conn is None:
        raise ValueError("database connection is not available")
    return conn


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _db_time(value: datetime) -> str:
    return _aware(value).isoformat()


def _day(timestamp: str) -> str:
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).date().isoformat()
