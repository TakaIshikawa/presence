"""Report configured repositories with quiet commit ingestion windows."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 100


def build_commit_ingestion_quiet_repos_report(
    commit_rows: list[dict[str, Any]],
    *,
    repositories: list[Any] | tuple[Any, ...] | None = None,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Find configured or historically active repos without recent ingested commits."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    configured = set(_normalize_repositories(repositories or []))
    historical = {_repo_name(row) for row in commit_rows if _repo_name(row)}
    repo_names = sorted(configured | historical)
    recent_counts: Counter[str] = Counter()
    historical_counts: Counter[str] = Counter()
    last_commits: dict[str, dict[str, Any]] = {}
    had_old_commit: set[str] = set()

    for row in commit_rows:
        repo = _repo_name(row)
        if not repo:
            continue
        commit_at = _parse_dt(_first(row, "timestamp", "committed_at", "created_at"))
        historical_counts[repo] += 1
        if commit_at is not None and (repo not in last_commits or commit_at > last_commits[repo]["timestamp_dt"]):
            last_commits[repo] = {
                "timestamp_dt": commit_at,
                "commit_sha": _text(_first(row, "commit_sha", "sha")),
                "commit_message": _text(_first(row, "commit_message", "message")),
                "author": _text(row.get("author")),
            }
        if commit_at is not None and commit_at < cutoff:
            had_old_commit.add(repo)
        if commit_at is not None and cutoff <= commit_at <= generated_at:
            recent_counts[repo] += 1

    rows = []
    for repo in repo_names:
        recent_count = recent_counts.get(repo, 0)
        last = last_commits.get(repo)
        status = _status(recent_count, historical_counts.get(repo, 0), repo in had_old_commit)
        if status in {"active", "recently_resumed"}:
            continue
        quiet_days = None
        if last:
            quiet_days = round(max((generated_at - last["timestamp_dt"]).total_seconds() / 86400, 0), 2)
        rows.append(
            {
                "repository": repo,
                "configured": repo in configured,
                "status": status,
                "ingested_commit_count": recent_count,
                "historical_commit_count": historical_counts.get(repo, 0),
                "last_commit_timestamp": _iso(last["timestamp_dt"]) if last else None,
                "quiet_days": quiet_days,
                "last_commit_sha": last["commit_sha"] if last else None,
                "last_commit_message": last["commit_message"][:160] if last else None,
                "last_commit_author": last["author"] if last else None,
            }
        )

    rows.sort(key=lambda row: (_status_rank(row["status"]), row["quiet_days"] if row["quiet_days"] is not None else 10**9, row["repository"]), reverse=True)
    rows = rows[:limit]
    status_counts = Counter(row["status"] for row in rows)
    return {
        "artifact_type": "commit_ingestion_quiet_repos",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "lookback_start": cutoff.isoformat()},
        "summary": {
            "repository_count": len(repo_names),
            "configured_repository_count": len(configured),
            "quiet_repository_count": len(rows),
            "no_history_count": status_counts.get("no_history", 0),
            "quiet_count": status_counts.get("quiet", 0),
        },
        "rows": rows,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_commit_ingestion_quiet_repos_report_from_db(
    db_or_conn: Any,
    *,
    repositories: list[Any] | tuple[Any, ...] | None = None,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load GitHub commit rows and build the quiet repositories report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_commits(conn, schema) if not gaps["missing_tables"] and not gaps["missing_columns"] else []
    return build_commit_ingestion_quiet_repos_report(
        rows,
        repositories=repositories,
        days=days,
        limit=limit,
        now=now,
        schema_gaps=gaps,
    )


def format_commit_ingestion_quiet_repos_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_commit_ingestion_quiet_repos_text(report: dict[str, Any]) -> str:
    lines = [
        "Commit Ingestion Quiet Repos",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days",
        (
            "Totals: "
            f"repositories={report['summary']['repository_count']} "
            f"quiet={report['summary']['quiet_repository_count']} "
            f"no_history={report['summary']['no_history_count']}"
        ),
    ]
    if not report["rows"]:
        lines.extend(["", "No quiet commit-ingestion repositories found."])
        return "\n".join(lines)
    lines.extend(["", "repository                    status      recent  quiet_days  last_commit"])
    for row in report["rows"]:
        lines.append(
            f"{row['repository'][:29]:<29} "
            f"{row['status']:<11} "
            f"{row['ingested_commit_count']:<7} "
            f"{_fmt_days(row['quiet_days']):<10} "
            f"{row['last_commit_timestamp'] or '-'}"
        )
    return "\n".join(lines)


def _load_commits(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("github_commits", set())
    selected = [
        _select(columns, ("repo_name", "repository", "repo"), "repo_name"),
        _select(columns, ("commit_sha", "sha"), "commit_sha"),
        _select(columns, ("commit_message", "message"), "commit_message"),
        _select(columns, ("timestamp", "committed_at", "created_at"), "timestamp"),
        _select(columns, ("author",), "author"),
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(selected)}
                FROM github_commits
                ORDER BY timestamp ASC, repo_name ASC, commit_sha ASC"""
        ).fetchall()
    ]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "github_commits" not in schema:
        return {"missing_tables": ["github_commits"], "missing_columns": {}}
    missing = sorted({"repo_name", "timestamp"} - schema["github_commits"])
    return {"missing_tables": [], "missing_columns": {"github_commits": missing} if missing else {}}


def _normalize_repositories(repositories: list[Any] | tuple[Any, ...]) -> list[str]:
    normalized = []
    for repo in repositories:
        if isinstance(repo, str):
            name = repo
        elif isinstance(repo, dict):
            name = _text(repo.get("full_name") or repo.get("repo_name") or repo.get("name"))
            owner = _text(repo.get("owner"))
            if owner and name and "/" not in name:
                name = f"{owner}/{name}"
        else:
            name = _text(getattr(repo, "full_name", "") or getattr(repo, "repo_name", "") or getattr(repo, "name", ""))
            owner = _text(getattr(repo, "owner", ""))
            if owner and name and "/" not in name:
                name = f"{owner}/{name}"
        name = _text(name).strip("/")
        if name:
            normalized.append(name)
    return sorted(set(normalized))


def _status(recent_count: int, historical_count: int, had_old_commit: bool) -> str:
    if historical_count == 0:
        return "no_history"
    if recent_count > 0 and had_old_commit:
        return "recently_resumed"
    if recent_count > 0:
        return "active"
    return "quiet"


def _status_rank(status: str) -> int:
    return {"no_history": 2, "quiet": 1}.get(status, 0)


def _repo_name(row: dict[str, Any]) -> str:
    return _text(_first(row, "repo_name", "repository", "repo"))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _fmt_days(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}"
