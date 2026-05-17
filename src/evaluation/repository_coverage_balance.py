"""Compare repository commit activity with published post attribution."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_DELTA_THRESHOLD = 0.2


@dataclass(frozen=True)
class RepositoryCoverageBalanceRow:
    repository: str
    commit_count: int
    attributed_post_count: int
    commit_share: float
    post_share: float
    coverage_delta: float
    coverage_status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RepositoryCoverageBalanceReport:
    generated_at: str
    filters: dict[str, Any]
    rows: tuple[RepositoryCoverageBalanceRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "repository_coverage_balance",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "rows": [row.to_dict() for row in self.rows],
        }


def build_repository_coverage_balance_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    delta_threshold: float = DEFAULT_DELTA_THRESHOLD,
    now: datetime | None = None,
) -> RepositoryCoverageBalanceReport:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if delta_threshold < 0:
        raise ValueError("delta_threshold must be non-negative")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    conn = _connection(db_or_conn)
    commit_repos = _recent_commit_repos(conn, cutoff)
    post_counts = _published_post_counts(conn, cutoff, commit_repos)
    commit_counts = Counter(commit_repos.values())
    repositories = sorted(set(commit_counts) | set(post_counts))
    total_commits = sum(commit_counts.values())
    total_posts = sum(post_counts.values())
    rows = []
    for repository in repositories:
        commit_count = commit_counts.get(repository, 0)
        post_count = post_counts.get(repository, 0)
        commit_share = round(commit_count / total_commits, 4) if total_commits else 0.0
        post_share = round(post_count / total_posts, 4) if total_posts else 0.0
        delta = round(post_share - commit_share, 4)
        if delta < -delta_threshold:
            status = "under_covered"
        elif delta > delta_threshold:
            status = "over_covered"
        else:
            status = "balanced"
        rows.append(
            RepositoryCoverageBalanceRow(
                repository=repository,
                commit_count=commit_count,
                attributed_post_count=post_count,
                commit_share=commit_share,
                post_share=post_share,
                coverage_delta=delta,
                coverage_status=status,
            )
        )
    rows.sort(key=lambda row: (_severity_rank(row.coverage_status), row.repository))
    return RepositoryCoverageBalanceReport(
        generated_at=generated_at.isoformat(),
        filters={
            "lookback_days": lookback_days,
            "delta_threshold": delta_threshold,
            "lookback_start": cutoff.isoformat(),
        },
        rows=tuple(rows),
    )


def format_repository_coverage_balance_json(report: RepositoryCoverageBalanceReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_repository_coverage_balance_table(report: RepositoryCoverageBalanceReport) -> str:
    lines = [
        "Repository Coverage Balance",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['lookback_days']} days",
        "",
        "repository | commit_count | attributed_post_count | commit_share | post_share | coverage_delta | coverage_status",
    ]
    if not report.rows:
        lines.append("No repository activity or attributed posts found.")
        return "\n".join(lines)
    for row in report.rows:
        lines.append(
            " | ".join(
                [
                    row.repository,
                    str(row.commit_count),
                    str(row.attributed_post_count),
                    f"{row.commit_share:.4f}",
                    f"{row.post_share:.4f}",
                    f"{row.coverage_delta:.4f}",
                    row.coverage_status,
                ]
            )
        )
    return "\n".join(lines)


def _recent_commit_repos(conn: sqlite3.Connection, cutoff: datetime) -> dict[str, str]:
    if not _has_table(conn, "github_commits") or {"commit_sha", "repo_name", "timestamp"} - _columns(conn, "github_commits"):
        return {}
    rows = conn.execute(
        """SELECT commit_sha, repo_name
           FROM github_commits
           WHERE datetime(timestamp) >= datetime(?)
           ORDER BY repo_name ASC, commit_sha ASC""",
        (cutoff.isoformat(),),
    ).fetchall()
    return {str(row["commit_sha"]): str(row["repo_name"]) for row in rows}


def _published_post_counts(conn: sqlite3.Connection, cutoff: datetime, commit_repos: dict[str, str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    if not _has_table(conn, "generated_content") or {"id", "source_commits", "published", "published_at"} - _columns(conn, "generated_content"):
        return counts
    fallback_repos = _all_commit_repos(conn)
    rows = conn.execute(
        """SELECT id, source_commits
           FROM generated_content
           WHERE COALESCE(published, 0) = 1
             AND published_at IS NOT NULL
             AND datetime(published_at) >= datetime(?)
           ORDER BY id ASC""",
        (cutoff.isoformat(),),
    ).fetchall()
    for row in rows:
        repos = {
            commit_repos.get(str(sha)) or fallback_repos.get(str(sha))
            for sha in _json_list(row["source_commits"])
        }
        for repository in sorted(repo for repo in repos if repo):
            counts[repository] += 1
    return counts


def _all_commit_repos(conn: sqlite3.Connection) -> dict[str, str]:
    if not _has_table(conn, "github_commits") or {"commit_sha", "repo_name"} - _columns(conn, "github_commits"):
        return {}
    rows = conn.execute("SELECT commit_sha, repo_name FROM github_commits").fetchall()
    return {str(row["commit_sha"]): str(row["repo_name"]) for row in rows}


def _json_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if not raw:
        return []
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _severity_rank(status: str) -> int:
    return {"under_covered": 0, "over_covered": 1, "balanced": 2}.get(status, 9)
