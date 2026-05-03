"""Report GitHub commit progression into synthesis and publication."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
import io
import json
import sqlite3
from typing import Any


@dataclass(frozen=True)
class GitHubCommitSynthesisCoverageRow:
    """One repository/day coverage aggregate."""

    date: str
    repo: str
    ingested: int
    synthesized: int
    published: int
    uncovered: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GitHubCommitSynthesisCoverageReport:
    """Deterministic commit-to-content coverage report."""

    filters: dict[str, Any]
    rows: tuple[GitHubCommitSynthesisCoverageRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "github_commit_synthesis_coverage",
            "filters": dict(self.filters),
            "rows": [row.to_dict() for row in self.rows],
        }


def build_github_commit_synthesis_coverage_report(
    db_or_conn: Any,
    *,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    repo: str | None = None,
) -> GitHubCommitSynthesisCoverageReport:
    """Return per-repository/per-day commit synthesis coverage rows.

    A commit is considered synthesized when it is referenced by a content idea
    or by generated content. It is considered published when referenced by a
    generated content row with publication evidence.
    """
    start_day = _parse_date_filter(start_date, "start_date")
    end_day = _parse_date_filter(end_date, "end_date")
    if start_day and end_day and start_day > end_day:
        raise ValueError("start_date must be on or before end_date")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    commits = _load_commits(
        conn,
        schema,
        start_day=start_day,
        end_day=end_day,
        repo=repo,
    )
    commit_shas = {commit["commit_sha"] for commit in commits}

    idea_refs = _load_content_idea_commit_refs(conn, schema, commit_shas)
    generated_refs, published_refs = _load_generated_content_commit_refs(
        conn,
        schema,
        commit_shas,
    )
    synthesized_refs = idea_refs | generated_refs

    groups: dict[tuple[str, str], dict[str, set[str]]] = {}
    for commit in commits:
        key = (commit["date"], commit["repo"])
        group = groups.setdefault(
            key,
            {
                "ingested": set(),
                "synthesized": set(),
                "published": set(),
            },
        )
        sha = commit["commit_sha"]
        group["ingested"].add(sha)
        if sha in synthesized_refs:
            group["synthesized"].add(sha)
        if sha in published_refs:
            group["published"].add(sha)

    rows = []
    for (day, repo_name), group in sorted(groups.items(), key=lambda item: item[0]):
        ingested = len(group["ingested"])
        synthesized = len(group["synthesized"])
        published = len(group["published"])
        rows.append(
            GitHubCommitSynthesisCoverageRow(
                date=day,
                repo=repo_name,
                ingested=ingested,
                synthesized=synthesized,
                published=published,
                uncovered=ingested - synthesized,
            )
        )

    return GitHubCommitSynthesisCoverageReport(
        filters={
            "start_date": start_day.isoformat() if start_day else None,
            "end_date": end_day.isoformat() if end_day else None,
            "repo": repo,
        },
        rows=tuple(rows),
    )


def format_github_commit_synthesis_coverage_json(
    report: GitHubCommitSynthesisCoverageReport,
) -> str:
    """Serialize the coverage report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_github_commit_synthesis_coverage_csv(
    report: GitHubCommitSynthesisCoverageReport,
) -> str:
    """Serialize the coverage report rows as CSV."""
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=(
            "date",
            "repo",
            "ingested",
            "synthesized",
            "published",
            "uncovered",
        ),
        lineterminator="\n",
    )
    writer.writeheader()
    for row in report.rows:
        writer.writerow(row.to_dict())
    return output.getvalue().rstrip("\n")


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    return {
        str(table["name"]): {
            str(row["name"])
            for row in conn.execute(
                f"PRAGMA table_info({_quote_identifier(str(table['name']))})"
            )
        }
        for table in tables
    }


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _parse_date_filter(value: str | date | datetime | None, name: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD") from exc


def _day_bounds(
    start_day: date | None,
    end_day: date | None,
) -> tuple[str | None, str | None]:
    start = (
        datetime.combine(start_day, time.min, tzinfo=timezone.utc).isoformat()
        if start_day
        else None
    )
    end = (
        datetime.combine(
            end_day + timedelta(days=1),
            time.min,
            tzinfo=timezone.utc,
        ).isoformat()
        if end_day
        else None
    )
    return start, end


def _load_commits(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    start_day: date | None,
    end_day: date | None,
    repo: str | None,
) -> list[dict[str, str]]:
    columns = schema.get("github_commits", set())
    if not {"repo_name", "commit_sha", "timestamp"}.issubset(columns):
        return []

    where = ["commit_sha IS NOT NULL", "commit_sha != ''"]
    params: list[Any] = []
    start_bound, end_bound = _day_bounds(start_day, end_day)
    if start_bound:
        where.append("timestamp >= ?")
        params.append(start_bound)
    if end_bound:
        where.append("timestamp < ?")
        params.append(end_bound)
    if repo:
        where.append("repo_name = ?")
        params.append(repo)

    rows = conn.execute(
        f"""SELECT repo_name, commit_sha, timestamp
            FROM github_commits
            WHERE {' AND '.join(where)}
            ORDER BY timestamp ASC, repo_name ASC, commit_sha ASC""",
        tuple(params),
    ).fetchall()
    commits = []
    for row in rows:
        day = _timestamp_day(row["timestamp"])
        if day is None:
            continue
        commits.append(
            {
                "date": day,
                "repo": str(row["repo_name"] or ""),
                "commit_sha": str(row["commit_sha"]),
            }
        )
    return commits


def _timestamp_day(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return text[:10] if len(text) >= 10 else None


def _load_content_idea_commit_refs(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    commit_shas: set[str],
) -> set[str]:
    columns = schema.get("content_ideas", set())
    if not commit_shas or "source_metadata" not in columns:
        return set()
    rows = conn.execute(
        """SELECT source_metadata
           FROM content_ideas
           WHERE source_metadata IS NOT NULL
             AND source_metadata != ''"""
    ).fetchall()
    refs: set[str] = set()
    for row in rows:
        refs.update(
            _matching_refs(
                _extract_commit_refs(_parse_json(row["source_metadata"])),
                commit_shas,
            )
        )
    return refs


def _load_generated_content_commit_refs(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    commit_shas: set[str],
) -> tuple[set[str], set[str]]:
    columns = schema.get("generated_content", set())
    if not commit_shas or "source_commits" not in columns:
        return set(), set()

    publish_expression = _generated_content_publish_expression(columns)
    rows = conn.execute(
        f"""SELECT id, source_commits, {publish_expression} AS legacy_published
            FROM generated_content
            WHERE source_commits IS NOT NULL
              AND source_commits != ''"""
    ).fetchall()
    generated_refs: set[str] = set()
    candidate_published: set[str] = set()
    content_ids_by_sha: dict[str, set[int]] = {}
    for row in rows:
        refs = _matching_refs(_parse_json_list(row["source_commits"]), commit_shas)
        if not refs:
            continue
        generated_refs.update(refs)
        content_id = int(row["id"])
        for ref in refs:
            content_ids_by_sha.setdefault(ref, set()).add(content_id)
        if bool(row["legacy_published"]):
            candidate_published.update(refs)

    platform_published_content_ids = _published_content_ids(conn, schema)
    if platform_published_content_ids:
        for sha, content_ids in content_ids_by_sha.items():
            if content_ids & platform_published_content_ids:
                candidate_published.add(sha)

    return generated_refs, candidate_published


def _generated_content_publish_expression(columns: set[str]) -> str:
    checks = []
    if "published" in columns:
        checks.append("COALESCE(published, 0) = 1")
    if "published_at" in columns:
        checks.append("published_at IS NOT NULL AND published_at != ''")
    if "published_url" in columns:
        checks.append("published_url IS NOT NULL AND published_url != ''")
    return f"CASE WHEN {' OR '.join(checks)} THEN 1 ELSE 0 END" if checks else "0"


def _published_content_ids(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> set[int]:
    ids: set[int] = set()
    cp_columns = schema.get("content_publications", set())
    if {"content_id", "status"}.issubset(cp_columns):
        rows = conn.execute(
            """SELECT DISTINCT content_id
               FROM content_publications
               WHERE status = 'published'"""
        ).fetchall()
        ids.update(int(row["content_id"]) for row in rows if row["content_id"] is not None)

    pq_columns = schema.get("publish_queue", set())
    if {"content_id", "status"}.issubset(pq_columns):
        rows = conn.execute(
            """SELECT DISTINCT content_id
               FROM publish_queue
               WHERE status = 'published'"""
        ).fetchall()
        ids.update(int(row["content_id"]) for row in rows if row["content_id"] is not None)
    return ids


def _parse_json(value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return None


def _parse_json_list(value: Any) -> list[Any]:
    parsed = _parse_json(value)
    return parsed if isinstance(parsed, list) else []


def _extract_commit_refs(value: Any) -> list[str]:
    refs: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {
                "commit_sha",
                "commit_shas",
                "latest_commit_sha",
                "sha",
                "source_id",
                "source_ids",
            }:
                refs.extend(_extract_commit_refs(child))
        return refs
    if isinstance(value, list):
        for child in value:
            refs.extend(_extract_commit_refs(child))
        return refs
    if value is not None:
        refs.append(_normalize_ref(str(value)))
    return refs


def _matching_refs(refs: list[Any], commit_shas: set[str]) -> set[str]:
    normalized = {_normalize_ref(str(ref)) for ref in refs if ref is not None}
    return normalized & commit_shas


def _normalize_ref(value: str) -> str:
    text = value.strip()
    return text.removeprefix("commit:").strip()
