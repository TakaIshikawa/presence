"""Export newsletter section seeds from GitHub release activity."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_MIN_BODY_LENGTH = 80
SUMMARY_MAX_CHARS = 420
RELEASE_ACTIVITY_TYPES = ("release", "github_release", "release_published")

_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
_MARKDOWN_PREFIX_RE = re.compile(r"^\s*(?:#{1,6}\s*|[-*+]\s+|\d+[.)]\s*)")
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class ReleaseNewsletterSeedCandidate:
    """One release-derived candidate section for newsletter planning."""

    rank: int
    repo: str
    release_title: str
    url: str
    summary_text: str
    source_activity_id: str
    released_at: str
    tag_name: str
    body_length: int
    score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReleaseNewsletterSeedReport:
    """Read-only export artifact for release newsletter seeds."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    seeds: tuple[ReleaseNewsletterSeedCandidate, ...]
    availability: dict[str, bool]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "availability": dict(sorted(self.availability.items())),
            "filters": self.filters,
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "seeds": [seed.to_dict() for seed in self.seeds],
            "totals": self.totals,
        }


def build_release_newsletter_seed_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    repo: str | None = None,
    min_body_length: int = DEFAULT_MIN_BODY_LENGTH,
    now: datetime | None = None,
) -> ReleaseNewsletterSeedReport:
    """Build ranked newsletter seed candidates from ingested GitHub releases."""

    if days <= 0:
        raise ValueError("days must be positive")
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

    rows = _load_release_rows(
        conn,
        schema,
        cutoff=cutoff,
        repo=repo,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    candidates = [
        candidate
        for row in rows
        if (candidate := _row_to_candidate(row, now=generated_at, days=days))
        and candidate.body_length >= min_body_length
    ]
    ranked = tuple(
        _with_rank(seed, index)
        for index, seed in enumerate(
            sorted(
                candidates,
                key=lambda item: (
                    -item.score,
                    -_sort_timestamp(item.released_at),
                    item.repo,
                    item.tag_name,
                    item.source_activity_id,
                ),
            ),
            start=1,
        )
    )

    return ReleaseNewsletterSeedReport(
        artifact_type="release_newsletter_seed_export",
        generated_at=generated_at.isoformat(),
        filters={"days": days, "repo": repo, "min_body_length": min_body_length},
        totals={
            "scanned": len(rows),
            "eligible": len(ranked),
            "excluded_by_body_length": len(rows) - len(ranked),
        },
        seeds=ranked,
        availability={"github_activity": "github_activity" in schema},
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_release_newsletter_seed_json(report: ReleaseNewsletterSeedReport) -> str:
    """Serialize a release seed report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_release_newsletter_seed_text(report: ReleaseNewsletterSeedReport) -> str:
    """Format release newsletter seed candidates for terminal review."""

    lines = [
        "Release Newsletter Seeds",
        f"Generated: {report.generated_at}",
        (
            f"Filters: days={report.filters['days']} "
            f"repo={report.filters['repo'] or 'all'} "
            f"min_body_length={report.filters['min_body_length']}"
        ),
        (
            f"Candidates: {report.totals['eligible']} "
            f"(scanned={report.totals['scanned']}, "
            f"excluded_by_body_length={report.totals['excluded_by_body_length']})"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if not report.seeds:
        lines.append("No release newsletter seed candidates found.")
        return "\n".join(lines)

    lines.append("Seeds:")
    for seed in report.seeds:
        lines.append(
            f"- {seed.rank}. {seed.repo} {seed.tag_name} "
            f"score={seed.score:.2f} date={seed.released_at}"
        )
        lines.append(f"  title: {seed.release_title}")
        if seed.url:
            lines.append(f"  url: {seed.url}")
        lines.append(f"  source: {seed.source_activity_id}")
        lines.append(f"  summary: {seed.summary_text}")
    return "\n".join(lines)


def _load_release_rows(
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

    required = {
        "repo_name",
        "activity_type",
        "number",
        "title",
        "body",
        "url",
        "updated_at",
        "created_at_github",
        "metadata",
    }
    available = schema["github_activity"]
    missing = tuple(sorted(required - available))
    if missing:
        missing_columns["github_activity"] = missing
        return []

    params: list[Any] = [cutoff.isoformat(), *RELEASE_ACTIVITY_TYPES]
    repo_clause = ""
    if repo:
        repo_clause = " AND repo_name = ?"
        params.append(repo)
    placeholders = ", ".join("?" for _ in RELEASE_ACTIVITY_TYPES)
    rows = _fetch_dicts(
        conn,
        f"""SELECT id, repo_name, activity_type, number, title, body, url,
                  updated_at, created_at_github, metadata
           FROM github_activity
           WHERE updated_at >= ?
             AND activity_type IN ({placeholders}){repo_clause}
           ORDER BY updated_at DESC, id DESC""",
        params,
    )
    return rows


def _row_to_candidate(
    row: dict[str, Any],
    *,
    now: datetime,
    days: int,
) -> ReleaseNewsletterSeedCandidate | None:
    metadata = _metadata(row.get("metadata"))
    body = _text(row.get("body"))
    body_length = len(_WHITESPACE_RE.sub(" ", body).strip())
    released_at_dt = _release_datetime(row, metadata)
    if released_at_dt is None:
        return None
    released_at = released_at_dt.isoformat()
    tag_name = _text(metadata.get("tag_name") or row.get("number"))
    title = _text(row.get("title") or metadata.get("name") or tag_name)
    repo = _text(row.get("repo_name"))
    activity_type = _text(row.get("activity_type") or "release")
    source_activity_id = _text(
        metadata.get("activity_id") or f"{repo}#{_text(row.get('number'))}:{activity_type}"
    )
    summary = _summary_text(body, title)
    url = _text(row.get("url") or metadata.get("html_url"))
    score = _score_release(body_length=body_length, released_at=released_at_dt, now=now, days=days)

    return ReleaseNewsletterSeedCandidate(
        rank=0,
        repo=repo,
        release_title=title,
        url=url,
        summary_text=summary,
        source_activity_id=source_activity_id,
        released_at=released_at,
        tag_name=tag_name,
        body_length=body_length,
        score=score,
    )


def _with_rank(seed: ReleaseNewsletterSeedCandidate, rank: int) -> ReleaseNewsletterSeedCandidate:
    return ReleaseNewsletterSeedCandidate(
        rank=rank,
        repo=seed.repo,
        release_title=seed.release_title,
        url=seed.url,
        summary_text=seed.summary_text,
        source_activity_id=seed.source_activity_id,
        released_at=seed.released_at,
        tag_name=seed.tag_name,
        body_length=seed.body_length,
        score=seed.score,
    )


def _summary_text(body: str, fallback: str) -> str:
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
        if not line:
            continue
        if line.lower() in {"what's changed", "whats changed", "changes", "changelog"}:
            continue
        lines.append(line)
        if len(" ".join(lines)) >= SUMMARY_MAX_CHARS:
            break

    summary = " ".join(lines) or fallback
    if len(summary) > SUMMARY_MAX_CHARS:
        return summary[: SUMMARY_MAX_CHARS - 3].rstrip() + "..."
    return summary


def _score_release(*, body_length: int, released_at: datetime, now: datetime, days: int) -> float:
    age_days = max(0.0, (now - released_at).total_seconds() / 86400)
    freshness = max(0.0, (days - age_days) / days) * 100.0
    richness = min(body_length, 2400) / 2400 * 35.0
    return round(freshness + richness, 4)


def _release_datetime(row: dict[str, Any], metadata: dict[str, Any]) -> datetime | None:
    for value in (
        metadata.get("published_at"),
        metadata.get("created_at"),
        row.get("created_at_github"),
        row.get("updated_at"),
    ):
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed
    return None


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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tables = []
    for row in rows:
        tables.append(row["name"] if hasattr(row, "keys") else row[0])
    return {
        table: {
            column["name"] if hasattr(column, "keys") else column[1]
            for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for table in tables
    }


def _fetch_dicts(
    conn: sqlite3.Connection,
    query: str,
    params: list[Any],
) -> list[dict[str, Any]]:
    cursor = conn.execute(query, tuple(params))
    columns = [column[0] for column in cursor.description or []]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
