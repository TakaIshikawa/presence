"""Rank GitHub activity that may need a response or content follow-up."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 20
ACTIVITY_TYPE_ALL = "all"
SUPPORTED_ACTIVITY_TYPES = (
    "issue",
    "pull_request",
    "discussion",
    "issue_comment",
    "review_comment",
    "discussion_comment",
    "pull_request_review",
)

MALFORMED_LABELS = "malformed_labels_json"
MALFORMED_METADATA = "malformed_metadata_json"
MALFORMED_SOURCE_ACTIVITY_IDS = "malformed_source_activity_ids_json"
NON_LIST_LABELS = "non_list_labels"
NON_OBJECT_METADATA = "non_object_metadata"
NON_LIST_SOURCE_ACTIVITY_IDS = "non_list_source_activity_ids"

_LABEL_HINTS = {
    "bug": 22,
    "question": 20,
    "help wanted": 18,
    "support": 16,
    "needs triage": 14,
    "needs response": 22,
    "docs": 8,
    "documentation": 8,
}
_TYPE_WEIGHTS = {
    "issue": 24,
    "discussion": 22,
    "pull_request": 18,
    "issue_comment": 16,
    "discussion_comment": 16,
    "review_comment": 14,
    "pull_request_review": 14,
}
_COMMENT_TYPES = {"issue_comment", "review_comment", "discussion_comment", "pull_request_review"}
_CLOSED_STATES = {"closed", "merged", "answered", "completed", "resolved"}
_BOT_AUTHORS = {"dependabot[bot]", "github-actions[bot]", "renovate[bot]"}


@dataclass(frozen=True)
class GitHubActivityResponseBacklogItem:
    activity_id: str
    repo_name: str
    activity_type: str
    number: str
    title: str
    url: str | None
    age_days: float | None
    labels: tuple[str, ...]
    score: float
    reasons: tuple[str, ...]
    covered: bool = False
    state: str | None = None
    author: str | None = None
    updated_at: str | None = None
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["labels"] = list(self.labels)
        payload["reasons"] = list(self.reasons)
        payload["warnings"] = list(self.warnings)
        return payload


@dataclass(frozen=True)
class GitHubActivityResponseBacklogReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    items: tuple[GitHubActivityResponseBacklogItem, ...]
    warnings: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "github_activity_response_backlog",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "items": [item.to_dict() for item in self.items],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
            "warnings": list(self.warnings),
        }


def build_github_activity_response_backlog_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    repo: str | None = None,
    activity_type: str = ACTIVITY_TYPE_ALL,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> GitHubActivityResponseBacklogReport:
    """Build a ranked backlog of GitHub activity likely to need follow-up."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    normalized_repo = _clean_optional(repo)
    if activity_type not in (ACTIVITY_TYPE_ALL, *SUPPORTED_ACTIVITY_TYPES):
        supported = ", ".join((ACTIVITY_TYPE_ALL, *SUPPORTED_ACTIVITY_TYPES))
        raise ValueError(f"activity_type must be one of {supported}")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(days=days)
    selected_types = (
        SUPPORTED_ACTIVITY_TYPES
        if activity_type == ACTIVITY_TYPE_ALL
        else (activity_type,)
    )
    filters = {
        "activity_type": activity_type,
        "days": days,
        "limit": limit,
        "repo": normalized_repo,
        "window_end": generated_at.isoformat(),
        "window_start": window_start.isoformat(),
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if "github_activity" in missing_tables or missing_columns.get("github_activity"):
        return GitHubActivityResponseBacklogReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_empty_totals(),
            items=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    warnings: list[str] = []
    covered_ids = _covered_activity_ids(conn, schema, warnings)
    rows = _load_activity_rows(
        conn,
        activity_types=selected_types,
        repo=normalized_repo,
        window_start=window_start,
    )
    candidates = [
        _row_to_item(row, now=generated_at, covered_ids=covered_ids, warnings=warnings)
        for row in rows
    ]
    candidates.sort(key=_item_sort_key)
    limited = tuple(candidates[:limit])

    totals = {
        "candidate_count": len(candidates),
        "covered_count": sum(1 for item in candidates if item.covered),
        "emitted_count": len(limited),
        "uncovered_count": sum(1 for item in candidates if not item.covered),
        "warning_count": len(set(warnings)),
    }

    return GitHubActivityResponseBacklogReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=totals,
        items=limited,
        warnings=tuple(sorted(set(warnings))),
        missing_tables=tuple(table for table in missing_tables if table != "generated_content"),
        missing_columns=missing_columns,
    )


def format_github_activity_response_backlog_json(
    report: GitHubActivityResponseBacklogReport,
) -> str:
    """Serialize a backlog report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_github_activity_response_backlog_text(
    report: GitHubActivityResponseBacklogReport,
) -> str:
    """Format a backlog report for terminal review."""
    lines = [
        "GitHub Activity Response Backlog",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['window_start']} to "
            f"{report.filters['window_end']} ({report.filters['days']} days)"
        ),
        f"Activity type: {report.filters['activity_type']}",
        f"Limit: {report.filters['limit']}",
    ]
    if report.filters.get("repo"):
        lines.append(f"Repo: {report.filters['repo']}")
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
        return "\n".join(lines)
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
        return "\n".join(lines)

    totals = report.totals
    lines.extend(
        [
            (
                "Totals: "
                f"candidates={totals['candidate_count']} "
                f"uncovered={totals['uncovered_count']} "
                f"covered={totals['covered_count']} "
                f"warnings={totals['warning_count']}"
            ),
            "",
            "Backlog:",
        ]
    )
    if not report.items:
        lines.append("  None.")
    for item in report.items:
        covered = "covered" if item.covered else "uncovered"
        labels = ",".join(item.labels) if item.labels else "-"
        lines.append(
            "  - "
            f"{item.repo_name} {item.activity_type} #{item.number} "
            f"score={item.score:g} age={_format_days(item.age_days)} {covered} "
            f"labels={labels} title={_shorten(item.title, 72)}"
        )
        lines.append(f"    reasons={'; '.join(item.reasons)} url={item.url or '-'}")
        if item.warnings:
            lines.append(f"    warnings={'; '.join(item.warnings)}")
    if report.warnings:
        lines.append("")
        lines.append("Warnings:")
        lines.extend(f"  - {warning}" for warning in report.warnings)
    return "\n".join(lines)


def _row_to_item(
    row: dict[str, Any],
    *,
    now: datetime,
    covered_ids: set[str],
    warnings: list[str],
) -> GitHubActivityResponseBacklogItem:
    repo_name = _clean(row.get("repo_name"))
    activity_type = _clean(row.get("activity_type"))
    number = _clean(row.get("number"))
    activity_id = _activity_id(repo_name, number, activity_type)
    labels, label_warnings = _parse_labels(row.get("labels"), activity_id)
    metadata, metadata_warnings = _parse_metadata(row.get("metadata"), activity_id)
    warnings.extend(label_warnings)
    warnings.extend(metadata_warnings)
    item_warnings = (*label_warnings, *metadata_warnings)
    updated_at = _parse_datetime(row.get("updated_at"))
    created_at = _parse_datetime(row.get("created_at_github")) or updated_at
    age_days = _age_days(created_at or updated_at, now)
    score, reasons = _score_row(
        row,
        labels=labels,
        metadata=metadata,
        age_days=age_days,
        covered=activity_id in covered_ids,
    )
    return GitHubActivityResponseBacklogItem(
        activity_id=activity_id,
        repo_name=repo_name,
        activity_type=activity_type,
        number=number,
        title=_clean(row.get("title")) or "GitHub activity",
        url=_clean_optional(row.get("url")),
        age_days=age_days,
        labels=tuple(labels),
        score=round(score, 2),
        reasons=tuple(reasons),
        covered=activity_id in covered_ids,
        state=_clean_optional(row.get("state")),
        author=_clean_optional(row.get("author")),
        updated_at=_clean_optional(row.get("updated_at")),
        warnings=tuple(item_warnings),
    )


def _score_row(
    row: dict[str, Any],
    *,
    labels: list[str],
    metadata: dict[str, Any],
    age_days: float | None,
    covered: bool,
) -> tuple[float, list[str]]:
    activity_type = _clean(row.get("activity_type"))
    state = _clean(row.get("state")).lower()
    author = _clean(row.get("author")).lower()
    score = float(_TYPE_WEIGHTS.get(activity_type, 8))
    reasons = [f"type:{activity_type}+{_TYPE_WEIGHTS.get(activity_type, 8)}"]

    if activity_type not in _COMMENT_TYPES and state in _CLOSED_STATES:
        score -= 40
        reasons.append(f"state:{state}-40")
    if age_days is not None:
        age_weight = min(age_days, 60.0) * 0.75
        score += age_weight
        reasons.append(f"age:{age_days:.1f}d+{age_weight:.1f}")

    for label in labels:
        weight = _LABEL_HINTS.get(label.lower())
        if weight:
            score += weight
            reasons.append(f"label:{label}+{weight}")

    if author and author not in _BOT_AUTHORS and not author.endswith("[bot]"):
        score += 6
        reasons.append("external_author+6")
    elif author:
        score -= 12
        reasons.append("bot_author-12")

    if _metadata_flag(metadata, "comments_count", "comment_count", "comments"):
        score += 6
        reasons.append("has_comments+6")
    if _metadata_flag(metadata, "parent_number", "parent_issue_number", "parent_pr_number"):
        score += 5
        reasons.append("comment_parent+5")
    if _clean(metadata.get("answer_state")).lower() == "open":
        score += 12
        reasons.append("unanswered_discussion+12")
    if covered:
        score -= 120
        reasons.append("covered_by_generated_content-120")
    else:
        reasons.append("not_covered")
    return score, reasons


def _metadata_flag(metadata: dict[str, Any], *keys: str) -> bool:
    for key in keys:
        value = metadata.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value > 0
        if _clean(value):
            return True
    return False


def _load_activity_rows(
    conn: sqlite3.Connection,
    *,
    activity_types: tuple[str, ...],
    repo: str | None,
    window_start: datetime,
) -> list[dict[str, Any]]:
    placeholders = ", ".join("?" for _ in activity_types)
    where = [
        f"activity_type IN ({placeholders})",
        """
        (
          (
            activity_type IN ('issue_comment', 'review_comment', 'discussion_comment', 'pull_request_review')
            AND datetime(updated_at) >= datetime(?)
          )
          OR (
            activity_type NOT IN ('issue_comment', 'review_comment', 'discussion_comment', 'pull_request_review')
            AND (
              LOWER(COALESCE(state, '')) NOT IN ('closed', 'merged', 'answered', 'completed', 'resolved')
              OR datetime(updated_at) >= datetime(?)
            )
          )
        )
        """,
    ]
    params: list[Any] = [*activity_types, window_start.isoformat(), window_start.isoformat()]
    if repo:
        where.append("repo_name = ?")
        params.append(repo)
    rows = conn.execute(
        f"""SELECT *
            FROM github_activity
            WHERE {' AND '.join(where)}
            ORDER BY datetime(COALESCE(created_at_github, updated_at)) ASC, id ASC""",
        params,
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def _covered_activity_ids(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    warnings: list[str],
) -> set[str]:
    if "generated_content" not in schema or "source_activity_ids" not in schema["generated_content"]:
        return set()
    covered: set[str] = set()
    rows = conn.execute(
        "SELECT id, source_activity_ids FROM generated_content ORDER BY id ASC"
    ).fetchall()
    for row in rows:
        values, row_warnings = _parse_source_activity_ids(
            row["source_activity_ids"],
            int(row["id"]),
        )
        warnings.extend(row_warnings)
        covered.update(str(value) for value in values if value is not None)
    return covered


def _parse_labels(value: Any, activity_id: str) -> tuple[list[str], list[str]]:
    parsed, warnings = _parse_json(value, list, activity_id, "labels", MALFORMED_LABELS, NON_LIST_LABELS)
    labels: list[str] = []
    for item in parsed:
        if isinstance(item, str):
            label = item.strip()
        elif isinstance(item, dict):
            label = _clean(item.get("name"))
        else:
            label = ""
        if label:
            labels.append(label)
    return labels, warnings


def _parse_metadata(value: Any, activity_id: str) -> tuple[dict[str, Any], list[str]]:
    parsed, warnings = _parse_json(
        value,
        dict,
        activity_id,
        "metadata",
        MALFORMED_METADATA,
        NON_OBJECT_METADATA,
    )
    return parsed, warnings


def _parse_source_activity_ids(value: Any, content_id: int) -> tuple[list[Any], list[str]]:
    parsed, warnings = _parse_json(
        value,
        list,
        f"generated_content:{content_id}",
        "source_activity_ids",
        MALFORMED_SOURCE_ACTIVITY_IDS,
        NON_LIST_SOURCE_ACTIVITY_IDS,
    )
    return parsed, warnings


def _parse_json(
    value: Any,
    expected_type: type,
    owner: str,
    field: str,
    malformed_code: str,
    wrong_type_code: str,
) -> tuple[Any, list[str]]:
    if value in (None, ""):
        return expected_type(), []
    if isinstance(value, expected_type):
        return value, []
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return expected_type(), [f"{owner} has {malformed_code} in {field}"]
    if not isinstance(parsed, expected_type):
        return expected_type(), [f"{owner} has {wrong_type_code} in {field}"]
    return parsed, []


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3.Connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    return {
        str(row["name"]): {
            str(column["name"]) for column in conn.execute(f"PRAGMA table_info({row['name']})")
        }
        for row in rows
    }


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "github_activity": {
            "activity_type",
            "author",
            "created_at_github",
            "labels",
            "metadata",
            "number",
            "repo_name",
            "state",
            "title",
            "updated_at",
            "url",
        },
        "generated_content": {"id", "source_activity_ids"},
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns: dict[str, tuple[str, ...]] = {}
    for table, columns in required.items():
        if table not in schema:
            continue
        missing = tuple(sorted(columns - schema[table]))
        if missing and table == "github_activity":
            missing_columns[table] = missing
    return missing_tables, missing_columns


def _empty_totals() -> dict[str, Any]:
    return {
        "candidate_count": 0,
        "covered_count": 0,
        "emitted_count": 0,
        "uncovered_count": 0,
        "warning_count": 0,
    }


def _item_sort_key(item: GitHubActivityResponseBacklogItem) -> tuple[Any, ...]:
    return (item.covered, -item.score, item.repo_name, item.activity_type, item.number)


def _activity_id(repo_name: str, number: str, activity_type: str) -> str:
    return f"{repo_name}#{number}:{activity_type}"


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _age_days(value: datetime | None, now: datetime) -> float | None:
    if value is None:
        return None
    return round(max((now - value).total_seconds(), 0) / 86400, 2)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _clean_optional(value: Any) -> str | None:
    cleaned = _clean(value)
    return cleaned or None


def _format_days(value: float | None) -> str:
    if value is None:
        return "unknown"
    return f"{value:.1f}d"


def _shorten(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    return value[: max(width - 3, 0)] + "..."
