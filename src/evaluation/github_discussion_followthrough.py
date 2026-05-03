"""Report GitHub discussions that have not yet produced generated content."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import io
import json
import sqlite3
from typing import Any


DEFAULT_DAYS_STALE = 14
STATUSES = ("covered", "stale_uncovered", "fresh_uncovered", "invalid_linkage")


@dataclass(frozen=True)
class GitHubDiscussionFollowthroughItem:
    status: str
    id: int | None
    activity_id: str | None
    title: str | None
    repo_name: str | None
    number: str | None
    url: str | None
    updated_at: str | None
    labels: tuple[str, ...]
    age_days: int | None
    linked_content_ids: tuple[int, ...]
    content_id: int | None = None
    error: str | None = None
    raw_source_activity_ids: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["labels"] = list(self.labels)
        payload["linked_content_ids"] = list(self.linked_content_ids)
        return payload


@dataclass(frozen=True)
class GitHubDiscussionFollowthroughReport:
    generated_at: str
    filters: dict[str, Any]
    items: tuple[GitHubDiscussionFollowthroughItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        counts = {status: 0 for status in STATUSES}
        for item in self.items:
            counts[item.status] = counts.get(item.status, 0) + 1
        return {
            "artifact_type": "github_discussion_followthrough",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "summary": {
                **counts,
                "total": len(self.items),
            },
            "items": [item.to_dict() for item in self.items],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_github_discussion_followthrough_report(
    db_or_conn: Any,
    *,
    days_stale: int = DEFAULT_DAYS_STALE,
    repo: str | None = None,
    label: str | None = None,
    now: datetime | None = None,
) -> GitHubDiscussionFollowthroughReport:
    """Classify GitHub discussion activity by generated-content follow-through."""
    if days_stale <= 0:
        raise ValueError("days_stale must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "days_stale": days_stale,
        "repo": repo,
        "label": label,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _has_missing_required_columns(missing_columns):
        return GitHubDiscussionFollowthroughReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            items=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    linked_content_ids, invalid_linkages = _load_linkages(conn)
    discussion_rows = _load_discussions(conn, repo=repo)
    items: list[GitHubDiscussionFollowthroughItem] = []
    normalized_label = _normalize_label(label)
    for row in discussion_rows:
        labels = tuple(_parse_labels(row.get("labels")))
        if normalized_label and normalized_label not in {_normalize_label(value) for value in labels}:
            continue
        activity_id = _activity_id(row)
        keys = {activity_id, str(row["id"])}
        content_ids = tuple(
            sorted(
                {
                    content_id
                    for key in keys
                    for content_id in linked_content_ids.get(key, set())
                }
            )
        )
        age_days = _age_days(row.get("updated_at"), generated_at)
        if content_ids:
            status = "covered"
        elif age_days is not None and age_days >= days_stale:
            status = "stale_uncovered"
        else:
            status = "fresh_uncovered"
        items.append(
            GitHubDiscussionFollowthroughItem(
                status=status,
                id=int(row["id"]),
                activity_id=activity_id,
                title=row.get("title"),
                repo_name=row.get("repo_name"),
                number=str(row.get("number")) if row.get("number") is not None else None,
                url=row.get("url"),
                updated_at=row.get("updated_at"),
                labels=labels,
                age_days=age_days,
                linked_content_ids=content_ids,
            )
        )

    items.extend(invalid_linkages)
    items.sort(key=_item_sort_key)
    return GitHubDiscussionFollowthroughReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        items=tuple(items),
        missing_columns=missing_columns,
    )


def format_github_discussion_followthrough_json(
    report: GitHubDiscussionFollowthroughReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_github_discussion_followthrough_csv(
    report: GitHubDiscussionFollowthroughReport,
) -> str:
    """Serialize the report as CSV with one row per classified item."""
    output = io.StringIO()
    fieldnames = [
        "status",
        "id",
        "activity_id",
        "title",
        "repo_name",
        "number",
        "url",
        "updated_at",
        "labels",
        "age_days",
        "linked_content_ids",
        "content_id",
        "error",
        "raw_source_activity_ids",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for item in report.items:
        row = item.to_dict()
        row["labels"] = json.dumps(row["labels"], sort_keys=True)
        row["linked_content_ids"] = json.dumps(row["linked_content_ids"])
        writer.writerow(row)
    return output.getvalue().rstrip("\r\n")


def _load_linkages(
    conn: sqlite3.Connection,
) -> tuple[dict[str, set[int]], list[GitHubDiscussionFollowthroughItem]]:
    linked_content_ids: dict[str, set[int]] = {}
    invalid_linkages: list[GitHubDiscussionFollowthroughItem] = []
    rows = conn.execute(
        """SELECT id, source_activity_ids
           FROM generated_content
           WHERE source_activity_ids IS NOT NULL
             AND source_activity_ids != ''
           ORDER BY id ASC"""
    ).fetchall()
    for row in rows:
        content_id = int(row["id"])
        raw_value = row["source_activity_ids"]
        try:
            refs = json.loads(raw_value)
        except (TypeError, json.JSONDecodeError) as exc:
            invalid_linkages.append(
                _invalid_linkage_item(content_id, raw_value, f"invalid_json: {exc.msg}")
            )
            continue
        if not isinstance(refs, list):
            invalid_linkages.append(
                _invalid_linkage_item(
                    content_id,
                    raw_value,
                    f"non_list_json: {type(refs).__name__}",
                )
            )
            continue
        for ref in refs:
            if ref is None:
                continue
            linked_content_ids.setdefault(str(ref), set()).add(content_id)
    return linked_content_ids, invalid_linkages


def _invalid_linkage_item(
    content_id: int,
    raw_value: str | None,
    error: str,
) -> GitHubDiscussionFollowthroughItem:
    return GitHubDiscussionFollowthroughItem(
        status="invalid_linkage",
        id=None,
        activity_id=None,
        title=None,
        repo_name=None,
        number=None,
        url=None,
        updated_at=None,
        labels=(),
        age_days=None,
        linked_content_ids=(content_id,),
        content_id=content_id,
        error=error,
        raw_source_activity_ids=raw_value,
    )


def _load_discussions(conn: sqlite3.Connection, *, repo: str | None) -> list[dict[str, Any]]:
    where = ["activity_type = 'discussion'"]
    params: list[Any] = []
    if repo:
        where.append("repo_name = ?")
        params.append(repo)
    rows = conn.execute(
        f"""SELECT id, repo_name, number, title, url, updated_at, labels
            FROM github_activity
            WHERE {' AND '.join(where)}
            ORDER BY updated_at DESC, id DESC""",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _parse_labels(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        parsed = value
    else:
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return []
    if not isinstance(parsed, list):
        return []
    return sorted(str(item) for item in parsed if item is not None and str(item))


def _activity_id(row: dict[str, Any]) -> str:
    return f"{row.get('repo_name')}#{row.get('number')}:discussion"


def _age_days(updated_at: Any, now: datetime) -> int | None:
    parsed = _parse_timestamp(updated_at)
    if parsed is None:
        return None
    delta = now - parsed
    return max(int(delta.total_seconds() // 86400), 0)


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _item_sort_key(item: GitHubDiscussionFollowthroughItem) -> tuple[int, str, int]:
    status_order = {status: index for index, status in enumerate(STATUSES)}
    return (
        status_order.get(item.status, len(status_order)),
        item.updated_at or "",
        item.id or item.content_id or 0,
    )


def _normalize_label(value: str | None) -> str:
    return (value or "").strip().lower()


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[table] = {
            info["name"] if isinstance(info, sqlite3.Row) else info[1]
            for info in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "github_activity": {
            "id",
            "repo_name",
            "activity_type",
            "number",
            "title",
            "url",
            "updated_at",
            "labels",
        },
        "generated_content": {"id", "source_activity_ids"},
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _has_missing_required_columns(missing_columns: dict[str, tuple[str, ...]]) -> bool:
    return any(missing_columns.values())
