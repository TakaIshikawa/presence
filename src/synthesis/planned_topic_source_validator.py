"""Validate planned topic source_material references against stored artifacts."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_STATUS = "planned"
DEFAULT_CAMPAIGN_STATUSES = ("active", "planned")

ISSUE_EMPTY_SOURCE_MATERIAL = "empty_source_material"
ISSUE_INVALID_JSON = "invalid_json"
ISSUE_UNRESOLVED_REFERENCE = "unresolved_reference"
ISSUE_AMBIGUOUS_PLAIN_TEXT_REFERENCE = "ambiguous_plain_text_reference"

COMMIT_KEYS = {
    "commit",
    "commits",
    "commit_sha",
    "commit_shas",
    "source_commit",
    "source_commits",
}
MESSAGE_KEYS = {
    "claude_message",
    "claude_messages",
    "message",
    "messages",
    "message_uuid",
    "message_uuids",
    "source_message",
    "source_messages",
}
SESSION_KEYS = {"claude_session", "claude_sessions", "session", "sessions", "session_id", "session_ids"}
ACTIVITY_KEYS = {
    "activity",
    "activity_id",
    "activity_ids",
    "github_activity",
    "github_activity_id",
    "github_activity_ids",
    "source_activity_id",
    "source_activity_ids",
}

JSON_LIKE_RE = re.compile(r"^\s*[\[{]")
COMMIT_TEXT_RE = re.compile(
    r"\b(?:commit|commits|sha|shas)\s+([0-9a-f]{7,40})\b",
    re.IGNORECASE,
)
MESSAGE_TEXT_RE = re.compile(
    r"\b(?:message|msg|uuid)\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b",
    re.IGNORECASE,
)
SESSION_TEXT_RE = re.compile(r"\b(?:session|session_id)\s+([A-Za-z0-9_.:/=-]{6,})\b", re.IGNORECASE)
ACTIVITY_TEXT_RE = re.compile(
    r"\b(?:activity|activity_id|github_activity|issue|pr|pull request|discussion|release)\s+#?([0-9]+)\b",
    re.IGNORECASE,
)
LOGICAL_ACTIVITY_RE = re.compile(r"^(.+)#([^#:]+):([A-Za-z_][A-Za-z0-9_-]*)$")
LOOSE_COMMIT_RE = re.compile(r"\b[0-9a-f]{7,40}\b", re.IGNORECASE)


@dataclass(frozen=True)
class PlannedTopicSourceIssue:
    """One validation issue found in a planned topic source_material value."""

    issue_type: str
    source_type: str | None = None
    reference: str | None = None
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlannedTopicSourceItem:
    """Validation result for one planned topic with source issues."""

    planned_topic_id: int
    campaign_id: int | None
    campaign_name: str | None
    campaign_status: str | None
    topic: str
    angle: str | None
    target_date: str | None
    status: str | None
    source_material_preview: str
    issues: tuple[PlannedTopicSourceIssue, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["issues"] = [issue.to_dict() for issue in self.issues]
        return payload


@dataclass(frozen=True)
class PlannedTopicSourceValidatorReport:
    """Aggregate planned topic source validation report."""

    ok: bool
    generated_at: str
    filters: dict[str, Any]
    audited_count: int
    issue_count: int
    by_issue_type: dict[str, int]
    items: tuple[PlannedTopicSourceItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def blocking_issue_count(self) -> int:
        return self.issue_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "planned_topic_source_validator",
            "ok": self.ok,
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "audited_count": self.audited_count,
            "issue_count": self.issue_count,
            "blocking_issue_count": self.blocking_issue_count,
            "by_issue_type": dict(sorted(self.by_issue_type.items())),
            "items": [item.to_dict() for item in self.items],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_planned_topic_source_validator_report(
    db_or_conn: Any,
    *,
    campaign_id: int | None = None,
    status: str = DEFAULT_STATUS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> PlannedTopicSourceValidatorReport:
    """Validate source_material references for active/planned campaign topics."""
    if campaign_id is not None and campaign_id <= 0:
        raise ValueError("campaign_id must be positive")
    if not status:
        raise ValueError("status is required")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")

    conn = _connection(db_or_conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "campaign_id": campaign_id,
        "status": status,
        "limit": limit,
        "campaign_statuses": list(DEFAULT_CAMPAIGN_STATUSES),
    }
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or any(missing_columns.values()):
        return PlannedTopicSourceValidatorReport(
            ok=False,
            generated_at=generated_at.isoformat(),
            filters=filters,
            audited_count=0,
            issue_count=0,
            by_issue_type={},
            items=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_topic_rows(conn, campaign_id=campaign_id, status=status, limit=limit)
    items = tuple(
        item for row in rows if (item := _validate_topic(conn, dict(row))) is not None
    )
    issues = [issue for item in items for issue in item.issues]
    return PlannedTopicSourceValidatorReport(
        ok=not issues,
        generated_at=generated_at.isoformat(),
        filters=filters,
        audited_count=len(rows),
        issue_count=len(issues),
        by_issue_type=dict(Counter(issue.issue_type for issue in issues)),
        items=items,
    )


def format_planned_topic_source_validator_json(report: PlannedTopicSourceValidatorReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_planned_topic_source_validator_text(report: PlannedTopicSourceValidatorReport) -> str:
    """Render a compact human-readable source validation report."""
    filters = report.filters
    lines = [
        "Planned Topic Source Validator",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"campaign_id={filters.get('campaign_id') or '*'} "
            f"status={filters.get('status')} "
            f"limit={filters.get('limit') if filters.get('limit') is not None else '*'} "
            f"campaign_statuses={','.join(filters.get('campaign_statuses') or [])}"
        ),
        f"Audited: {report.audited_count}",
        (
            "Issues: "
            f"total={report.issue_count} "
            + " ".join(
                f"{issue_type}={count}"
                for issue_type, count in sorted(report.by_issue_type.items())
            )
        ).rstrip(),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
            if columns
        )
        if missing:
            lines.append("Missing columns: " + missing)
    if not report.items:
        lines.extend(["", "No planned topic source issues found."])
        return "\n".join(lines)

    lines.append("")
    lines.append("Topics:")
    for item in report.items:
        campaign = item.campaign_name or (
            str(item.campaign_id) if item.campaign_id is not None else "-"
        )
        lines.append(
            f"- topic_id={item.planned_topic_id} campaign={campaign} "
            f"status={item.status or '-'} topic={_shorten(item.topic, 56)}"
        )
        for issue in item.issues:
            ref = f" reference={issue.reference}" if issue.reference else ""
            source = f" source_type={issue.source_type}" if issue.source_type else ""
            message = f" message={issue.message}" if issue.message else ""
            lines.append(f"  issue={issue.issue_type}{source}{ref}{message}")
    return "\n".join(lines)


def _validate_topic(conn: sqlite3.Connection, row: dict[str, Any]) -> PlannedTopicSourceItem | None:
    source_material = row.get("source_material")
    issues: list[PlannedTopicSourceIssue] = []
    if source_material is None or not str(source_material).strip():
        issues.append(
            PlannedTopicSourceIssue(
                issue_type=ISSUE_EMPTY_SOURCE_MATERIAL,
                message="source_material is empty",
            )
        )
    else:
        extracted, parse_issue = _extract_references(str(source_material))
        if parse_issue is not None:
            issues.append(parse_issue)
        for source_type, refs in extracted.items():
            for ref in refs:
                issues.extend(_reference_issues(conn, source_type, ref))

    if not issues:
        return None
    return PlannedTopicSourceItem(
        planned_topic_id=int(row["planned_topic_id"]),
        campaign_id=_optional_int(row.get("campaign_id")),
        campaign_name=_none_if_blank(row.get("campaign_name")),
        campaign_status=_none_if_blank(row.get("campaign_status")),
        topic=str(row.get("topic") or ""),
        angle=_none_if_blank(row.get("angle")),
        target_date=_none_if_blank(row.get("target_date")),
        status=_none_if_blank(row.get("status")),
        source_material_preview=_shorten(source_material, 160),
        issues=tuple(issues),
    )


def _extract_references(
    value: str,
) -> tuple[dict[str, tuple[str, ...]], PlannedTopicSourceIssue | None]:
    text = value.strip()
    refs: dict[str, list[str]] = {"commit": [], "message": [], "session": [], "activity": []}
    if JSON_LIKE_RE.match(text):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            return _freeze_refs(refs), PlannedTopicSourceIssue(
                issue_type=ISSUE_INVALID_JSON,
                message=f"invalid JSON at character {exc.pos}",
            )
        _extract_json_refs(parsed, refs)
        return _freeze_refs(refs), None

    ambiguous = _extract_plain_text_refs(text, refs)
    issue = None
    if ambiguous:
        issue = PlannedTopicSourceIssue(
            issue_type=ISSUE_AMBIGUOUS_PLAIN_TEXT_REFERENCE,
            reference=", ".join(ambiguous),
            message="plain-text reference needs an explicit source type",
        )
    return _freeze_refs(refs), issue


def _extract_json_refs(value: Any, refs: dict[str, list[str]], key: str | None = None) -> None:
    normalized_key = _normalize_key(key)
    if isinstance(value, dict):
        if _is_activity_object(value):
            refs["activity"].append(str(value["id"]).strip())
        for child_key, child in value.items():
            _extract_json_refs(child, refs, str(child_key))
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _extract_json_refs(item, refs, key)
        return
    if value in (None, "") or normalized_key is None:
        return

    target = _source_type_for_key(normalized_key)
    if target is not None:
        refs[target].append(str(value).strip())


def _extract_plain_text_refs(text: str, refs: dict[str, list[str]]) -> list[str]:
    matched_spans: list[tuple[int, int]] = []
    for source_type, pattern in (
        ("commit", COMMIT_TEXT_RE),
        ("message", MESSAGE_TEXT_RE),
        ("session", SESSION_TEXT_RE),
        ("activity", ACTIVITY_TEXT_RE),
    ):
        for match in pattern.finditer(text):
            refs[source_type].append(match.group(1).strip())
            matched_spans.append(match.span(1))

    ambiguous: list[str] = []
    for match in LOOSE_COMMIT_RE.finditer(text):
        if any(start <= match.start() and match.end() <= end for start, end in matched_spans):
            continue
        ambiguous.append(match.group(0))
    return list(dict.fromkeys(ambiguous))


def _reference_issues(
    conn: sqlite3.Connection,
    source_type: str,
    reference: str,
) -> tuple[PlannedTopicSourceIssue, ...]:
    ref = str(reference).strip()
    if not ref:
        return ()
    if source_type == "commit":
        rows = conn.execute(
            "SELECT commit_sha FROM github_commits WHERE commit_sha LIKE ?",
            (f"{ref}%",),
        ).fetchall()
        if len(rows) > 1:
            return (
                PlannedTopicSourceIssue(
                    issue_type=ISSUE_AMBIGUOUS_PLAIN_TEXT_REFERENCE,
                    source_type=source_type,
                    reference=ref,
                    message="commit prefix matches multiple commits",
                ),
            )
        return _unresolved_if(not rows, source_type, ref)
    if source_type == "message":
        row = conn.execute(
            "SELECT 1 FROM claude_messages WHERE message_uuid = ?",
            (ref,),
        ).fetchone()
        return _unresolved_if(row is None, source_type, ref)
    if source_type == "session":
        row = conn.execute(
            "SELECT 1 FROM claude_messages WHERE session_id = ?",
            (ref,),
        ).fetchone()
        return _unresolved_if(row is None, source_type, ref)
    if source_type == "activity":
        row = _activity_row(conn, ref)
        return _unresolved_if(row is None, source_type, ref)
    return ()


def _unresolved_if(
    unresolved: bool,
    source_type: str,
    reference: str,
) -> tuple[PlannedTopicSourceIssue, ...]:
    if not unresolved:
        return ()
    return (
        PlannedTopicSourceIssue(
            issue_type=ISSUE_UNRESOLVED_REFERENCE,
            source_type=source_type,
            reference=reference,
            message="reference was not found",
        ),
    )


def _load_topic_rows(
    conn: sqlite3.Connection,
    *,
    campaign_id: int | None,
    status: str,
    limit: int | None,
) -> list[sqlite3.Row]:
    where = ["cc.status IN (?, ?)"]
    params: list[Any] = list(DEFAULT_CAMPAIGN_STATUSES)
    if status != "all":
        where.append("pt.status = ?")
        params.append(status)
    if campaign_id is not None:
        where.append("pt.campaign_id = ?")
        params.append(campaign_id)
    sql = f"""SELECT pt.id AS planned_topic_id,
                     pt.campaign_id,
                     cc.name AS campaign_name,
                     cc.status AS campaign_status,
                     pt.topic,
                     pt.angle,
                     pt.target_date,
                     pt.status,
                     pt.source_material
              FROM planned_topics pt
              JOIN content_campaigns cc ON cc.id = pt.campaign_id
              WHERE {' AND '.join(where)}
              ORDER BY date(pt.target_date) ASC NULLS LAST, pt.created_at ASC, pt.id ASC"""
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, tuple(params)).fetchall()


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "claude_messages": {"session_id", "message_uuid"},
        "content_campaigns": {"id", "name", "status"},
        "github_activity": {"id", "repo_name", "number", "activity_type"},
        "github_commits": {"commit_sha"},
        "planned_topics": {"id", "campaign_id", "topic", "source_material", "status"},
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3.Connection or expose .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    tables = {row["name"] if isinstance(row, sqlite3.Row) else row[0] for row in rows}
    return {table: _table_columns(conn, table) for table in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        row["name"] if isinstance(row, sqlite3.Row) else row[1]
        for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
    }


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _freeze_refs(refs: dict[str, list[str]]) -> dict[str, tuple[str, ...]]:
    return {
        key: tuple(dict.fromkeys(str(ref).strip() for ref in values if str(ref).strip()))
        for key, values in refs.items()
    }


def _source_type_for_key(key: str) -> str | None:
    if key in COMMIT_KEYS:
        return "commit"
    if key in MESSAGE_KEYS:
        return "message"
    if key in SESSION_KEYS:
        return "session"
    if key in ACTIVITY_KEYS:
        return "activity"
    return None


def _activity_row(conn: sqlite3.Connection, reference: str) -> sqlite3.Row | None:
    row = conn.execute("SELECT 1 FROM github_activity WHERE CAST(id AS TEXT) = ?", (reference,)).fetchone()
    if row is not None:
        return row
    match = LOGICAL_ACTIVITY_RE.match(reference)
    if match:
        repo, number, activity_type = match.groups()
        return conn.execute(
            """SELECT 1 FROM github_activity
               WHERE repo_name = ? AND number = ? AND activity_type = ?""",
            (repo, number, activity_type),
        ).fetchone()
    return None


def _normalize_key(key: str | None) -> str | None:
    if key is None:
        return None
    return str(key).strip().lower().replace("-", "_")


def _is_activity_object(value: dict[Any, Any]) -> bool:
    return (
        "id" in value
        and any(_normalize_key(key) in {"activity_type", "repo_name", "github_activity"} for key in value)
    )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _none_if_blank(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _shorten(value: Any, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."
