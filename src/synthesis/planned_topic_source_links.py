"""Audit planned topics for missing generated-content source links."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS_AHEAD = 30
SOURCE_COLUMNS = ("source_commits", "source_messages", "source_activity_ids")
_SLUG_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class PlannedTopicSourceLinkFinding:
    """One planned topic that needs source-link attention."""

    planned_topic_id: int
    campaign_id: int | None
    campaign_name: str | None
    topic: str
    angle: str | None
    target_date: str | None
    status: str | None
    content_id: int | None
    reason: str
    content_type: str | None = None
    generated_at: str | None = None
    source_commits: tuple[str, ...] = ()
    source_messages: tuple[str, ...] = ()
    source_activity_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source_activity_ids"] = list(self.source_activity_ids)
        payload["source_commits"] = list(self.source_commits)
        payload["source_messages"] = list(self.source_messages)
        return payload


@dataclass(frozen=True)
class PlannedTopicSourceLinkReport:
    """Planned-topic source-link audit report."""

    generated_at: str
    filters: dict[str, Any]
    unsourced_generated: tuple[PlannedTopicSourceLinkFinding, ...]
    overdue_ungenerated: tuple[PlannedTopicSourceLinkFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def has_issues(self) -> bool:
        return bool(self.unsourced_generated or self.overdue_ungenerated)

    def to_dict(self) -> dict[str, Any]:
        findings = {
            "overdue_ungenerated": [item.to_dict() for item in self.overdue_ungenerated],
            "unsourced_generated": [item.to_dict() for item in self.unsourced_generated],
        }
        return {
            "artifact_type": "planned_topic_source_links",
            "filters": dict(self.filters),
            "findings": findings,
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": {
                "overdue_ungenerated": len(self.overdue_ungenerated),
                "unsourced_generated": len(self.unsourced_generated),
                "issue_count": len(self.overdue_ungenerated) + len(self.unsourced_generated),
            },
        }


def build_planned_topic_source_link_report(
    db_or_conn: Any,
    *,
    campaign: str | None = None,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    include_future: bool = False,
    now: datetime | None = None,
) -> PlannedTopicSourceLinkReport:
    """Find planned topics whose generated content is unsourced or overdue missing."""
    if days_ahead < 0:
        raise ValueError("days_ahead must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    today = generated_at.date()
    window_end = today + timedelta(days=days_ahead)
    filters: dict[str, Any] = {
        "campaign": campaign,
        "days_ahead": days_ahead,
        "include_future": include_future,
        "window_end": None if include_future else window_end.isoformat(),
    }

    missing_tables, missing_columns = _schema_gaps(schema)
    campaign_id: int | None = None
    if campaign and "content_campaigns" in schema and not missing_columns.get("content_campaigns"):
        campaign_id = _resolve_campaign(conn, schema, campaign)
        filters["campaign_id"] = campaign_id
    elif campaign:
        filters["campaign_id"] = None

    rows = (
        _load_rows(
            conn,
            schema,
            campaign_id=campaign_id,
            today=today,
            window_end=window_end,
            include_future=include_future,
        )
        if not missing_tables and not any(missing_columns.values())
        else []
    )
    unsourced: list[PlannedTopicSourceLinkFinding] = []
    overdue: list[PlannedTopicSourceLinkFinding] = []
    for row in rows:
        finding = _finding(row, today=today)
        if finding is None:
            continue
        if finding.reason == "unsourced_generated_content":
            unsourced.append(finding)
        else:
            overdue.append(finding)

    return PlannedTopicSourceLinkReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        unsourced_generated=tuple(unsourced),
        overdue_ungenerated=tuple(overdue),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_planned_topic_source_link_json(report: PlannedTopicSourceLinkReport) -> str:
    """Serialize the audit report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_planned_topic_source_link_text(report: PlannedTopicSourceLinkReport) -> str:
    """Render the audit report for terminal review."""
    filters = report.filters
    totals = report.to_dict()["totals"]
    lines = [
        "Planned Topic Source Link Audit",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"campaign={filters.get('campaign') or '*'} "
            f"days_ahead={filters['days_ahead']} "
            f"include_future={filters['include_future']}"
        ),
        (
            "Totals: "
            f"unsourced_generated={totals['unsourced_generated']} "
            f"overdue_ungenerated={totals['overdue_ungenerated']} "
            f"issue_count={totals['issue_count']}"
        ),
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

    if not report.has_issues:
        lines.extend(["", "No planned topic source link issues found."])
        return "\n".join(lines)

    _append_findings(lines, "Unsourced generated content", report.unsourced_generated)
    _append_findings(lines, "Overdue ungenerated topics", report.overdue_ungenerated)
    return "\n".join(lines)


def _append_findings(
    lines: list[str],
    title: str,
    findings: tuple[PlannedTopicSourceLinkFinding, ...],
) -> None:
    if not findings:
        return
    lines.extend(["", f"{title}:"])
    for finding in findings:
        campaign = finding.campaign_name or (
            str(finding.campaign_id) if finding.campaign_id is not None else "-"
        )
        lines.append(
            f"- topic_id={finding.planned_topic_id} campaign={campaign} "
            f"target={finding.target_date or '-'} content={finding.content_id or '-'} "
            f"reason={finding.reason} topic={_shorten(finding.topic, 56)}"
        )


def _finding(
    row: dict[str, Any],
    *,
    today: date,
) -> PlannedTopicSourceLinkFinding | None:
    content_id = row.get("content_id")
    if content_id is not None:
        source_commits = _json_list(row.get("source_commits"))
        source_messages = _json_list(row.get("source_messages"))
        source_activity_ids = _json_list(row.get("source_activity_ids"))
        if source_commits or source_messages or source_activity_ids:
            return None
        return _base_finding(
            row,
            reason="unsourced_generated_content",
            source_commits=source_commits,
            source_messages=source_messages,
            source_activity_ids=source_activity_ids,
        )

    target = _parse_date(row.get("target_date"))
    if target is not None and target < today:
        return _base_finding(row, reason="overdue_ungenerated_topic")
    return None


def _base_finding(
    row: dict[str, Any],
    *,
    reason: str,
    source_commits: tuple[str, ...] = (),
    source_messages: tuple[str, ...] = (),
    source_activity_ids: tuple[str, ...] = (),
) -> PlannedTopicSourceLinkFinding:
    return PlannedTopicSourceLinkFinding(
        planned_topic_id=int(row["planned_topic_id"]),
        campaign_id=_optional_int(row.get("campaign_id")),
        campaign_name=_none_if_blank(row.get("campaign_name")),
        topic=str(row.get("topic") or ""),
        angle=_none_if_blank(row.get("angle")),
        target_date=_none_if_blank(row.get("target_date")),
        status=_none_if_blank(row.get("status")),
        content_id=_optional_int(row.get("content_id")),
        content_type=_none_if_blank(row.get("content_type")),
        generated_at=_none_if_blank(row.get("generated_at")),
        reason=reason,
        source_commits=source_commits,
        source_messages=source_messages,
        source_activity_ids=source_activity_ids,
    )


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    campaign_id: int | None,
    today: date,
    window_end: date,
    include_future: bool,
) -> list[dict[str, Any]]:
    pt = schema["planned_topics"]
    gc = schema["generated_content"]
    cc = schema["content_campaigns"]
    selected = [
        _column_expr(pt, "id", "pt", "planned_topic_id"),
        _column_expr(pt, "campaign_id", "pt"),
        _column_expr(cc, "name", "cc", "campaign_name"),
        _column_expr(pt, "topic", "pt"),
        _column_expr(pt, "angle", "pt"),
        _column_expr(pt, "target_date", "pt"),
        _column_expr(pt, "status", "pt"),
        _column_expr(pt, "content_id", "pt"),
        _column_expr(gc, "content_type", "gc"),
        _column_expr(gc, "created_at", "gc", "generated_at"),
        _column_expr(gc, "source_commits", "gc"),
        _column_expr(gc, "source_messages", "gc"),
        _column_expr(gc, "source_activity_ids", "gc"),
    ]
    where = [
        """(
             pt.content_id IS NOT NULL
             OR (
                pt.content_id IS NULL
                AND pt.target_date IS NOT NULL
                AND date(pt.target_date) < ?
             )
           )"""
    ]
    params: list[Any] = [today.isoformat()]
    if not include_future:
        where.append("(pt.target_date IS NULL OR date(pt.target_date) <= ?)")
        params.append(window_end.isoformat())
    if campaign_id is not None:
        where.append("pt.campaign_id = ?")
        params.append(campaign_id)

    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM planned_topics pt
            LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
            LEFT JOIN generated_content gc ON gc.id = pt.content_id
            WHERE {' AND '.join(where)}
            ORDER BY
                CASE WHEN pt.content_id IS NOT NULL THEN 0 ELSE 1 END,
                date(pt.target_date) ASC NULLS LAST,
                pt.id ASC""",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _resolve_campaign(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign: str,
) -> int:
    text = campaign.strip()
    if not text:
        raise ValueError("campaign must not be blank")
    if text.isdigit():
        row = conn.execute("SELECT id FROM content_campaigns WHERE id = ?", (int(text),)).fetchone()
        if row:
            return int(row["id"])

    columns = schema["content_campaigns"]
    predicates = ["name = ?"]
    params: list[Any] = [text]
    if "slug" in columns:
        predicates.append("slug = ?")
        params.append(text)
    rows = conn.execute(
        f"""SELECT id, name
            FROM content_campaigns
            WHERE {' OR '.join(predicates)}
            ORDER BY created_at ASC, id ASC""",
        tuple(params),
    ).fetchall()
    if not rows:
        all_rows = conn.execute(
            "SELECT id, name FROM content_campaigns ORDER BY created_at ASC, id ASC"
        ).fetchall()
        rows = [row for row in all_rows if _slug(row["name"]) == _slug(text)]
    if not rows:
        raise ValueError(f"Campaign {campaign!r} not found")
    if len(rows) > 1:
        ids = ", ".join(str(row["id"]) for row in rows)
        raise ValueError(f"Campaign {campaign!r} matched multiple campaigns: {ids}")
    return int(rows[0]["id"])


def _json_list(value: Any) -> tuple[str, ...]:
    if isinstance(value, (list, tuple, set)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    if value in (None, ""):
        return ()
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return ()
    if not isinstance(parsed, list):
        return ()
    return tuple(str(item).strip() for item in parsed if str(item).strip())


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "content_campaigns": {"id", "name"},
        "generated_content": {"id", *SOURCE_COLUMNS},
        "planned_topics": {"id", "campaign_id", "topic", "target_date", "content_id"},
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


def _column_expr(columns: set[str], column: str, alias: str, output: str | None = None) -> str:
    name = output or column
    if column in columns:
        return f"{alias}.{_quote_identifier(column)} AS {_quote_identifier(name)}"
    return f"NULL AS {_quote_identifier(name)}"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


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


def _slug(value: Any) -> str:
    return _SLUG_RE.sub("-", str(value or "").strip().lower()).strip("-")


def _shorten(value: Any, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."
