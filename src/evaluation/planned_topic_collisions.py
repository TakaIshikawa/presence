"""Report planned topics competing for publishing slots or campaign fit."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
INACTIVE_CAMPAIGN_STATUSES = {"completed", "paused"}


@dataclass(frozen=True)
class PlannedTopicCollisionFinding:
    """One planned-topic collision or campaign-window issue."""

    finding_type: str
    planned_topic_ids: tuple[int, ...]
    campaign_ids: tuple[int, ...]
    campaign_id: int | None
    campaign_status: str | None
    topic: str | None
    target_date: str | None
    content_id: int | None
    campaign_start_date: str | None
    campaign_end_date: str | None
    reason: str
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["campaign_ids"] = list(self.campaign_ids)
        payload["planned_topic_ids"] = list(self.planned_topic_ids)
        return payload


@dataclass(frozen=True)
class PlannedTopicCollisionReport:
    """Planned-topic collision report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    findings: tuple[PlannedTopicCollisionFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def has_issues(self) -> bool:
        return bool(self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "planned_topic_collisions",
            "findings": [finding.to_dict() for finding in self.findings],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_planned_topic_collision_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    campaign_id: int | None = None,
    now: datetime | None = None,
) -> PlannedTopicCollisionReport:
    """Build a read-only report of planned topic collisions and campaign mismatches."""
    if days <= 0:
        raise ValueError("days must be positive")
    if campaign_id is not None and campaign_id <= 0:
        raise ValueError("campaign_id must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff_dt = generated_at - timedelta(days=days)
    cutoff_date = cutoff_dt.date()
    filters = {
        "campaign_id": campaign_id,
        "days": days,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff_dt.isoformat(),
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(
        table for table in ("planned_topics", "content_campaigns") if table not in schema
    )
    missing_columns = _missing_columns(schema)
    if "planned_topics" not in schema:
        return PlannedTopicCollisionReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals={
                "content_id_collisions": 0,
                "duplicate_topic_date_collisions": 0,
                "findings": 0,
                "inactive_campaign_topics": 0,
                "orphaned_campaign_topics": 0,
                "out_of_window_topics": 0,
                "topics_scanned": 0,
            },
            findings=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_planned_topic_rows(
        conn,
        schema=schema,
        cutoff_date=cutoff_date,
        cutoff_dt=cutoff_dt,
        campaign_id=campaign_id,
    )
    active_rows = [row for row in rows if _status(row) != "skipped"]
    findings = _findings(active_rows)
    findings.sort(key=_finding_sort_key)

    return PlannedTopicCollisionReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "content_id_collisions": sum(
                1 for finding in findings if finding.finding_type == "shared_content_id"
            ),
            "duplicate_topic_date_collisions": sum(
                1 for finding in findings if finding.finding_type == "duplicate_topic_date"
            ),
            "findings": len(findings),
            "inactive_campaign_topics": sum(
                1 for finding in findings if finding.finding_type == "inactive_campaign"
            ),
            "orphaned_campaign_topics": sum(
                1
                for row in active_rows
                if row.get("campaign_id") is not None and row.get("campaign_row_missing")
            ),
            "out_of_window_topics": sum(
                1 for finding in findings if finding.finding_type == "target_date_outside_campaign_window"
            ),
            "topics_scanned": len(active_rows),
        },
        findings=tuple(findings),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_planned_topic_collisions_json(report: PlannedTopicCollisionReport) -> str:
    """Serialize a planned-topic collision report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_planned_topic_collisions_text(report: PlannedTopicCollisionReport) -> str:
    """Render a concise human-readable planned-topic collision report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Planned Topic Collisions",
        f"Generated: {report.generated_at}",
        f"Filters: days={filters['days']} campaign_id={filters['campaign_id'] or '-'}",
        (
            "Totals: "
            f"topics={totals['topics_scanned']} findings={totals['findings']} "
            f"duplicate_topic_date={totals['duplicate_topic_date_collisions']} "
            f"shared_content_id={totals['content_id_collisions']} "
            f"inactive_campaign={totals['inactive_campaign_topics']} "
            f"out_of_window={totals['out_of_window_topics']} "
            f"orphaned_campaign_topics={totals['orphaned_campaign_topics']}"
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

    if not report.findings:
        lines.extend(["", "No planned topic collisions found."])
        return "\n".join(lines)

    lines.extend(["", "Findings:"])
    for finding in report.findings:
        lines.append(
            f"- type={finding.finding_type} planned_topic_ids="
            f"{','.join(str(item) for item in finding.planned_topic_ids)} "
            f"campaign_id={finding.campaign_id or '-'} "
            f"campaign_status={finding.campaign_status or '-'} "
            f"target_date={finding.target_date or '-'} "
            f"content_id={finding.content_id or '-'}"
        )
        lines.append(f"  reason={finding.reason}")
        lines.append(f"  recommended_action={finding.recommended_action}")
    return "\n".join(lines)


def _findings(rows: list[dict[str, Any]]) -> list[PlannedTopicCollisionFinding]:
    findings: list[PlannedTopicCollisionFinding] = []
    by_topic_date: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    by_content_id: dict[int, list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        topic_key = _normalized_topic(row.get("topic"))
        date_key = _date_key(row.get("target_date"))
        if topic_key and date_key:
            by_topic_date[(topic_key, date_key)].append(row)
        content_id = _optional_int(row.get("content_id"))
        if content_id is not None:
            by_content_id[content_id].append(row)

        campaign_status = _status_value(row.get("campaign_status"))
        if campaign_status in INACTIVE_CAMPAIGN_STATUSES:
            findings.append(
                _single_row_finding(
                    row,
                    finding_type="inactive_campaign",
                    reason=(
                        f"planned topic belongs to a {campaign_status} campaign"
                    ),
                    recommended_action="move_topic_to_active_campaign_or_mark_skipped",
                )
            )

        window_reason = _out_of_window_reason(row)
        if window_reason:
            findings.append(
                _single_row_finding(
                    row,
                    finding_type="target_date_outside_campaign_window",
                    reason=window_reason,
                    recommended_action="reschedule_topic_within_campaign_window_or_move_campaign",
                )
            )

    for (_topic_key, target_date), duplicates in by_topic_date.items():
        if len(duplicates) < 2:
            continue
        ordered = sorted(duplicates, key=lambda row: _optional_int(row.get("id")) or 0)
        findings.append(
            _group_finding(
                ordered,
                finding_type="duplicate_topic_date",
                target_date=target_date,
                content_id=None,
                reason="multiple planned topics use the same normalized topic and target_date",
                recommended_action="merge_or_reschedule_duplicate_topics",
            )
        )

    for content_id, linked_rows in by_content_id.items():
        if len(linked_rows) < 2:
            continue
        ordered = sorted(linked_rows, key=lambda row: _optional_int(row.get("id")) or 0)
        findings.append(
            _group_finding(
                ordered,
                finding_type="shared_content_id",
                target_date=None,
                content_id=content_id,
                reason="multiple planned topics are linked to the same generated content",
                recommended_action="split_planned_topics_or_keep_single_owner",
            )
        )

    return findings


def _single_row_finding(
    row: dict[str, Any],
    *,
    finding_type: str,
    reason: str,
    recommended_action: str,
) -> PlannedTopicCollisionFinding:
    campaign_id = _optional_int(row.get("campaign_id"))
    return PlannedTopicCollisionFinding(
        finding_type=finding_type,
        planned_topic_ids=(_optional_int(row.get("id")) or 0,),
        campaign_ids=(campaign_id,) if campaign_id is not None else (),
        campaign_id=campaign_id,
        campaign_status=_optional_text(row.get("campaign_status")),
        topic=_optional_text(row.get("topic")),
        target_date=_date_key(row.get("target_date")),
        content_id=_optional_int(row.get("content_id")),
        campaign_start_date=_date_key(row.get("campaign_start_date")),
        campaign_end_date=_date_key(row.get("campaign_end_date")),
        reason=reason,
        recommended_action=recommended_action,
    )


def _group_finding(
    rows: list[dict[str, Any]],
    *,
    finding_type: str,
    target_date: str | None,
    content_id: int | None,
    reason: str,
    recommended_action: str,
) -> PlannedTopicCollisionFinding:
    campaign_ids = tuple(
        sorted(
            {
                campaign_id
                for campaign_id in (_optional_int(row.get("campaign_id")) for row in rows)
                if campaign_id is not None
            }
        )
    )
    statuses = tuple(
        sorted(
            {
                status
                for status in (_optional_text(row.get("campaign_status")) for row in rows)
                if status
            }
        )
    )
    return PlannedTopicCollisionFinding(
        finding_type=finding_type,
        planned_topic_ids=tuple(_optional_int(row.get("id")) or 0 for row in rows),
        campaign_ids=campaign_ids,
        campaign_id=campaign_ids[0] if len(campaign_ids) == 1 else None,
        campaign_status=statuses[0] if len(statuses) == 1 else None,
        topic=_optional_text(rows[0].get("topic")),
        target_date=target_date,
        content_id=content_id,
        campaign_start_date=None,
        campaign_end_date=None,
        reason=reason,
        recommended_action=recommended_action,
    )


def _load_planned_topic_rows(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    cutoff_date: date,
    cutoff_dt: datetime,
    campaign_id: int | None,
) -> list[dict[str, Any]]:
    planned_cols = schema["planned_topics"]
    campaign_cols = schema.get("content_campaigns", set())
    select_columns = [
        _column_expr(planned_cols, "id", "pt", "id"),
        _column_expr(planned_cols, "campaign_id", "pt", "campaign_id"),
        _column_expr(planned_cols, "topic", "pt", "topic"),
        _column_expr(planned_cols, "target_date", "pt", "target_date"),
        _column_expr(planned_cols, "status", "pt", "status"),
        _column_expr(planned_cols, "content_id", "pt", "content_id"),
        (
            "cc.status AS campaign_status"
            if "content_campaigns" in schema and "status" in campaign_cols
            else "NULL AS campaign_status"
        ),
        (
            "cc.start_date AS campaign_start_date"
            if "content_campaigns" in schema and "start_date" in campaign_cols
            else "NULL AS campaign_start_date"
        ),
        (
            "cc.end_date AS campaign_end_date"
            if "content_campaigns" in schema and "end_date" in campaign_cols
            else "NULL AS campaign_end_date"
        ),
        (
            "CASE WHEN pt.campaign_id IS NOT NULL AND cc.id IS NULL THEN 1 ELSE 0 END "
            "AS campaign_row_missing"
            if "content_campaigns" in schema
            else "0 AS campaign_row_missing"
        ),
    ]
    join = (
        "LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id"
        if "content_campaigns" in schema and "campaign_id" in planned_cols and "id" in campaign_cols
        else ""
    )
    window_filter, params = _window_filter(
        planned_cols,
        cutoff_date=cutoff_date,
        cutoff_dt=cutoff_dt,
    )
    where = [window_filter]
    if campaign_id is not None:
        if "campaign_id" not in planned_cols:
            return []
        where.append("pt.campaign_id = ?")
        params.append(campaign_id)

    order = _order_by(planned_cols)
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM planned_topics pt
            {join}
            WHERE {' AND '.join(where)}
            ORDER BY {order}""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _window_filter(
    planned_cols: set[str],
    *,
    cutoff_date: date,
    cutoff_dt: datetime,
) -> tuple[str, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if "target_date" in planned_cols:
        filters.append("(pt.target_date IS NULL OR date(pt.target_date) >= date(?))")
        params.append(cutoff_date.isoformat())
    else:
        filters.append("1")
    if "created_at" in planned_cols:
        filters.append("datetime(pt.created_at) >= datetime(?)")
        params.append(cutoff_dt.isoformat())
    else:
        filters.append("0")
    return "(" + " OR ".join(filters) + ")", params


def _order_by(planned_cols: set[str]) -> str:
    columns = []
    if "target_date" in planned_cols:
        columns.append("pt.target_date ASC NULLS LAST")
    if "created_at" in planned_cols:
        columns.append("pt.created_at ASC")
    columns.append("pt.id ASC" if "id" in planned_cols else "rowid ASC")
    return ", ".join(columns)


def _out_of_window_reason(row: dict[str, Any]) -> str | None:
    target = _parse_date(row.get("target_date"))
    start = _parse_date(row.get("campaign_start_date"))
    end = _parse_date(row.get("campaign_end_date"))
    if target is None:
        return None
    if start is not None and target < start:
        return "target_date is before the campaign start_date"
    if end is not None and target > end:
        return "target_date is after the campaign end_date"
    return None


def _finding_sort_key(finding: PlannedTopicCollisionFinding) -> tuple[Any, ...]:
    return (
        finding.finding_type,
        finding.target_date or "",
        finding.content_id or 0,
        finding.campaign_id or 0,
        finding.planned_topic_ids,
    )


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    required = {
        "planned_topics": ("id", "topic"),
        "content_campaigns": ("id",),
    }
    optional = {
        "planned_topics": ("campaign_id", "target_date", "status", "content_id", "created_at"),
        "content_campaigns": ("status", "start_date", "end_date"),
    }
    missing: dict[str, tuple[str, ...]] = {}
    for table, columns in {**required, **optional}.items():
        if table not in schema:
            continue
        expected = tuple(dict.fromkeys((*required.get(table, ()), *optional.get(table, ()))))
        absent = tuple(column for column in expected if column not in schema[table])
        if absent:
            missing[table] = absent
    return missing


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row[0]
        schema[table] = {info[1] for info in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _column_expr(columns: set[str], column: str, alias: str, output: str) -> str:
    if column in columns:
        return f"{alias}.{column} AS {output}"
    return f"NULL AS {output}"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_date(value: Any) -> date | None:
    text = _optional_text(value)
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


def _date_key(value: Any) -> str | None:
    parsed = _parse_date(value)
    return parsed.isoformat() if parsed else None


def _normalized_topic(value: Any) -> str:
    text = _optional_text(value)
    if not text:
        return ""
    return " ".join(text.casefold().split())


def _status(row: dict[str, Any]) -> str | None:
    return _status_value(row.get("status"))


def _status_value(value: Any) -> str | None:
    text = _optional_text(value)
    return text.casefold() if text else None


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
