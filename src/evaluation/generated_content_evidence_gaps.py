"""Audit generated content rows for missing evidence-packet sections."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
EVIDENCE_AREAS = (
    "source_commits",
    "source_messages",
    "github_activity",
    "claim_check",
    "persona_guard",
    "feedback",
)
OPTIONAL_TABLES = (
    "github_commits",
    "claude_messages",
    "github_activity",
    "content_claim_checks",
    "content_persona_guard",
    "content_feedback",
)


@dataclass(frozen=True)
class GeneratedContentEvidenceGapGroup:
    """Missing evidence sections for one generated_content row."""

    content_id: int
    content_type: str | None
    created_at: str | None
    missing_areas: tuple[str, ...]
    reasons: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["missing_areas"] = list(self.missing_areas)
        return payload


@dataclass(frozen=True)
class GeneratedContentEvidenceGapsReport:
    """Generated content evidence gap audit report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    gap_groups: tuple[GeneratedContentEvidenceGapGroup, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_gaps(self) -> bool:
        return bool(self.gap_groups)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "generated_content_evidence_gaps",
            "filters": dict(self.filters),
            "gap_groups": [group.to_dict() for group in self.gap_groups],
            "generated_at": self.generated_at,
            "has_gaps": self.has_gaps,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_generated_content_evidence_gaps_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> GeneratedContentEvidenceGapsReport:
    """Build a read-only report for generated content evidence packet gaps."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
    }
    missing_tables, missing_columns = _schema_gaps(schema)
    if "generated_content" not in schema or "id" not in schema.get("generated_content", set()):
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_generated_content_rows(conn, schema, cutoff=cutoff, limit=limit)
    get_packet = getattr(db_or_conn, "get_content_evidence_packet", None)
    groups = []
    malformed_source_count = 0
    for row in rows:
        source_refs, malformed = _source_ref_counts(row, schema["generated_content"])
        malformed_source_count += malformed
        packet = get_packet(int(row["id"])) if callable(get_packet) else None
        if packet is None:
            packet = _fallback_packet(row, source_refs, conn=conn, schema=schema)
        group = _gap_group(row, packet, source_refs, schema)
        if group is not None:
            groups.append(group)

    groups.sort(key=lambda group: (group.created_at or "", group.content_id), reverse=True)
    totals = {
        "rows_scanned": len(rows),
        "gap_group_count": len(groups),
        "content_with_gaps": len(groups),
        "malformed_source_field_count": malformed_source_count,
        "missing_area_counts": {
            area: sum(1 for group in groups if area in group.missing_areas)
            for area in EVIDENCE_AREAS
        },
    }
    return GeneratedContentEvidenceGapsReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=totals,
        gap_groups=tuple(groups),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_generated_content_evidence_gaps_json(
    report: GeneratedContentEvidenceGapsReport,
) -> str:
    """Serialize the evidence gap report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_generated_content_evidence_gaps_text(
    report: GeneratedContentEvidenceGapsReport,
) -> str:
    """Render the evidence gap report for command-line review."""
    totals = report.totals
    lines = [
        "Generated Content Evidence Gaps",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"limit={report.filters['limit']}"
        ),
        (
            "Totals: "
            f"rows_scanned={totals['rows_scanned']} "
            f"gap_groups={totals['gap_group_count']}"
        ),
    ]
    if totals["malformed_source_field_count"]:
        lines.append(f"Malformed source fields: {totals['malformed_source_field_count']}")
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
            if columns
        ]
        if missing:
            lines.append("Missing columns: " + "; ".join(missing))

    if not report.gap_groups:
        lines.extend(["", "No generated content evidence gaps found."])
        return "\n".join(lines)

    lines.extend(["", "Gaps:"])
    for group in report.gap_groups:
        lines.append(
            f"  - content_id={group.content_id} "
            f"type={group.content_type or '-'} "
            f"created_at={group.created_at or '-'} "
            f"missing={', '.join(group.missing_areas)}"
        )
    return "\n".join(lines)


def _load_generated_content_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    limit: int,
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    select_columns = [
        "gc.id AS id",
        _column_expr(columns, "content_type", "gc", "content_type"),
        _column_expr(columns, "created_at", "gc", "created_at"),
        _column_expr(columns, "source_commits", "gc", "source_commits"),
        _column_expr(columns, "source_messages", "gc", "source_messages"),
        _column_expr(columns, "source_activity_ids", "gc", "source_activity_ids"),
    ]
    where = ""
    params: list[Any] = []
    if "created_at" in columns:
        where = "WHERE gc.created_at >= ?"
        params.append(cutoff.isoformat())
    order = "gc.created_at DESC, gc.id DESC" if "created_at" in columns else "gc.id DESC"
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM generated_content gc
            {where}
            ORDER BY {order}
            LIMIT ?""",
        (*params, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def _gap_group(
    row: dict[str, Any],
    packet: dict[str, Any],
    source_refs: dict[str, int],
    schema: dict[str, set[str]],
) -> GeneratedContentEvidenceGapGroup | None:
    reasons: dict[str, str] = {}
    if source_refs["source_commits"] == 0:
        reasons["source_commits"] = "no source commit references on generated_content"
    elif not _has_matched_item(packet.get("source_commits")):
        reasons["source_commits"] = "source commit references have no matched commit evidence"

    if source_refs["source_messages"] == 0:
        reasons["source_messages"] = "no source message references on generated_content"
    elif not _has_matched_item(packet.get("source_messages")):
        reasons["source_messages"] = "source message references have no matched message evidence"

    if source_refs["source_activity_ids"] == 0:
        reasons["github_activity"] = "no GitHub activity references on generated_content"
    elif not _has_matched_item(packet.get("source_github_activity")):
        reasons["github_activity"] = "GitHub activity references have no matched activity evidence"

    if packet.get("claim_check") is None:
        reasons["claim_check"] = (
            "content_claim_checks table is unavailable"
            if "content_claim_checks" not in schema
            else "no claim-check summary for content"
        )
    if packet.get("persona_guard") is None:
        reasons["persona_guard"] = (
            "content_persona_guard table is unavailable"
            if "content_persona_guard" not in schema
            else "no persona guard summary for content"
        )
    if not packet.get("feedback"):
        reasons["feedback"] = (
            "content_feedback table is unavailable"
            if "content_feedback" not in schema
            else "no feedback records for content"
        )

    if not reasons:
        return None
    return GeneratedContentEvidenceGapGroup(
        content_id=int(row["id"]),
        content_type=row.get("content_type"),
        created_at=row.get("created_at"),
        missing_areas=tuple(area for area in EVIDENCE_AREAS if area in reasons),
        reasons={area: reasons[area] for area in EVIDENCE_AREAS if area in reasons},
    )


def _fallback_packet(
    row: dict[str, Any],
    source_refs: dict[str, int],
    *,
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, Any]:
    content_id = int(row["id"])
    return {
        "source_commits": _matched_refs(
            conn,
            schema,
            table="github_commits",
            column="commit_sha",
            raw_refs=row.get("source_commits"),
            id_key="commit_sha",
        )
        if source_refs["source_commits"]
        else [],
        "source_messages": _matched_refs(
            conn,
            schema,
            table="claude_messages",
            column="message_uuid",
            raw_refs=row.get("source_messages"),
            id_key="message_uuid",
        )
        if source_refs["source_messages"]
        else [],
        "source_github_activity": _matched_refs(
            conn,
            schema,
            table="github_activity",
            column="activity_id",
            raw_refs=row.get("source_activity_ids"),
            id_key="activity_id",
        )
        if source_refs["source_activity_ids"]
        else [],
        "claim_check": _one_row(conn, schema, "content_claim_checks", content_id),
        "persona_guard": _one_row(conn, schema, "content_persona_guard", content_id),
        "feedback": _feedback_rows(conn, schema, content_id),
    }


def _matched_refs(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    table: str,
    column: str,
    raw_refs: Any,
    id_key: str,
) -> list[dict[str, Any]]:
    refs = _json_list(raw_refs)
    if table not in schema or column not in schema[table]:
        return [{id_key: str(ref), "source_index": index, "matched": False} for index, ref in enumerate(refs)]
    placeholders = ", ".join("?" for _ in refs)
    found = {
        str(row[0])
        for row in conn.execute(
            f"SELECT {column} FROM {table} WHERE {column} IN ({placeholders})",
            tuple(str(ref) for ref in refs),
        ).fetchall()
    }
    return [
        {id_key: str(ref), "source_index": index, "matched": str(ref) in found}
        for index, ref in enumerate(refs)
    ]


def _one_row(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    table: str,
    content_id: int,
) -> dict[str, Any] | None:
    if table not in schema or "content_id" not in schema[table]:
        return None
    row = conn.execute(
        f"SELECT * FROM {table} WHERE content_id = ? LIMIT 1",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None


def _feedback_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> list[dict[str, Any]]:
    if "content_feedback" not in schema or "content_id" not in schema["content_feedback"]:
        return []
    rows = conn.execute(
        "SELECT * FROM content_feedback WHERE content_id = ? ORDER BY id DESC LIMIT 5",
        (content_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _source_ref_counts(
    row: dict[str, Any],
    columns: set[str],
) -> tuple[dict[str, int], int]:
    counts: dict[str, int] = {}
    malformed = 0
    for field in ("source_commits", "source_messages", "source_activity_ids"):
        if field not in columns:
            counts[field] = 0
            continue
        values, bad = _json_list_with_malformed(row.get(field))
        counts[field] = len(values)
        malformed += int(bad)
    return counts, malformed


def _has_matched_item(value: Any) -> bool:
    if not isinstance(value, list) or not value:
        return False
    return any(item.get("matched") is not False for item in value if isinstance(item, dict))


def _json_list(value: Any) -> list[Any]:
    parsed, _malformed = _json_list_with_malformed(value)
    return parsed


def _json_list_with_malformed(value: Any) -> tuple[list[Any], bool]:
    if value in (None, ""):
        return [], False
    if isinstance(value, list):
        return value, False
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return [], True
    if not isinstance(parsed, list):
        return [], True
    return parsed, False


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    missing_tables = tuple(
        table for table in ("generated_content", *OPTIONAL_TABLES) if table not in schema
    )
    required_columns = {
        "generated_content": {
            "id",
            "content_type",
            "created_at",
            "source_commits",
            "source_messages",
            "source_activity_ids",
        },
        "content_claim_checks": {"content_id"},
        "content_persona_guard": {"content_id"},
        "content_feedback": {"content_id"},
        "github_commits": {"commit_sha"},
        "claude_messages": {"message_uuid"},
        "github_activity": {"activity_id"},
    }
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required_columns.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> GeneratedContentEvidenceGapsReport:
    return GeneratedContentEvidenceGapsReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "rows_scanned": 0,
            "gap_group_count": 0,
            "content_with_gaps": 0,
            "malformed_source_field_count": 0,
            "missing_area_counts": {area: 0 for area in EVIDENCE_AREAS},
        },
        gap_groups=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
            for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
        for table in tables
    }


def _column_expr(columns: set[str], column: str, alias: str, output: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"NULL AS {output}"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
