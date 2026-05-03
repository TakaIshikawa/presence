"""Audit generated content rows for missing topic coverage."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_CONFIDENCE = 0.5


@dataclass(frozen=True)
class GeneratedContentTopicCoverageFinding:
    """One generated content topic coverage issue."""

    finding_type: str
    content_id: int
    content_type: str | None
    published_state: str
    topic_id: int | None
    topic: str | None
    confidence: float | None
    created_at: str | None
    published_at: str | None
    reason: str
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GeneratedContentTopicCoverageReport:
    """Generated content topic coverage audit report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[GeneratedContentTopicCoverageFinding, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def has_issues(self) -> bool:
        return bool(self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "generated_content_topic_coverage",
            "findings": [finding.to_dict() for finding in self.findings],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": _sorted_totals(self.totals),
        }


def build_generated_content_topic_coverage_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    content_type: str | None = None,
    published_only: bool = False,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    now: datetime | None = None,
) -> GeneratedContentTopicCoverageReport:
    """Build a read-only report for generated content topic coverage gaps."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_confidence < 0 or min_confidence > 1:
        raise ValueError("min_confidence must be between 0 and 1")
    normalized_content_type = _optional_text(content_type)

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "content_type": normalized_content_type,
        "days": days,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "min_confidence": min_confidence,
        "published_only": published_only,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(
        table for table in ("generated_content", "content_topics") if table not in schema
    )
    missing_columns = _missing_columns(schema)
    if "generated_content" not in schema or "id" not in schema.get("generated_content", set()):
        return GeneratedContentTopicCoverageReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_totals([], []),
            findings=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    content_rows = _load_generated_content_rows(
        conn,
        schema=schema,
        cutoff=cutoff,
        content_type=normalized_content_type,
        published_only=published_only,
    )
    topic_rows = _load_topic_rows(conn, schema, content_rows)
    findings = _findings(
        content_rows,
        topic_rows,
        has_content_topics_table="content_topics" in schema,
        min_confidence=min_confidence,
    )
    findings.sort(key=_finding_sort_key)

    return GeneratedContentTopicCoverageReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(content_rows, findings),
        findings=tuple(findings),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_generated_content_topic_coverage_json(
    report: GeneratedContentTopicCoverageReport,
) -> str:
    """Serialize a topic coverage report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_generated_content_topic_coverage_text(
    report: GeneratedContentTopicCoverageReport,
) -> str:
    """Render a concise human-readable topic coverage report."""
    filters = report.filters
    totals = report.totals
    issue_counts = totals["by_issue_type"]
    lines = [
        "Generated Content Topic Coverage",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} "
            f"content_type={filters['content_type'] or '-'} "
            f"published_only={int(filters['published_only'])} "
            f"min_confidence={filters['min_confidence']}"
        ),
        (
            "Totals: "
            f"content={totals['content_scanned']} "
            f"findings={totals['findings']} "
            f"missing_topic={issue_counts['missing_topic']} "
            f"blank_topic={issue_counts['blank_topic']} "
            f"low_confidence={issue_counts['low_confidence']}"
        ),
        "By content_type: " + _format_count_map(totals["by_content_type"]),
        "By published_state: " + _format_count_map(totals["by_published_state"]),
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
        lines.extend(["", "No generated content topic coverage issues found."])
        return "\n".join(lines)

    lines.extend(["", "Findings:"])
    for finding in report.findings:
        confidence = "-" if finding.confidence is None else f"{finding.confidence:g}"
        lines.append(
            f"- type={finding.finding_type} content_id={finding.content_id} "
            f"content_type={finding.content_type or '-'} "
            f"published_state={finding.published_state} topic_id={finding.topic_id or '-'} "
            f"topic={finding.topic or '-'} confidence={confidence}"
        )
        lines.append(f"  reason={finding.reason}")
        lines.append(f"  recommended_action={finding.recommended_action}")
    return "\n".join(lines)


def _load_generated_content_rows(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    cutoff: datetime,
    content_type: str | None,
    published_only: bool,
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    select_columns = [
        "gc.id AS id",
        _column_expr(columns, "content_type", "gc", "content_type"),
        _column_expr(columns, "published", "gc", "published"),
        _column_expr(columns, "published_url", "gc", "published_url"),
        _column_expr(columns, "published_at", "gc", "published_at"),
        _column_expr(columns, "created_at", "gc", "created_at"),
    ]
    where = [_lookback_filter(columns)]
    params: list[Any] = []
    if "created_at" in columns:
        params.append(cutoff.isoformat())
    if content_type is not None:
        if "content_type" not in columns:
            return []
        where.append("gc.content_type = ?")
        params.append(content_type)
    if published_only:
        where.append(_published_filter(columns))

    order = "gc.created_at ASC, gc.id ASC" if "created_at" in columns else "gc.id ASC"
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM generated_content gc
            WHERE {' AND '.join(where)}
            ORDER BY {order}""",
        params,
    ).fetchall()
    return [_with_published_state(dict(row)) for row in rows]


def _load_topic_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_rows: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    if not content_rows or "content_topics" not in schema:
        return {}
    columns = schema["content_topics"]
    if "content_id" not in columns:
        return {}
    content_ids = [int(row["id"]) for row in content_rows]
    placeholders = ", ".join("?" for _ in content_ids)
    select_columns = [
        _column_expr(columns, "id", "ct", "id"),
        "ct.content_id AS content_id",
        _column_expr(columns, "topic", "ct", "topic"),
        _column_expr(columns, "confidence", "ct", "confidence"),
    ]
    order = "ct.content_id ASC"
    if "id" in columns:
        order += ", ct.id ASC"
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM content_topics ct
            WHERE ct.content_id IN ({placeholders})
            ORDER BY {order}""",
        content_ids,
    ).fetchall()
    topics_by_content: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        payload = dict(row)
        content_id = _optional_int(payload.get("content_id"))
        if content_id is not None:
            topics_by_content.setdefault(content_id, []).append(payload)
    return topics_by_content


def _findings(
    content_rows: list[dict[str, Any]],
    topic_rows: dict[int, list[dict[str, Any]]],
    *,
    has_content_topics_table: bool,
    min_confidence: float,
) -> list[GeneratedContentTopicCoverageFinding]:
    findings: list[GeneratedContentTopicCoverageFinding] = []
    for content in content_rows:
        content_id = int(content["id"])
        topics = topic_rows.get(content_id, [])
        if not topics:
            reason = "generated content has no content_topics rows"
            if not has_content_topics_table:
                reason = "content_topics table is missing; topic coverage cannot be verified"
            findings.append(
                _finding(
                    content,
                    finding_type="missing_topic",
                    topic=None,
                    reason=reason,
                    recommended_action="insert_content_topic_assignment",
                )
            )
            continue

        for topic in topics:
            topic_label = _optional_text(topic.get("topic"))
            if topic_label is None:
                findings.append(
                    _finding(
                        content,
                        topic=topic,
                        finding_type="blank_topic",
                        reason="content_topics row has a blank topic value",
                        recommended_action="replace_blank_topic_label",
                    )
                )
            confidence = _optional_float(topic.get("confidence"))
            if confidence is not None and confidence < min_confidence:
                findings.append(
                    _finding(
                        content,
                        topic=topic,
                        finding_type="low_confidence",
                        reason=(
                            "content_topics confidence is below the configured "
                            "minimum threshold"
                        ),
                        recommended_action="review_or_regenerate_topic_assignment",
                    )
                )
    return findings


def _finding(
    content: dict[str, Any],
    *,
    finding_type: str,
    reason: str,
    recommended_action: str,
    topic: dict[str, Any] | None = None,
) -> GeneratedContentTopicCoverageFinding:
    return GeneratedContentTopicCoverageFinding(
        finding_type=finding_type,
        content_id=int(content["id"]),
        content_type=_optional_text(content.get("content_type")),
        published_state=content.get("published_state") or "unpublished",
        topic_id=_optional_int((topic or {}).get("id")),
        topic=_optional_text((topic or {}).get("topic")),
        confidence=_optional_float((topic or {}).get("confidence")),
        created_at=_optional_text(content.get("created_at")),
        published_at=_optional_text(content.get("published_at")),
        reason=reason,
        recommended_action=recommended_action,
    )


def _totals(
    content_rows: list[dict[str, Any]],
    findings: list[GeneratedContentTopicCoverageFinding],
) -> dict[str, Any]:
    by_content_type: dict[str, int] = {}
    by_published_state: dict[str, int] = {}
    for row in content_rows:
        by_content_type[_optional_text(row.get("content_type")) or "unknown"] = (
            by_content_type.get(_optional_text(row.get("content_type")) or "unknown", 0) + 1
        )
        state = row.get("published_state") or "unpublished"
        by_published_state[state] = by_published_state.get(state, 0) + 1

    by_issue_type = {"blank_topic": 0, "low_confidence": 0, "missing_topic": 0}
    for finding in findings:
        by_issue_type[finding.finding_type] = by_issue_type.get(finding.finding_type, 0) + 1

    return {
        "by_content_type": dict(sorted(by_content_type.items())),
        "by_issue_type": dict(sorted(by_issue_type.items())),
        "by_published_state": dict(sorted(by_published_state.items())),
        "content_scanned": len(content_rows),
        "findings": len(findings),
    }


def _sorted_totals(totals: dict[str, Any]) -> dict[str, Any]:
    payload = dict(totals)
    for key in ("by_content_type", "by_issue_type", "by_published_state"):
        payload[key] = dict(sorted(payload.get(key, {}).items()))
    return dict(sorted(payload.items()))


def _format_count_map(values: dict[str, int]) -> str:
    if not values:
        return "-"
    return ", ".join(f"{key}={value}" for key, value in sorted(values.items()))


def _lookback_filter(columns: set[str]) -> str:
    if "created_at" in columns:
        return "datetime(gc.created_at) >= datetime(?)"
    return "1"


def _published_filter(columns: set[str]) -> str:
    filters = []
    if "published" in columns:
        filters.append(
            "(gc.published = 1 OR lower(trim(CAST(gc.published AS TEXT))) "
            "IN ('true', 'yes', 'published'))"
        )
    if "published_url" in columns:
        filters.append("(gc.published_url IS NOT NULL AND trim(gc.published_url) != '')")
    if "published_at" in columns:
        filters.append("(gc.published_at IS NOT NULL AND trim(gc.published_at) != '')")
    if not filters:
        return "0"
    return "(" + " OR ".join(filters) + ")"


def _with_published_state(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["published_state"] = "published" if _is_published(payload) else "unpublished"
    return payload


def _is_published(row: dict[str, Any]) -> bool:
    published = row.get("published")
    if isinstance(published, str):
        if published.strip().lower() in {"1", "true", "yes", "published"}:
            return True
    elif published:
        return True
    return bool(_optional_text(row.get("published_url")) or _optional_text(row.get("published_at")))


def _finding_sort_key(finding: GeneratedContentTopicCoverageFinding) -> tuple[Any, ...]:
    return (
        finding.finding_type,
        finding.content_type or "",
        finding.published_state,
        finding.content_id,
        finding.topic_id or 0,
    )


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "generated_content": (
            "id",
            "content_type",
            "published",
            "published_url",
            "published_at",
            "created_at",
        ),
        "content_topics": ("id", "content_id", "topic", "confidence"),
    }
    missing: dict[str, tuple[str, ...]] = {}
    for table, columns in expected.items():
        if table not in schema:
            continue
        absent = tuple(column for column in columns if column not in schema[table])
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


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
