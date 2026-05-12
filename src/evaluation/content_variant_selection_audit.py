"""Audit content variant selection health across generated content."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30

ISSUE_MISSING_SELECTED_VARIANT = "missing_selected_variant"
ISSUE_MULTIPLE_UNSELECTED_CANDIDATES = "multiple_unselected_candidates"
ISSUE_STALE_SELECTED_VARIANT = "stale_selected_variant"


@dataclass(frozen=True)
class ContentVariantSelectionAuditIssue:
    """One content/platform variant selection issue."""

    issue_type: str
    content_id: int
    platform: str
    variant_ids: tuple[int, ...]
    selected_variant_ids: tuple[int, ...]
    unselected_variant_ids: tuple[int, ...]
    latest_feedback_at: str | None = None
    stale_selected_variant_ids: tuple[int, ...] = ()
    recommendation: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_id": self.content_id,
            "issue_type": self.issue_type,
            "latest_feedback_at": self.latest_feedback_at,
            "platform": self.platform,
            "recommendation": self.recommendation,
            "selected_variant_ids": list(self.selected_variant_ids),
            "stale_selected_variant_ids": list(self.stale_selected_variant_ids),
            "unselected_variant_ids": list(self.unselected_variant_ids),
            "variant_ids": list(self.variant_ids),
        }


@dataclass(frozen=True)
class ContentVariantSelectionAuditReport:
    """Deterministic read-only content variant selection audit."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    issues: tuple[ContentVariantSelectionAuditIssue, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "content_variant_selection_audit",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "issues": [issue.to_dict() for issue in self.issues],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_content_variant_selection_audit_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str | None = None,
    now: datetime | None = None,
) -> ContentVariantSelectionAuditReport:
    """Return a content variant selection audit report."""
    if days <= 0:
        raise ValueError("days must be positive")
    if platform is not None and not platform.strip():
        raise ValueError("platform must not be blank")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": (generated_at - timedelta(days=days)).isoformat(),
        "platform": platform,
    }
    required = {
        "generated_content": {"id"},
        "content_variants": {"id", "content_id", "platform"},
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and not columns.issubset(schema[table])
    }
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at.isoformat(),
            filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    content_ids = _content_ids(conn, schema, cutoff=generated_at - timedelta(days=days))
    variants = _variant_rows(conn, schema, content_ids=content_ids, platform=platform)
    latest_feedback = _latest_feedback_by_content(conn, schema, content_ids)
    groups: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in variants:
        groups.setdefault((int(row["content_id"]), str(row["platform"])), []).append(row)

    issues: list[ContentVariantSelectionAuditIssue] = []
    for (content_id, platform_name), rows in sorted(groups.items(), key=lambda item: item[0]):
        selected = [row for row in rows if int(row.get("selected") or 0) == 1]
        unselected = [row for row in rows if int(row.get("selected") or 0) != 1]
        base = {
            "content_id": content_id,
            "platform": platform_name,
            "variant_ids": tuple(int(row["variant_id"]) for row in rows),
            "selected_variant_ids": tuple(int(row["variant_id"]) for row in selected),
            "unselected_variant_ids": tuple(int(row["variant_id"]) for row in unselected),
        }
        if not selected:
            issues.append(
                ContentVariantSelectionAuditIssue(
                    issue_type=ISSUE_MISSING_SELECTED_VARIANT,
                    recommendation="select one variant before this content is reused",
                    **base,
                )
            )
        if len(unselected) > 1:
            issues.append(
                ContentVariantSelectionAuditIssue(
                    issue_type=ISSUE_MULTIPLE_UNSELECTED_CANDIDATES,
                    recommendation="archive or remove extra unselected candidates",
                    **base,
                )
            )

        latest_feedback_at = latest_feedback.get(content_id)
        stale_selected = [
            row
            for row in selected
            if latest_feedback_at is not None
            and _timestamp_before(row.get("created_at"), latest_feedback_at)
        ]
        if stale_selected:
            issues.append(
                ContentVariantSelectionAuditIssue(
                    issue_type=ISSUE_STALE_SELECTED_VARIANT,
                    latest_feedback_at=latest_feedback_at.isoformat(),
                    stale_selected_variant_ids=tuple(
                        int(row["variant_id"]) for row in stale_selected
                    ),
                    recommendation="refresh the selected variant after newer feedback",
                    **base,
                )
            )

    issues = sorted(
        issues,
        key=lambda issue: (issue.platform, issue.content_id, issue.issue_type, issue.variant_ids),
    )
    return ContentVariantSelectionAuditReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "content_checked": len(content_ids),
            "issues_found": len(issues),
            "variant_groups_checked": len(groups),
        },
        issues=tuple(issues),
    )


def format_content_variant_selection_audit_json(
    report: ContentVariantSelectionAuditReport,
) -> str:
    """Render deterministic JSON suitable for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_variant_selection_audit_text(
    report: ContentVariantSelectionAuditReport,
) -> str:
    """Render a compact human-readable audit report."""
    totals = report.totals
    lines = [
        "Content Variant Selection Audit",
        (
            f"Window: {report.filters['days']} days "
            f"platform={report.filters.get('platform') or 'all'}"
        ),
        (
            f"Checked: content={totals['content_checked']} "
            f"variant_groups={totals['variant_groups_checked']} "
            f"issues={totals['issues_found']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if not report.issues:
        lines.append("No content variant selection audit issues found.")
        return "\n".join(lines)

    lines.append("Issues:")
    for issue in report.issues:
        lines.append(
            "  - "
            f"content_id={issue.content_id} platform={issue.platform} "
            f"type={issue.issue_type} variants={_join_ints(issue.variant_ids)} "
            f"selected={_join_ints(issue.selected_variant_ids)} "
            f"unselected={_join_ints(issue.unselected_variant_ids)}"
        )
        if issue.latest_feedback_at:
            lines.append(
                "    "
                f"latest_feedback_at={issue.latest_feedback_at} "
                f"stale_selected={_join_ints(issue.stale_selected_variant_ids)}"
            )
    return "\n".join(lines)


def _empty_report(
    generated_at: str,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ContentVariantSelectionAuditReport:
    return ContentVariantSelectionAuditReport(
        generated_at=generated_at,
        filters=filters,
        totals={
            "content_checked": 0,
            "issues_found": 0,
            "variant_groups_checked": 0,
        },
        issues=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _content_ids(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> tuple[int, ...]:
    columns = schema["generated_content"]
    where = "WHERE datetime(COALESCE(created_at, ?)) >= datetime(?)" if "created_at" in columns else ""
    params: list[Any] = [cutoff.isoformat(), cutoff.isoformat()] if where else []
    rows = conn.execute(
        f"SELECT id FROM generated_content {where} ORDER BY id ASC",
        params,
    ).fetchall()
    return tuple(int(row["id"]) for row in rows)


def _variant_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    content_ids: tuple[int, ...],
    platform: str | None,
) -> list[dict[str, Any]]:
    if not content_ids:
        return []
    columns = schema["content_variants"]
    selected_expr = "selected" if "selected" in columns else "0"
    created_expr = "created_at" if "created_at" in columns else "NULL"
    placeholders = ",".join("?" for _ in content_ids)
    filters = [f"content_id IN ({placeholders})"]
    params: list[Any] = list(content_ids)
    if platform is not None:
        filters.append("platform = ?")
        params.append(platform)
    rows = conn.execute(
        f"""SELECT id AS variant_id,
                  content_id,
                  platform,
                  {selected_expr} AS selected,
                  {created_expr} AS created_at
           FROM content_variants
           WHERE {' AND '.join(filters)}
           ORDER BY platform ASC, content_id ASC, id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _latest_feedback_by_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: tuple[int, ...],
) -> dict[int, datetime]:
    columns = schema.get("content_feedback")
    if not content_ids or not columns or not {"content_id", "created_at"}.issubset(columns):
        return {}
    placeholders = ",".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT content_id, MAX(created_at) AS latest_feedback_at
            FROM content_feedback
            WHERE content_id IN ({placeholders})
            GROUP BY content_id""",
        list(content_ids),
    ).fetchall()
    latest: dict[int, datetime] = {}
    for row in rows:
        parsed = _parse_timestamp(row["latest_feedback_at"])
        if parsed is not None:
            latest[int(row["content_id"])] = parsed
    return latest


def _timestamp_before(value: Any, cutoff: datetime) -> bool:
    parsed = _parse_timestamp(value)
    return parsed is not None and parsed < cutoff


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_aware(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return _ensure_aware(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        try:
            return _ensure_aware(datetime.strptime(text, "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            return None


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object with conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        str(row["name"]): {
            str(column["name"])
            for column in conn.execute(f"PRAGMA table_info({row['name']})").fetchall()
        }
        for row in tables
    }


def _join_ints(values: tuple[int, ...]) -> str:
    return ",".join(str(value) for value in values) if values else "-"
