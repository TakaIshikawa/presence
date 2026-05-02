"""Audit generated visual content for missing or weak alt text."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_CHARS = 20
VISUAL_CONTENT_TYPES = {
    "image",
    "image_post",
    "visual",
    "visual_post",
    "x_visual",
}
FINDING_SEVERITY_ORDER = {
    "error": 0,
    "warning": 1,
}


@dataclass(frozen=True)
class VisualAltTextFinding:
    """One alt-text coverage issue for a generated visual item."""

    content_id: int
    finding_type: str
    severity: str
    recommended_action: str
    content_type: str | None
    image_path: str | None
    image_prompt: str | None
    image_alt_text: str | None
    alt_text_length: int
    created_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class VisualAltTextCoverageReport:
    """Read-only visual alt-text coverage report."""

    artifact_type: str
    generated_at: str
    window_days: int
    min_chars: int
    totals: dict[str, int]
    missing_optional_columns: tuple[str, ...]
    findings: tuple[VisualAltTextFinding, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "min_chars": self.min_chars,
            "missing_optional_columns": list(self.missing_optional_columns),
            "totals": self.totals,
            "window_days": self.window_days,
        }


def build_visual_alt_text_coverage_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_chars: int = DEFAULT_MIN_CHARS,
    now: datetime | None = None,
) -> VisualAltTextCoverageReport:
    """Build a visual alt-text coverage report from generated_content rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_chars <= 0:
        raise ValueError("min_chars must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("generated_content")
    if not columns:
        return _empty_report(generated_at, days=days, min_chars=min_chars)

    relevant_columns = ("content_type", "image_path", "image_alt_text")
    missing_columns = tuple(column for column in relevant_columns if column not in columns)
    if "image_alt_text" not in columns or (
        "image_path" not in columns and "content_type" not in columns
    ):
        return _empty_report(
            generated_at,
            days=days,
            min_chars=min_chars,
            missing_optional_columns=missing_columns,
        )

    rows = _visual_rows(conn, columns, days=days, now=generated_at)
    findings = tuple(
        sorted(
            (finding for row in rows for finding in _row_findings(row, min_chars=min_chars)),
            key=lambda finding: (
                FINDING_SEVERITY_ORDER.get(finding.severity, 99),
                finding.content_id,
                finding.finding_type,
            ),
        )
    )
    totals = _totals(rows, findings)
    return VisualAltTextCoverageReport(
        artifact_type="visual_alt_text_coverage",
        generated_at=generated_at.isoformat(),
        window_days=days,
        min_chars=min_chars,
        totals=totals,
        missing_optional_columns=missing_columns,
        findings=findings,
    )


def format_visual_alt_text_coverage_json(
    report: VisualAltTextCoverageReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_visual_alt_text_coverage_text(
    report: VisualAltTextCoverageReport,
) -> str:
    """Render the report as stable operator-facing text."""
    totals = report.totals
    lines = [
        "Visual Alt Text Coverage",
        f"Generated: {report.generated_at}",
        f"Window: {report.window_days} days",
        f"Minimum characters: {report.min_chars}",
        (
            "Totals: "
            f"visual={totals['visual_content']} "
            f"ok={totals['ok']} "
            f"findings={totals['findings']} "
            f"missing={totals['missing_alt_text']} "
            f"too_short={totals['too_short_alt_text']} "
            f"duplicate_prompt={totals['duplicate_prompt_alt_text']}"
        ),
    ]
    if report.missing_optional_columns:
        lines.append("Missing optional columns: " + ", ".join(report.missing_optional_columns))
    lines.append("")

    if not report.findings:
        lines.append("No visual alt-text coverage findings found.")
        return "\n".join(lines)

    for finding in report.findings:
        lines.append(
            f"- content={finding.content_id} severity={finding.severity} "
            f"type={finding.finding_type} length={finding.alt_text_length} "
            f"action={finding.recommended_action}"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")
        }
    return schema


def _visual_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    now: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "content_type"),
        _column_expr(columns, "image_path"),
        _column_expr(columns, "image_prompt"),
        _column_expr(columns, "image_alt_text"),
        _column_expr(columns, "created_at"),
    ]
    cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    where: list[str] = []
    params: list[Any] = []
    visual_terms: list[str] = []
    if "image_path" in columns:
        visual_terms.append("(image_path IS NOT NULL AND trim(image_path) != '')")
    if "content_type" in columns:
        visual_terms.append(
            "(lower(content_type) IN ({}) OR lower(content_type) LIKE '%visual%')".format(
                ", ".join("?" for _ in VISUAL_CONTENT_TYPES)
            )
        )
        params.extend(sorted(VISUAL_CONTENT_TYPES))
    if visual_terms:
        where.append("(" + " OR ".join(visual_terms) + ")")
    if "created_at" in columns:
        where.append("(created_at IS NULL OR created_at >= ?)")
        params.append(cutoff)

    sql = f"SELECT {', '.join(select_columns)} FROM generated_content"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id ASC"
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _row_findings(
    row: dict[str, Any],
    *,
    min_chars: int,
) -> tuple[VisualAltTextFinding, ...]:
    content_id = _int_value(row.get("id"))
    if content_id is None:
        return ()

    alt_text = _clean(row.get("image_alt_text"))
    prompt = _clean(row.get("image_prompt"))
    if not alt_text:
        return (
            _finding(
                row,
                content_id=content_id,
                finding_type="missing_alt_text",
                severity="error",
                recommended_action="write_descriptive_alt_text",
            ),
        )

    if len(alt_text) < min_chars:
        return (
            _finding(
                row,
                content_id=content_id,
                finding_type="too_short_alt_text",
                severity="warning",
                recommended_action="expand_alt_text",
            ),
        )

    if prompt and _normalized_text(alt_text) == _normalized_text(prompt):
        return (
            _finding(
                row,
                content_id=content_id,
                finding_type="duplicate_prompt_alt_text",
                severity="warning",
                recommended_action="rewrite_alt_text_for_accessibility",
            ),
        )
    return ()


def _finding(
    row: dict[str, Any],
    *,
    content_id: int,
    finding_type: str,
    severity: str,
    recommended_action: str,
) -> VisualAltTextFinding:
    alt_text = _clean(row.get("image_alt_text"))
    return VisualAltTextFinding(
        content_id=content_id,
        finding_type=finding_type,
        severity=severity,
        recommended_action=recommended_action,
        content_type=_clean(row.get("content_type")),
        image_path=_clean(row.get("image_path")),
        image_prompt=_clean(row.get("image_prompt")),
        image_alt_text=alt_text,
        alt_text_length=len(alt_text or ""),
        created_at=_clean(row.get("created_at")),
    )


def _totals(
    rows: list[dict[str, Any]],
    findings: tuple[VisualAltTextFinding, ...],
) -> dict[str, int]:
    counts = {
        "visual_content": len(rows),
        "ok": len(rows) - len({finding.content_id for finding in findings}),
        "findings": len(findings),
        "missing_alt_text": 0,
        "too_short_alt_text": 0,
        "duplicate_prompt_alt_text": 0,
    }
    for finding in findings:
        counts[finding.finding_type] += 1
    return counts


def _empty_report(
    generated_at: datetime,
    *,
    days: int,
    min_chars: int,
    missing_optional_columns: tuple[str, ...] = (),
) -> VisualAltTextCoverageReport:
    return VisualAltTextCoverageReport(
        artifact_type="visual_alt_text_coverage",
        generated_at=generated_at.isoformat(),
        window_days=days,
        min_chars=min_chars,
        totals={
            "visual_content": 0,
            "ok": 0,
            "findings": 0,
            "missing_alt_text": 0,
            "too_short_alt_text": 0,
            "duplicate_prompt_alt_text": 0,
        },
        missing_optional_columns=missing_optional_columns,
        findings=(),
    )


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    return column if column in columns else f"{default} AS {column}"


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _int_value(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalized_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
