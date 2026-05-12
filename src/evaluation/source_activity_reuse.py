"""Report GitHub source activities reused across generated posts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_WARNING_THRESHOLD = 3
DEFAULT_CRITICAL_THRESHOLD = 5


@dataclass(frozen=True)
class SourceActivityReuseRow:
    source_activity_id: str
    reuse_count: int
    content_ids: tuple[int, ...]
    newest_created_at: str | None
    severity: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["content_ids"] = list(self.content_ids)
        return payload


@dataclass(frozen=True)
class SourceActivityReuseReport:
    generated_at: str
    filters: dict[str, Any]
    rows: tuple[SourceActivityReuseRow, ...]
    malformed_rows: tuple[dict[str, Any], ...]
    totals: dict[str, int]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "source_activity_reuse",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "malformed_rows": [dict(row) for row in self.malformed_rows],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_source_activity_reuse_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    warning_threshold: int = DEFAULT_WARNING_THRESHOLD,
    critical_threshold: int = DEFAULT_CRITICAL_THRESHOLD,
    now: datetime | None = None,
) -> SourceActivityReuseReport:
    """Flag source_activity_ids reused across too many generated posts."""
    if days <= 0:
        raise ValueError("days must be positive")
    if warning_threshold <= 0:
        raise ValueError("warning_threshold must be positive")
    if critical_threshold < warning_threshold:
        raise ValueError("critical_threshold must be >= warning_threshold")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {
        "days": days,
        "warning_threshold": warning_threshold,
        "critical_threshold": critical_threshold,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "generated_content" not in schema:
        return _report(generated_at, filters, (), (), ("generated_content",), {})
    columns = schema["generated_content"]
    required = ("id", "source_activity_ids")
    optional = ("created_at",)
    missing_required = tuple(column for column in required if column not in columns)
    missing_optional = tuple(column for column in optional if column not in columns)
    missing_columns = {}
    if missing_required:
        missing_columns["generated_content"] = missing_required + missing_optional
        return _report(generated_at, filters, (), (), (), missing_columns)
    if missing_optional:
        missing_columns["generated_content"] = missing_optional

    activity_posts: dict[str, dict[str, Any]] = {}
    malformed: list[dict[str, Any]] = []
    for record in _content_records(conn, columns, days=days, now=generated_at):
        content_id = int(record["id"])
        raw_value = record["source_activity_ids"]
        if raw_value is None or str(raw_value).strip() == "":
            continue
        activity_ids, error = _parse_activity_ids(raw_value)
        if error:
            malformed.append(
                {
                    "content_id": content_id,
                    "raw_source_activity_ids": raw_value,
                    "error": error,
                }
            )
            continue
        for activity_id in activity_ids:
            bucket = activity_posts.setdefault(
                activity_id,
                {"content_ids": set(), "newest_created_at": None},
            )
            bucket["content_ids"].add(content_id)
            created_at = _clean(record["created_at"])
            if created_at and (
                bucket["newest_created_at"] is None
                or created_at > bucket["newest_created_at"]
            ):
                bucket["newest_created_at"] = created_at

    rows = tuple(
        sorted(
            (
                SourceActivityReuseRow(
                    source_activity_id=activity_id,
                    reuse_count=len(bucket["content_ids"]),
                    content_ids=tuple(sorted(bucket["content_ids"])),
                    newest_created_at=bucket["newest_created_at"],
                    severity=_severity(
                        len(bucket["content_ids"]),
                        warning_threshold=warning_threshold,
                        critical_threshold=critical_threshold,
                    ),
                )
                for activity_id, bucket in activity_posts.items()
                if len(bucket["content_ids"]) >= warning_threshold
            ),
            key=lambda row: (
                {"critical": 0, "warning": 1}.get(row.severity, 99),
                -row.reuse_count,
                row.source_activity_id,
            ),
        )
    )
    return _report(generated_at, filters, rows, tuple(malformed), (), missing_columns)


def format_source_activity_reuse_json(report: SourceActivityReuseReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_activity_reuse_text(report: SourceActivityReuseReport) -> str:
    lines = [
        "Source Activity Reuse",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={report.filters['days']} "
            f"warning_threshold={report.filters['warning_threshold']} "
            f"critical_threshold={report.filters['critical_threshold']}"
        ),
        (
            "Totals: "
            f"rows={report.totals['row_count']} "
            f"warning={report.totals['warning']} "
            f"critical={report.totals['critical']} "
            f"malformed={report.totals['malformed_rows']}"
        ),
        "",
    ]
    if not report.rows:
        lines.append("No source activity reuse above threshold found.")
        return "\n".join(lines)
    for row in report.rows:
        lines.append(
            f"- activity={row.source_activity_id} severity={row.severity} "
            f"reuse_count={row.reuse_count} content_ids={','.join(map(str, row.content_ids))}"
        )
    return "\n".join(lines)


def _content_records(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    now: datetime,
) -> list[sqlite3.Row]:
    select_columns = ["id", "source_activity_ids", _column_expr(columns, "created_at")]
    where = ""
    params: list[Any] = []
    if "created_at" in columns:
        cutoff = (now - timedelta(days=days)).isoformat()
        where = "WHERE created_at IS NULL OR created_at >= ?"
        params.append(cutoff)
    return conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM generated_content
            {where}
            ORDER BY id ASC""",
        params,
    ).fetchall()


def _parse_activity_ids(raw_value: Any) -> tuple[tuple[str, ...], str | None]:
    try:
        parsed = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError) as exc:
        return (), f"invalid_json: {exc}"
    if parsed is None:
        return (), None
    if not isinstance(parsed, list):
        return (), f"non_list_json: {type(parsed).__name__}"
    values = []
    for item in parsed:
        cleaned = _clean(item)
        if cleaned:
            values.append(cleaned)
    return tuple(dict.fromkeys(values)), None


def _severity(
    reuse_count: int,
    *,
    warning_threshold: int,
    critical_threshold: int,
) -> str:
    if reuse_count >= critical_threshold:
        return "critical"
    if reuse_count >= warning_threshold:
        return "warning"
    return "ok"


def _report(
    generated_at: datetime,
    filters: dict[str, Any],
    rows: tuple[SourceActivityReuseRow, ...],
    malformed_rows: tuple[dict[str, Any], ...],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> SourceActivityReuseReport:
    return SourceActivityReuseReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        rows=rows,
        malformed_rows=malformed_rows,
        totals={
            "row_count": len(rows),
            "warning": sum(row.severity == "warning" for row in rows),
            "critical": sum(row.severity == "critical" for row in rows),
            "malformed_rows": len(malformed_rows),
        },
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in tables:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    return column if column in columns else f"{default} AS {column}"


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
