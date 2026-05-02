"""Summarize curated source license and reuse posture."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_STALE_AFTER_DAYS = 90
KNOWN_LICENSES = {"open", "attribution_required", "restricted"}
KNOWLEDGE_SOURCE_TYPES = {
    "curated_x": "x_account",
    "curated_article": "blog",
    "curated_newsletter": "newsletter",
}


@dataclass(frozen=True)
class SourceLicenseSummaryRow:
    """License posture for one curated source."""

    source_id: int
    source_type: str
    identifier: str
    name: str | None
    license_label: str
    reuse_allowed: bool
    attribution_required: bool
    last_checked_at: str | None
    stale_license: bool
    item_count: int
    blocker_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceLicenseSummaryReport:
    """Read-only inventory of curated source license posture."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    sources: tuple[SourceLicenseSummaryRow, ...]
    missing_required_tables: tuple[str, ...] = ()
    missing_optional_tables: tuple[str, ...] = ()
    missing_optional_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": self.filters,
            "totals": self.totals,
            "sources": [source.to_dict() for source in self.sources],
            "missing_required_tables": list(self.missing_required_tables),
            "missing_optional_tables": list(self.missing_optional_tables),
            "missing_optional_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_optional_columns or {}).items())
            },
        }


def build_source_license_summary_report(
    db_or_conn: Any,
    *,
    source_type: str | None = None,
    stale_after_days: int = DEFAULT_STALE_AFTER_DAYS,
    now: datetime | None = None,
) -> SourceLicenseSummaryReport:
    """Build a read-only license inventory for curated sources."""
    if source_type is not None and not source_type.strip():
        raise ValueError("source_type must not be blank")
    if stale_after_days <= 0:
        raise ValueError("stale_after_days must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {
        "source_type": source_type.strip() if source_type else None,
        "stale_after_days": stale_after_days,
    }
    if "curated_sources" not in schema:
        return SourceLicenseSummaryReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_totals(()),
            sources=(),
            missing_required_tables=("curated_sources",),
            missing_optional_tables=_missing_optional_tables(schema),
            missing_optional_columns=_missing_optional_columns(schema),
        )

    item_counts = _load_item_counts(conn, schema)
    rows = tuple(
        _summary_row(
            row,
            item_count=item_counts.get(_source_key(row), 0),
            stale_after_days=stale_after_days,
            now=generated_at,
        )
        for row in _load_source_rows(conn, schema, source_type=filters["source_type"])
    )
    return SourceLicenseSummaryReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(rows),
        sources=rows,
        missing_optional_tables=_missing_optional_tables(schema),
        missing_optional_columns=_missing_optional_columns(schema),
    )


def format_source_license_summary_json(report: SourceLicenseSummaryReport) -> str:
    """Serialize a source license summary as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_license_summary_text(report: SourceLicenseSummaryReport) -> str:
    """Render a source license summary for operator review."""
    lines = [
        "Source License Summary",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"source_type={report.filters['source_type'] or 'all'} "
            f"stale_after_days={report.filters['stale_after_days']}"
        ),
        (
            "Totals: "
            f"sources={report.totals['source_count']} "
            f"reuse_allowed={report.totals['reuse_allowed_count']} "
            f"attribution_required={report.totals['attribution_required_count']} "
            f"stale={report.totals['stale_license_count']} "
            f"blocked={report.totals['blocked_count']} "
            f"items={report.totals['item_count']}"
        ),
    ]
    if report.missing_required_tables:
        lines.append("Missing required tables: " + ", ".join(report.missing_required_tables))
    if report.missing_optional_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_optional_tables))
    if report.missing_optional_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_optional_columns.items())
        )
        lines.append("Missing optional columns: " + details)
    if not report.sources:
        lines.append("No curated sources found for the selected filters.")
        return "\n".join(lines)

    lines.append("Sources:")
    for source in report.sources:
        lines.append(
            f"- #{source.source_id} {source.source_type} {source.identifier}: "
            f"license={source.license_label} "
            f"reuse_allowed={_yes_no(source.reuse_allowed)} "
            f"attribution_required={_yes_no(source.attribution_required)} "
            f"last_checked={source.last_checked_at or '-'} "
            f"stale={_yes_no(source.stale_license)} "
            f"items={source.item_count} "
            f"blocker={source.blocker_reason or '-'}"
        )
    return "\n".join(lines)


def _summary_row(
    row: dict[str, Any],
    *,
    item_count: int,
    stale_after_days: int,
    now: datetime,
) -> SourceLicenseSummaryRow:
    license_label = _clean(row.get("license")) or "unknown"
    normalized_license = license_label.casefold()
    last_checked_at = _last_checked_at(row)
    stale_license = _is_stale(last_checked_at, stale_after_days, now)
    blocker_reason = _blocker_reason(normalized_license, stale_license)
    reuse_allowed = normalized_license in {"open", "attribution_required"} and not blocker_reason
    attribution_required = normalized_license == "attribution_required"
    if normalized_license == "restricted":
        attribution_required = True
    return SourceLicenseSummaryRow(
        source_id=int(row.get("id") or 0),
        source_type=_clean(row.get("source_type")) or "",
        identifier=_clean(row.get("identifier")) or "",
        name=_clean(row.get("name")),
        license_label=license_label,
        reuse_allowed=reuse_allowed,
        attribution_required=attribution_required,
        last_checked_at=last_checked_at,
        stale_license=stale_license,
        item_count=item_count,
        blocker_reason=blocker_reason,
    )


def _blocker_reason(license_label: str, stale_license: bool) -> str | None:
    if license_label == "restricted":
        return "restricted_license"
    if license_label not in KNOWN_LICENSES:
        return "unknown_license"
    if stale_license:
        return "stale_license_review"
    return None


def _load_source_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    source_type: str | None,
) -> list[dict[str, Any]]:
    columns = schema["curated_sources"]
    select = [
        _column_expr(columns, "id"),
        _column_expr(columns, "source_type"),
        _column_expr(columns, "identifier"),
        _column_expr(columns, "name"),
        _column_expr(columns, "license"),
        _column_expr(columns, "reviewed_at"),
        _column_expr(columns, "last_success_at"),
        _column_expr(columns, "last_failure_at"),
        _column_expr(columns, "feed_last_modified"),
        _column_expr(columns, "created_at"),
    ]
    where = ""
    params: tuple[Any, ...] = ()
    if source_type is not None:
        where = "WHERE source_type = ?"
        params = (source_type,)
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM curated_sources
            {where}
            ORDER BY source_type ASC, identifier ASC, id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _load_item_counts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[tuple[str, str], int]:
    columns = schema.get("knowledge")
    if not columns or not {"source_type", "source_id"} <= columns:
        return {}
    select = [
        _column_expr(columns, "source_type"),
        _column_expr(columns, "source_id"),
        _column_expr(columns, "source_url"),
        _column_expr(columns, "author"),
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM knowledge
            ORDER BY id ASC"""
    ).fetchall()
    counts: dict[tuple[str, str], int] = {}
    for raw in rows:
        keys = _candidate_source_keys(dict(raw))
        for key in keys:
            counts[key] = counts.get(key, 0) + 1
    return counts


def _candidate_source_keys(row: dict[str, Any]) -> set[tuple[str, str]]:
    curated_type = KNOWLEDGE_SOURCE_TYPES.get(_clean(row.get("source_type")) or "")
    if not curated_type:
        return set()
    values = {
        _normalize_identifier(row.get("author")),
        _normalize_identifier(row.get("source_id")),
        _normalize_identifier(_host(row.get("source_url"))),
        _normalize_identifier(_host(row.get("source_id"))),
    }
    values.discard("")
    return {(curated_type, value) for value in values}


def _source_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        _clean(row.get("source_type")) or "",
        _normalize_identifier(row.get("identifier")),
    )


def _last_checked_at(row: dict[str, Any]) -> str | None:
    reviewed_at = _clean(row.get("reviewed_at"))
    if reviewed_at:
        return reviewed_at
    return _max_timestamp(
        _clean(row.get("last_success_at")),
        _clean(row.get("last_failure_at")),
        _clean(row.get("feed_last_modified")),
        _clean(row.get("created_at")),
    )


def _is_stale(value: str | None, stale_after_days: int, now: datetime) -> bool:
    parsed = _parse_datetime(value)
    if parsed is None:
        return True
    return parsed < now - timedelta(days=stale_after_days)


def _totals(rows: tuple[SourceLicenseSummaryRow, ...]) -> dict[str, int]:
    return {
        "source_count": len(rows),
        "reuse_allowed_count": sum(1 for row in rows if row.reuse_allowed),
        "attribution_required_count": sum(1 for row in rows if row.attribution_required),
        "stale_license_count": sum(1 for row in rows if row.stale_license),
        "blocked_count": sum(1 for row in rows if row.blocker_reason),
        "item_count": sum(row.item_count for row in rows),
    }


def _missing_optional_tables(schema: dict[str, set[str]]) -> tuple[str, ...]:
    return tuple(sorted(table for table in ("knowledge",) if table not in schema))


def _missing_optional_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    requirements = {
        "curated_sources": (
            "name",
            "license",
            "reviewed_at",
            "last_success_at",
            "last_failure_at",
            "feed_last_modified",
            "created_at",
        ),
        "knowledge": ("source_type", "source_id", "source_url", "author"),
    }
    missing: dict[str, tuple[str, ...]] = {}
    for table, columns in requirements.items():
        if table not in schema:
            continue
        absent = tuple(column for column in columns if column not in schema[table])
        if absent:
            missing[table] = absent
    return missing


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {
            str(column[1]) for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _column_expr(columns: set[str], name: str, table_alias: str | None = None) -> str:
    qualified = f"{table_alias}.{name}" if table_alias else name
    if name in columns:
        return f"{qualified} AS {name}"
    return f"NULL AS {name}"


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _host(value: Any) -> str:
    text = _clean(value) or ""
    parsed = urlparse(text)
    return parsed.netloc or text


def _normalize_identifier(value: Any) -> str:
    text = (_clean(value) or "").casefold()
    if text.startswith("@"):
        text = text[1:]
    if text.startswith("www."):
        text = text[4:]
    return text.rstrip("/")


def _max_timestamp(*values: str | None) -> str | None:
    best_value = None
    best_dt = None
    for value in values:
        parsed = _parse_datetime(value)
        if parsed is None:
            continue
        if best_dt is None or parsed > best_dt:
            best_dt = parsed
            best_value = value
    return best_value


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"
