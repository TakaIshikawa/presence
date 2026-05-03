"""Report source-author coverage across curated knowledge entries."""

from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 180
DEFAULT_MIN_ENTRIES = 2
DEFAULT_DOMINANCE_THRESHOLD = 0.5
DEFAULT_RECENT_DAYS = 45
MISSING_AUTHOR_LABEL = "(missing author)"
UNKNOWN_VALUES = {"", "unknown", "n/a", "none", "null", "-"}
HANDLE_PREFIX_RE = re.compile(r"^(?:@|https?://(?:www\.)?(?:x|twitter)\.com/)+", re.I)


@dataclass(frozen=True)
class KnowledgeAuthorCoverageRow:
    """One normalized author bucket in the coverage report."""

    author: str
    display_author: str
    status: str
    entry_count: int
    recent_entry_count: int
    share: float
    first_seen_at: str | None
    last_seen_at: str | None
    knowledge_ids: tuple[int, ...]
    source_types: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["knowledge_ids"] = list(self.knowledge_ids)
        payload["source_types"] = list(self.source_types)
        return payload


@dataclass(frozen=True)
class KnowledgeAuthorCoverageReport:
    """Author coverage summary for curated knowledge rows."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[KnowledgeAuthorCoverageRow, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "knowledge_author_coverage",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def normalize_author(value: Any) -> str | None:
    """Normalize author handles/names for deterministic grouping."""
    text = str(value or "").strip()
    if not text:
        return None
    text = HANDLE_PREFIX_RE.sub("", text).strip()
    text = text.split("?", 1)[0].split("#", 1)[0].strip().strip("/")
    normalized = " ".join(text.casefold().split())
    return None if normalized in UNKNOWN_VALUES else normalized


def build_knowledge_author_coverage_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_entries: int = DEFAULT_MIN_ENTRIES,
    dominance_threshold: float = DEFAULT_DOMINANCE_THRESHOLD,
    recent_days: int = DEFAULT_RECENT_DAYS,
    now: datetime | None = None,
) -> KnowledgeAuthorCoverageReport:
    """Build a read-only author coverage report from SQLite or row mappings."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_entries <= 0:
        raise ValueError("min_entries must be positive")
    if not 0 <= dominance_threshold <= 1:
        raise ValueError("dominance_threshold must be between 0 and 1")
    if recent_days <= 0:
        raise ValueError("recent_days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    recent_cutoff = generated_at - timedelta(days=recent_days)

    if _is_sqlite_source(db_or_rows):
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        missing_tables = tuple(table for table in ("knowledge",) if table not in schema)
        missing_columns = _missing_columns(schema)
        if missing_tables:
            return _empty_report(
                generated_at,
                days,
                min_entries,
                dominance_threshold,
                recent_days,
                missing_tables,
                missing_columns,
            )
        rows = _load_knowledge_rows(conn, schema, cutoff)
    else:
        missing_tables = ()
        missing_columns = {}
        rows = [dict(row) for row in db_or_rows]
        rows = [
            row for row in rows
            if _timestamp_in_window(row.get("effective_at") or row.get("published_at")
                                    or row.get("ingested_at") or row.get("created_at"), cutoff)
        ]

    coverage_rows = _coverage_rows(
        rows,
        min_entries=min_entries,
        dominance_threshold=dominance_threshold,
        recent_cutoff=recent_cutoff,
    )
    totals = _totals(rows, coverage_rows)

    return KnowledgeAuthorCoverageReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "min_entries": min_entries,
            "dominance_threshold": dominance_threshold,
            "recent_days": recent_days,
        },
        totals=totals,
        rows=tuple(coverage_rows),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_knowledge_author_coverage_json(report: KnowledgeAuthorCoverageReport) -> str:
    """Serialize an author coverage report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_knowledge_author_coverage_csv(report: KnowledgeAuthorCoverageReport) -> str:
    """Serialize author coverage rows as CSV."""
    output = io.StringIO()
    fieldnames = [
        "author",
        "display_author",
        "status",
        "entry_count",
        "recent_entry_count",
        "share",
        "first_seen_at",
        "last_seen_at",
        "source_types",
        "knowledge_ids",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in report.rows:
        writer.writerow(
            {
                "author": row.author,
                "display_author": row.display_author,
                "status": row.status,
                "entry_count": row.entry_count,
                "recent_entry_count": row.recent_entry_count,
                "share": f"{row.share:.3f}",
                "first_seen_at": row.first_seen_at or "",
                "last_seen_at": row.last_seen_at or "",
                "source_types": ";".join(row.source_types),
                "knowledge_ids": ";".join(str(value) for value in row.knowledge_ids),
            }
        )
    return output.getvalue().rstrip("\n")


def _coverage_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    min_entries: int,
    dominance_threshold: float,
    recent_cutoff: datetime,
) -> list[KnowledgeAuthorCoverageRow]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    display_values: dict[str, set[str]] = {}
    for row in rows:
        normalized = normalize_author(row.get("author")) or MISSING_AUTHOR_LABEL
        grouped.setdefault(normalized, []).append(row)
        raw_author = str(row.get("author") or "").strip()
        if raw_author:
            display_values.setdefault(normalized, set()).add(raw_author)

    total_entries = sum(len(values) for values in grouped.values())
    coverage_rows: list[KnowledgeAuthorCoverageRow] = []
    for author, author_rows in grouped.items():
        entry_count = len(author_rows)
        dated_rows = [
            (row, _parse_timestamp(_row_timestamp(row)))
            for row in author_rows
        ]
        timestamps = sorted(
            timestamp.isoformat()
            for _row, timestamp in dated_rows
            if timestamp is not None
        )
        recent_entry_count = sum(
            1
            for _row, timestamp in dated_rows
            if timestamp is not None and timestamp >= recent_cutoff
        )
        share = round(entry_count / total_entries, 3) if total_entries else 0.0
        status = _status(
            author=author,
            entry_count=entry_count,
            recent_entry_count=recent_entry_count,
            share=share,
            min_entries=min_entries,
            dominance_threshold=dominance_threshold,
        )
        ids = tuple(
            sorted(
                value
                for value in (
                    _int_or_none(row.get("id") or row.get("knowledge_id"))
                    for row in author_rows
                )
                if value is not None
            )
        )
        source_types = tuple(
            sorted(
                {
                    source_type
                    for source_type in (_clean(row.get("source_type")) for row in author_rows)
                    if source_type
                }
            )
        )
        coverage_rows.append(
            KnowledgeAuthorCoverageRow(
                author=author,
                display_author=_display_author(author, display_values.get(author, set())),
                status=status,
                entry_count=entry_count,
                recent_entry_count=recent_entry_count,
                share=share,
                first_seen_at=timestamps[0] if timestamps else None,
                last_seen_at=timestamps[-1] if timestamps else None,
                knowledge_ids=ids,
                source_types=source_types,
            )
        )

    coverage_rows.sort(
        key=lambda row: (
            _status_rank(row.status),
            -row.share,
            -row.entry_count,
            row.author,
        )
    )
    return coverage_rows


def _status(
    *,
    author: str,
    entry_count: int,
    recent_entry_count: int,
    share: float,
    min_entries: int,
    dominance_threshold: float,
) -> str:
    if author == MISSING_AUTHOR_LABEL or entry_count < min_entries:
        return "underrepresented"
    if share >= dominance_threshold:
        return "dominant"
    if recent_entry_count == 0:
        return "inactive"
    return "healthy"


def _totals(
    source_rows: list[Mapping[str, Any]],
    coverage_rows: list[KnowledgeAuthorCoverageRow],
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    for row in coverage_rows:
        status_counts[row.status] = status_counts.get(row.status, 0) + 1
    return {
        "author_count": len(coverage_rows),
        "entry_count": len(source_rows),
        "missing_author_entry_count": sum(
            row.entry_count
            for row in coverage_rows
            if row.author == MISSING_AUTHOR_LABEL
        ),
        "recent_entry_count": sum(row.recent_entry_count for row in coverage_rows),
        "status_counts": dict(sorted(status_counts.items())),
    }


def _load_knowledge_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema.get("knowledge", set())
    timestamp_expr = _knowledge_timestamp_expr(columns)
    where: list[str] = []
    params: list[Any] = []
    if timestamp_expr != "NULL":
        where.append(f"{timestamp_expr} >= ?")
        params.append(cutoff.isoformat())
    if "approved" in columns:
        where.append("COALESCE(approved, 0) = 1")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    cursor = conn.execute(
        f"""SELECT {_column_expr(columns, "id", "id")},
                  {_column_expr(columns, "source_type", "source_type")},
                  {_column_expr(columns, "author", "author")},
                  {timestamp_expr} AS effective_at
           FROM knowledge
           {where_sql}
           ORDER BY effective_at DESC, id ASC""",
        params,
    )
    return [dict(row) for row in cursor.fetchall()]


def _knowledge_timestamp_expr(columns: set[str]) -> str:
    parts = [
        column
        for column in ("published_at", "ingested_at", "created_at")
        if column in columns
    ]
    if not parts:
        return "NULL"
    if len(parts) == 1:
        return parts[0]
    return f"COALESCE({', '.join(parts)})"


def _column_expr(columns: set[str], column: str, output: str) -> str:
    if column in columns:
        return f"{column} AS {output}"
    return f"NULL AS {output}"


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "knowledge": (
            "id",
            "source_type",
            "author",
            "published_at",
            "ingested_at",
            "created_at",
            "approved",
        )
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _empty_report(
    generated_at: datetime,
    days: int,
    min_entries: int,
    dominance_threshold: float,
    recent_days: int,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> KnowledgeAuthorCoverageReport:
    return KnowledgeAuthorCoverageReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "min_entries": min_entries,
            "dominance_threshold": dominance_threshold,
            "recent_days": recent_days,
        },
        totals={
            "author_count": 0,
            "entry_count": 0,
            "missing_author_entry_count": 0,
            "recent_entry_count": 0,
            "status_counts": {},
        },
        rows=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _is_sqlite_source(value: Any) -> bool:
    return isinstance(value, sqlite3.Connection) or hasattr(value, "conn")


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    names = [row[0] for row in rows]
    return {
        name: {column[1] for column in conn.execute(f"PRAGMA table_info({name})")}
        for name in names
    }


def _row_timestamp(row: Mapping[str, Any]) -> Any:
    return (
        row.get("effective_at")
        or row.get("published_at")
        or row.get("ingested_at")
        or row.get("created_at")
    )


def _timestamp_in_window(value: Any, cutoff: datetime) -> bool:
    timestamp = _parse_timestamp(value)
    return True if timestamp is None else timestamp >= cutoff


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _display_author(author: str, values: set[str]) -> str:
    if author == MISSING_AUTHOR_LABEL:
        return MISSING_AUTHOR_LABEL
    if not values:
        return author
    return sorted(values, key=lambda value: (len(value), value.casefold()))[0]


def _status_rank(status: str) -> int:
    return {
        "dominant": 0,
        "underrepresented": 1,
        "inactive": 2,
        "healthy": 3,
    }.get(status, 99)


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
