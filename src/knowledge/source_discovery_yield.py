"""Measure curated source review outcomes by discovery channel."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from io import StringIO
import json
import sqlite3
from typing import Any


UNKNOWN_DISCOVERY_SOURCE = "unknown"

CANDIDATE_STATUSES = frozenset(
    {
        "candidate",
        "discovered",
        "needs_review",
        "pending",
        "pending_review",
        "review",
        "unreviewed",
    }
)
ACTIVE_STATUSES = frozenset({"active", "approved"})
REJECTED_STATUSES = frozenset({"rejected", "inactive", "retired"})
PAUSED_STATUSES = frozenset({"paused", "quarantined"})
REVIEWED_STATUSES = ACTIVE_STATUSES | REJECTED_STATUSES | PAUSED_STATUSES

CSV_FIELDS = (
    "discovery_source",
    "source_type",
    "total_count",
    "candidate_count",
    "active_count",
    "rejected_count",
    "paused_count",
    "reviewed_count",
    "average_relevance_score",
    "average_sample_count",
    "conversion_rate",
)


@dataclass(frozen=True)
class SourceDiscoveryYieldRow:
    """One discovery-source/source-type review outcome aggregate."""

    discovery_source: str
    source_type: str
    total_count: int
    candidate_count: int
    active_count: int
    rejected_count: int
    paused_count: int
    reviewed_count: int
    average_relevance_score: float | None
    average_sample_count: float
    conversion_rate: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceDiscoveryYieldReport:
    """Read-only curated source discovery yield report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[SourceDiscoveryYieldRow, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "source_discovery_yield",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(self.totals),
        }


def build_source_discovery_yield_report(
    db_or_conn: Any,
    *,
    source_type: str | None = None,
    discovery_source: str | None = None,
    min_samples: int = 0,
    now: datetime | None = None,
) -> SourceDiscoveryYieldReport:
    """Return curated source review outcome aggregates by discovery channel."""
    if min_samples < 0:
        raise ValueError("min_samples must be non-negative")

    source_type_filter = _clean_optional(source_type)
    discovery_source_filter = _normalize_discovery_source(discovery_source)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "discovery_source": discovery_source_filter,
        "min_samples": min_samples,
        "source_type": source_type_filter,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    raw_rows = _load_rows(
        conn,
        source_type=source_type_filter,
        discovery_source=discovery_source_filter,
        min_samples=min_samples,
    )
    rows = tuple(_aggregate_row(row) for row in raw_rows)
    return SourceDiscoveryYieldReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(rows),
        rows=rows,
        missing_tables=(),
        missing_columns={},
    )


def format_source_discovery_yield_json(report: SourceDiscoveryYieldReport) -> str:
    """Serialize a discovery yield report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_discovery_yield_csv(report: SourceDiscoveryYieldReport) -> str:
    """Render discovery yield rows as stable CSV."""
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for row in report.rows:
        payload = row.to_dict()
        writer.writerow({field: _csv_value(payload[field]) for field in CSV_FIELDS})
    return output.getvalue().rstrip("\n")


def _load_rows(
    conn: sqlite3.Connection,
    *,
    source_type: str | None,
    discovery_source: str | None,
    min_samples: int,
) -> list[dict[str, Any]]:
    where = ["COALESCE(sample_count, 0) >= ?"]
    params: list[Any] = [min_samples]
    if source_type is not None:
        where.append("source_type = ?")
        params.append(source_type)
    if discovery_source is not None:
        if discovery_source == UNKNOWN_DISCOVERY_SOURCE:
            where.append("(discovery_source IS NULL OR TRIM(discovery_source) = '')")
        else:
            where.append("discovery_source = ?")
            params.append(discovery_source)

    status = "LOWER(COALESCE(status, 'candidate'))"
    rows = conn.execute(
        f"""SELECT
                COALESCE(NULLIF(TRIM(discovery_source), ''), ?) AS discovery_source,
                COALESCE(NULLIF(TRIM(source_type), ''), 'unknown') AS source_type,
                COUNT(*) AS total_count,
                SUM(CASE WHEN {status} IN ({_placeholders(CANDIDATE_STATUSES)}) THEN 1 ELSE 0 END)
                    AS candidate_count,
                SUM(CASE WHEN {status} IN ({_placeholders(ACTIVE_STATUSES)}) THEN 1 ELSE 0 END)
                    AS active_count,
                SUM(CASE WHEN {status} IN ({_placeholders(REJECTED_STATUSES)}) THEN 1 ELSE 0 END)
                    AS rejected_count,
                SUM(CASE WHEN {status} IN ({_placeholders(PAUSED_STATUSES)}) THEN 1 ELSE 0 END)
                    AS paused_count,
                AVG(relevance_score) AS average_relevance_score,
                AVG(COALESCE(sample_count, 0)) AS average_sample_count
            FROM curated_sources
            WHERE {' AND '.join(where)}
            GROUP BY
                COALESCE(NULLIF(TRIM(discovery_source), ''), ?),
                COALESCE(NULLIF(TRIM(source_type), ''), 'unknown')
            ORDER BY discovery_source ASC, source_type ASC""",
        (
            UNKNOWN_DISCOVERY_SOURCE,
            *sorted(CANDIDATE_STATUSES),
            *sorted(ACTIVE_STATUSES),
            *sorted(REJECTED_STATUSES),
            *sorted(PAUSED_STATUSES),
            *params,
            UNKNOWN_DISCOVERY_SOURCE,
        ),
    ).fetchall()
    return [dict(row) for row in rows]


def _aggregate_row(row: dict[str, Any]) -> SourceDiscoveryYieldRow:
    candidate_count = _int(row.get("candidate_count"))
    active_count = _int(row.get("active_count"))
    rejected_count = _int(row.get("rejected_count"))
    paused_count = _int(row.get("paused_count"))
    reviewed_count = active_count + rejected_count + paused_count
    denominator = candidate_count + reviewed_count
    return SourceDiscoveryYieldRow(
        discovery_source=str(row.get("discovery_source") or UNKNOWN_DISCOVERY_SOURCE),
        source_type=str(row.get("source_type") or "unknown"),
        total_count=_int(row.get("total_count")),
        candidate_count=candidate_count,
        active_count=active_count,
        rejected_count=rejected_count,
        paused_count=paused_count,
        reviewed_count=reviewed_count,
        average_relevance_score=_round_or_none(row.get("average_relevance_score")),
        average_sample_count=_round(row.get("average_sample_count")),
        conversion_rate=_round(active_count / denominator if denominator else 0.0),
    )


def _totals(rows: tuple[SourceDiscoveryYieldRow, ...]) -> dict[str, Any]:
    total_count = sum(row.total_count for row in rows)
    candidate_count = sum(row.candidate_count for row in rows)
    active_count = sum(row.active_count for row in rows)
    rejected_count = sum(row.rejected_count for row in rows)
    paused_count = sum(row.paused_count for row in rows)
    reviewed_count = sum(row.reviewed_count for row in rows)
    denominator = candidate_count + reviewed_count
    return {
        "active_count": active_count,
        "candidate_count": candidate_count,
        "conversion_rate": _round(active_count / denominator if denominator else 0.0),
        "group_count": len(rows),
        "paused_count": paused_count,
        "rejected_count": rejected_count,
        "reviewed_count": reviewed_count,
        "total_count": total_count,
    }


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "curated_sources": (
            "discovery_source",
            "relevance_score",
            "sample_count",
            "source_type",
            "status",
        )
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in required.items()
        if table in schema
        and any(column not in schema.get(table, set()) for column in columns)
    }
    return missing_tables, missing_columns


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> SourceDiscoveryYieldReport:
    return SourceDiscoveryYieldReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(()),
        rows=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3.Connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        name = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(name)] = {
            str(column[1])
            for column in conn.execute(f"PRAGMA table_info({name})").fetchall()
        }
    return schema


def _placeholders(values: frozenset[str]) -> str:
    return ", ".join("?" for _ in values)


def _normalize_discovery_source(value: str | None) -> str | None:
    cleaned = _clean_optional(value)
    return UNKNOWN_DISCOVERY_SOURCE if cleaned == UNKNOWN_DISCOVERY_SOURCE else cleaned


def _clean_optional(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int(value: Any) -> int:
    return int(value or 0)


def _round(value: Any) -> float:
    return round(float(value or 0.0), 4)


def _round_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return _round(value)


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    return value
