"""Report unpublished generated content waiting on curation."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
REPRESENTATIVE_LIMIT = 5


@dataclass(frozen=True)
class GeneratedContentCurationBottleneck:
    """One grouped curation bottleneck."""

    curation_state: str
    eval_score_band: str
    content_type: str
    age_bucket: str
    count: int
    representative_content_ids: tuple[int, ...]
    oldest_created_at: str | None
    newest_created_at: str | None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["representative_content_ids"] = list(self.representative_content_ids)
        return payload


@dataclass(frozen=True)
class GeneratedContentCurationBottlenecksReport:
    """Generated content curation bottleneck report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    age_buckets: dict[str, int]
    bottlenecks: tuple[GeneratedContentCurationBottleneck, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_bottlenecks(self) -> bool:
        return bool(self.bottlenecks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "generated_content_curation_bottlenecks",
            "age_buckets": dict(self.age_buckets),
            "bottlenecks": [group.to_dict() for group in self.bottlenecks],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_bottlenecks": self.has_bottlenecks,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_generated_content_curation_bottlenecks_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> GeneratedContentCurationBottlenecksReport:
    """Build a read-only report for unpublished content awaiting curation."""
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
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = _load_rows(conn, schema, cutoff=cutoff, limit=limit)
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    age_bucket_counts: Counter[str] = Counter()
    for row in rows:
        age_bucket = _age_bucket(row.get("created_at"), generated_at)
        curation_state = _curation_state(row.get("curation_quality"))
        score_band = _eval_score_band(row.get("eval_score"))
        content_type = _clean(row.get("content_type")) or "unknown"
        row = dict(row)
        row["age_bucket"] = age_bucket
        key = (curation_state, score_band, content_type, age_bucket)
        grouped.setdefault(key, []).append(row)
        age_bucket_counts[age_bucket] += 1

    bottlenecks = [_bottleneck(key, group_rows) for key, group_rows in grouped.items()]
    bottlenecks.sort(
        key=lambda group: (
            -group.count,
            _age_bucket_sort(group.age_bucket),
            group.curation_state,
            group.eval_score_band,
            group.content_type,
        )
    )
    return GeneratedContentCurationBottlenecksReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "rows_scanned": len(rows),
            "bottleneck_group_count": len(bottlenecks),
            "unpublished_content_count": len(rows),
            "curation_state_counts": dict(
                sorted(Counter(_curation_state(row.get("curation_quality")) for row in rows).items())
            ),
            "eval_score_band_counts": dict(
                sorted(Counter(_eval_score_band(row.get("eval_score")) for row in rows).items())
            ),
            "content_type_counts": dict(
                sorted(Counter(_clean(row.get("content_type")) or "unknown" for row in rows).items())
            ),
        },
        age_buckets={bucket: age_bucket_counts.get(bucket, 0) for bucket in _age_bucket_order()},
        bottlenecks=tuple(bottlenecks),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_generated_content_curation_bottlenecks_json(
    report: GeneratedContentCurationBottlenecksReport,
) -> str:
    """Serialize the curation bottleneck report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_generated_content_curation_bottlenecks_text(
    report: GeneratedContentCurationBottlenecksReport,
) -> str:
    """Render generated content curation bottlenecks for command-line review."""
    totals = report.totals
    lines = [
        "Generated Content Curation Bottlenecks",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"limit={report.filters['limit']}"
        ),
        (
            "Totals: "
            f"rows_scanned={totals['rows_scanned']} "
            f"groups={totals['bottleneck_group_count']} "
            f"unpublished={totals['unpublished_content_count']}"
        ),
        "Age buckets: "
        + ", ".join(f"{bucket}={count}" for bucket, count in report.age_buckets.items()),
    ]
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

    if not report.bottlenecks:
        lines.extend(["", "No generated content curation bottlenecks found."])
        return "\n".join(lines)

    lines.extend(["", "Bottlenecks:"])
    for group in report.bottlenecks:
        ids = ", ".join(str(content_id) for content_id in group.representative_content_ids)
        lines.append(
            f"  - curation={group.curation_state} "
            f"score={group.eval_score_band} "
            f"type={group.content_type} "
            f"age={group.age_bucket} "
            f"count={group.count} "
            f"content_ids={ids}"
        )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    limit: int,
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    select_columns = [
        "gc.id AS content_id",
        _column_expr(columns, "content_type", "gc", "content_type"),
        _column_expr(columns, "curation_quality", "gc", "curation_quality"),
        _column_expr(columns, "eval_score", "gc", "eval_score"),
        _column_expr(columns, "created_at", "gc", "created_at"),
    ]
    where_parts = []
    params: list[Any] = []
    if "created_at" in columns:
        where_parts.append("gc.created_at >= ?")
        params.append(cutoff.isoformat())
    if "published" in columns:
        where_parts.append("(gc.published IS NULL OR gc.published NOT IN (1, -1))")
    if "published_at" in columns:
        where_parts.append("gc.published_at IS NULL")
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    order = "gc.created_at ASC, gc.id ASC" if "created_at" in columns else "gc.id ASC"
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM generated_content gc
            {where_sql}
            ORDER BY {order}
            LIMIT ?""",
        (*params, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def _bottleneck(
    key: tuple[str, str, str, str],
    rows: list[dict[str, Any]],
) -> GeneratedContentCurationBottleneck:
    curation_state, score_band, content_type, age_bucket = key
    sorted_rows = sorted(rows, key=lambda row: (_timestamp_sort(row.get("created_at")), int(row["content_id"])))
    created_values = [row.get("created_at") for row in sorted_rows if row.get("created_at")]
    return GeneratedContentCurationBottleneck(
        curation_state=curation_state,
        eval_score_band=score_band,
        content_type=content_type,
        age_bucket=age_bucket,
        count=len(rows),
        representative_content_ids=tuple(
            int(row["content_id"]) for row in sorted_rows[:REPRESENTATIVE_LIMIT]
        ),
        oldest_created_at=created_values[0] if created_values else None,
        newest_created_at=created_values[-1] if created_values else None,
    )


def _curation_state(value: Any) -> str:
    cleaned = _clean(value)
    return cleaned if cleaned else "unreviewed"


def _eval_score_band(value: Any) -> str:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return "unscored"
    if score < 6:
        return "low"
    if score < 8:
        return "medium"
    return "high"


def _age_bucket(value: Any, now: datetime) -> str:
    created_at = _parse_timestamp(value)
    if created_at is None:
        return "unknown"
    age_days = max((now - created_at).total_seconds() / 86400, 0)
    if age_days < 1:
        return "0-1d"
    if age_days < 3:
        return "1-3d"
    if age_days < 7:
        return "3-7d"
    if age_days < 14:
        return "7-14d"
    return "14d+"


def _age_bucket_order() -> tuple[str, ...]:
    return ("0-1d", "1-3d", "3-7d", "7-14d", "14d+", "unknown")


def _age_bucket_sort(bucket: str) -> int:
    try:
        return _age_bucket_order().index(bucket)
    except ValueError:
        return len(_age_bucket_order())


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_timestamp(value)
    return parsed.isoformat() if parsed is not None else ""


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    missing_tables = tuple(table for table in ("generated_content",) if table not in schema)
    required_columns = {
        "generated_content": {
            "id",
            "content_type",
            "curation_quality",
            "eval_score",
            "created_at",
            "published",
        },
    }
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required_columns.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> GeneratedContentCurationBottlenecksReport:
    return GeneratedContentCurationBottlenecksReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "rows_scanned": 0,
            "bottleneck_group_count": 0,
            "unpublished_content_count": 0,
            "curation_state_counts": {},
            "eval_score_band_counts": {},
            "content_type_counts": {},
        },
        age_buckets={bucket: 0 for bucket in _age_bucket_order()},
        bottlenecks=(),
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
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
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


def _clean(value: Any) -> str:
    return " ".join(str(value).split()) if value not in (None, "") else ""


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
