"""Report embedding coverage for stored knowledge rows."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
from typing import Any


@dataclass(frozen=True)
class KnowledgeEmbeddingCoverageBucket:
    """Embedding coverage counts for a source grouping."""

    source_type: str
    total_count: int
    embedded_count: int
    missing_count: int
    stale_count: int
    source_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class KnowledgeEmbeddingCoverageReport:
    """Read-only coverage report for knowledge embeddings."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    by_source_type: tuple[KnowledgeEmbeddingCoverageBucket, ...]
    by_source: tuple[KnowledgeEmbeddingCoverageBucket, ...]
    samples: dict[str, list[int]]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "knowledge_embedding_coverage",
            "by_source": [bucket.to_dict() for bucket in self.by_source],
            "by_source_type": [bucket.to_dict() for bucket in self.by_source_type],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "samples": {
                key: list(value) for key, value in sorted(self.samples.items())
            },
            "totals": dict(sorted(self.totals.items())),
        }


def build_knowledge_embedding_coverage_report(
    db_or_conn: Any,
    *,
    limit: int | None = None,
    now: datetime | None = None,
) -> KnowledgeEmbeddingCoverageReport:
    """Summarize missing and stale embeddings without mutating the database."""
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    missing_tables = tuple(table for table in ("knowledge",) if table not in schema)
    missing_columns = _missing_columns(schema)

    if missing_tables:
        return _empty_report(
            generated_at=generated_at,
            limit=limit,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_rows(conn, schema["knowledge"])
    for row in rows:
        row["missing_embedding"] = not bool(row.get("embedding"))
        row["stale_embedding"] = (
            not row["missing_embedding"] and _has_stale_embedding_metadata(row)
        )

    return KnowledgeEmbeddingCoverageReport(
        generated_at=generated_at.isoformat(),
        filters={"limit": limit},
        totals={
            "total_knowledge_items": len(rows),
            "embedded_count": sum(1 for row in rows if not row["missing_embedding"]),
            "missing_count": sum(1 for row in rows if row["missing_embedding"]),
            "stale_count": sum(1 for row in rows if row["stale_embedding"]),
        },
        by_source_type=tuple(_build_buckets(rows, include_source_id=False)),
        by_source=tuple(_build_buckets(rows, include_source_id=True)),
        samples=_samples(rows, limit),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_knowledge_embedding_coverage_json(
    report: KnowledgeEmbeddingCoverageReport,
) -> str:
    """Serialize an embedding coverage report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_knowledge_embedding_coverage_text(
    report: KnowledgeEmbeddingCoverageReport,
) -> str:
    """Render a compact human-readable embedding coverage report."""
    totals = report.totals
    lines = [
        "Knowledge Embedding Coverage",
        f"Generated: {report.generated_at}",
        (
            "Totals: "
            f"{totals['total_knowledge_items']} knowledge rows, "
            f"{totals['embedded_count']} embedded, "
            f"{totals['missing_count']} missing, "
            f"{totals['stale_count']} stale"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    missing = [
        f"{table}({', '.join(columns)})"
        for table, columns in report.missing_columns.items()
        if columns
    ]
    if missing:
        lines.append(f"Missing optional columns: {'; '.join(missing)}")
    if report.samples["missing_item_ids"]:
        lines.append(f"Sample missing IDs: {report.samples['missing_item_ids']}")
    if report.samples["stale_item_ids"]:
        lines.append(f"Sample stale IDs: {report.samples['stale_item_ids']}")
    lines.append("")

    if not report.by_source_type:
        lines.append("No knowledge rows found.")
        return "\n".join(lines)

    lines.append("By source type:")
    for bucket in report.by_source_type:
        lines.append(
            "  - {source_type}: total={total_count} embedded={embedded_count} "
            "missing={missing_count} stale={stale_count}".format(**bucket.to_dict())
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    names = [_row_get(row, 0) for row in rows]
    return {
        name: {column[1] for column in conn.execute(f"PRAGMA table_info({name})")}
        for name in names
    }


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "knowledge": (
            "id",
            "source_type",
            "source_id",
            "content",
            "insight",
            "embedding",
            "metadata",
        ),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id", "id"),
        _column_expr(columns, "source_type", "source_type"),
        _column_expr(columns, "source_id", "source_id"),
        _column_expr(columns, "content", "content"),
        _column_expr(columns, "insight", "insight"),
        _column_expr(columns, "embedding", "embedding"),
        _column_expr(columns, "metadata", "metadata"),
    ]
    cursor = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM knowledge
            ORDER BY id ASC"""
    )
    return [_row_to_dict(row) for row in cursor.fetchall()]


def _column_expr(columns: set[str], column: str, output: str) -> str:
    if column in columns:
        return f"{column} AS {output}"
    return f"NULL AS {output}"


def _build_buckets(
    rows: list[dict[str, Any]],
    *,
    include_source_id: bool,
) -> list[KnowledgeEmbeddingCoverageBucket]:
    grouped: dict[tuple[str, str | None], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            _clean(row.get("source_type")) or "(unknown source type)",
            _clean(row.get("source_id")) if include_source_id else None,
        )
        grouped.setdefault(key, []).append(row)

    buckets = [
        KnowledgeEmbeddingCoverageBucket(
            source_type=source_type,
            source_id=source_id,
            total_count=len(group_rows),
            embedded_count=sum(1 for row in group_rows if not row["missing_embedding"]),
            missing_count=sum(1 for row in group_rows if row["missing_embedding"]),
            stale_count=sum(1 for row in group_rows if row["stale_embedding"]),
        )
        for (source_type, source_id), group_rows in grouped.items()
    ]
    buckets.sort(
        key=lambda bucket: (
            -bucket.missing_count,
            -bucket.stale_count,
            -bucket.total_count,
            bucket.source_type,
            bucket.source_id or "",
        )
    )
    return buckets


def _samples(rows: list[dict[str, Any]], limit: int | None) -> dict[str, list[int]]:
    if limit is None or limit == 0:
        return {"missing_item_ids": [], "stale_item_ids": []}
    return {
        "missing_item_ids": [
            int(row["id"])
            for row in rows
            if row.get("id") is not None and row["missing_embedding"]
        ][:limit],
        "stale_item_ids": [
            int(row["id"])
            for row in rows
            if row.get("id") is not None and row["stale_embedding"]
        ][:limit],
    }


def _has_stale_embedding_metadata(row: dict[str, Any]) -> bool:
    metadata = _decode_metadata(row.get("metadata"))
    if not metadata:
        return False

    current_text = str(row.get("insight") or row.get("content") or "")
    current_content = str(row.get("content") or "")
    current_insight = str(row.get("insight") or "")
    candidates = _hash_candidates(metadata)
    if not candidates and isinstance(metadata.get("embedding"), dict):
        candidates = _hash_candidates(metadata["embedding"])

    for key, stored in candidates:
        if stored is None:
            continue
        expected = _hash_target_for_key(
            key,
            current_text=current_text,
            current_content=current_content,
            current_insight=current_insight,
        )
        if _normalize_hash(stored) != _sha256(expected):
            return True
    return False


def _hash_candidates(metadata: dict[str, Any]) -> list[tuple[str, Any]]:
    keys = (
        "embedding_text_hash",
        "embedding_text_sha256",
        "knowledge_embedding_text_hash",
        "text_hash",
        "text_sha256",
        "source_text_hash",
        "source_text_sha256",
        "content_hash",
        "content_sha256",
        "insight_hash",
        "insight_sha256",
    )
    return [(key, metadata.get(key)) for key in keys if key in metadata]


def _hash_target_for_key(
    key: str,
    *,
    current_text: str,
    current_content: str,
    current_insight: str,
) -> str:
    if key.startswith("content_"):
        return current_content
    if key.startswith("insight_"):
        return current_insight
    return current_text


def _decode_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalize_hash(value: Any) -> str:
    return str(value or "").strip().lower()


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _empty_report(
    *,
    generated_at: datetime,
    limit: int | None,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> KnowledgeEmbeddingCoverageReport:
    return KnowledgeEmbeddingCoverageReport(
        generated_at=generated_at.isoformat(),
        filters={"limit": limit},
        totals={
            "total_knowledge_items": 0,
            "embedded_count": 0,
            "missing_count": 0,
            "stale_count": 0,
        },
        by_source_type=(),
        by_source=(),
        samples={"missing_item_ids": [], "stale_item_ids": []},
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return {
        "id": row[0],
        "source_type": row[1],
        "source_id": row[2],
        "content": row[3],
        "insight": row[4],
        "embedding": row[5],
        "metadata": row[6],
    }


def _row_get(row: Any, index: int) -> Any:
    return row[index]


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
