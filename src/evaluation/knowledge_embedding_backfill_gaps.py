"""Find knowledge sources that need embedding backfill."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_EXPECTED_MODEL = "text-embedding-3-small"


def build_knowledge_embedding_backfill_gaps_report(
    source_rows: list[dict[str, Any]],
    embedding_rows: list[dict[str, Any]] | None = None,
    *,
    expected_model: str = DEFAULT_EXPECTED_MODEL,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    embeddings = {_text(row.get("source_id") or row.get("knowledge_id")): row for row in embedding_rows or []}
    findings = []
    for row in source_rows:
        source_id = _text(row.get("id") or row.get("source_id"))
        emb = embeddings.get(source_id)
        metadata = _json_obj(row.get("metadata"))
        reason = None
        model = _text((emb or {}).get("embedding_model") or (emb or {}).get("model"))
        vector = (emb or {}).get("embedding") or (emb or {}).get("vector")
        if metadata.get("embedding_failed") or metadata.get("embedding_error"):
            reason = "failed_embedding"
        elif emb is None:
            reason = "missing"
        elif not _filled_vector(vector):
            reason = "empty"
        elif expected_model and model and model != expected_model:
            reason = "stale_model"
        if reason:
            findings.append(
                {
                    "source_id": source_id,
                    "source_type": _text(row.get("source_type") or row.get("type")) or "knowledge",
                    "title": _text(row.get("title")),
                    "url": _text(row.get("url")),
                    "embedding_model": model or None,
                    "expected_model": expected_model,
                    "updated_at": _text(row.get("updated_at") or row.get("created_at")) or None,
                    "reason_code": reason,
                }
            )
    findings.sort(key=lambda item: (item["reason_code"], item["source_type"], item["source_id"]))
    by_reason = Counter(item["reason_code"] for item in findings)
    by_type = Counter(item["source_type"] for item in findings)
    return {
        "artifact_type": "knowledge_embedding_backfill_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"expected_model": expected_model, "limit": limit},
        "summary": {
            "sources_scanned": len(source_rows),
            "gap_count": len(findings),
            "counts_by_reason": dict(sorted(by_reason.items())),
            "counts_by_source_type": dict(sorted(by_type.items())),
        },
        "findings": findings[:limit],
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_knowledge_embedding_backfill_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    source_rows = _load_sources(conn, schema) if not gaps["missing_tables"] else []
    embedding_rows = _load_embeddings(conn, schema)
    return build_knowledge_embedding_backfill_gaps_report(source_rows, embedding_rows, schema_gaps=gaps, **kwargs)


def format_knowledge_embedding_backfill_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_embedding_backfill_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Knowledge Embedding Backfill Gaps",
        f"Generated: {report['generated_at']}",
        f"Totals: sources={summary['sources_scanned']} gaps={summary['gap_count']}",
    ]
    if not report["findings"]:
        lines.extend(["", "No knowledge embedding backfill gaps found."])
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for item in report["findings"]:
        lines.append(
            f"  - source={item['source_id']} type={item['source_type']} reason={item['reason_code']} model={item['embedding_model'] or '-'} expected={item['expected_model']}"
        )
    return "\n".join(lines)


def _load_sources(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "knowledge_sources" if "knowledge_sources" in schema else "content_sources"
    columns = schema[table]
    select = [
        _select(columns, ("id", "source_id"), "id"),
        _select(columns, ("source_type", "type"), "source_type"),
        _select(columns, ("title", "name"), "title"),
        _select(columns, ("url",), "url"),
        _select(columns, ("metadata",), "metadata"),
        _select(columns, ("updated_at", "created_at"), "updated_at"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _load_embeddings(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "knowledge_embeddings" if "knowledge_embeddings" in schema else "embeddings" if "embeddings" in schema else ""
    if not table:
        return []
    columns = schema[table]
    select = [
        _select(columns, ("source_id", "knowledge_id", "content_source_id"), "source_id"),
        _select(columns, ("embedding", "vector"), "embedding"),
        _select(columns, ("embedding_model", "model"), "embedding_model"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "knowledge_sources" not in schema and "content_sources" not in schema:
        return {"missing_tables": ["knowledge_sources|content_sources"], "missing_columns": {}}
    return {"missing_tables": [], "missing_columns": {}}


def _filled_vector(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, tuple)):
        return bool(value)
    text = str(value).strip()
    return bool(text and text not in {"[]", "{}", "null"})


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
