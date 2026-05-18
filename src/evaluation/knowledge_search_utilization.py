"""Report utilization of retrieved knowledge search results."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_LOW_UTILIZATION_RATE = 0.5
DEFAULT_TOP_RANK = 3


def build_knowledge_search_utilization_report(
    generation_rows: Iterable[Mapping[str, Any]],
    retrieval_rows: Iterable[Mapping[str, Any]],
    used_rows: Iterable[Mapping[str, Any]],
    *,
    low_utilization_rate: float = DEFAULT_LOW_UTILIZATION_RATE,
    top_rank: int = DEFAULT_TOP_RANK,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare retrieved knowledge results to sources used by generated content."""
    if not 0 <= low_utilization_rate <= 1:
        raise ValueError("low_utilization_rate must be between 0 and 1")
    if top_rank <= 0:
        raise ValueError("top_rank must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    retrievals = _retrieval_index(retrieval_rows)
    used = _used_index(used_rows)
    rows = [
        _generation_row(row, retrievals=retrievals, used=used, low_utilization_rate=low_utilization_rate, top_rank=top_rank)
        for row in generation_rows
    ]
    rows.sort(key=_sort_key)
    return {
        "artifact_type": "knowledge_search_utilization",
        "generated_at": generated_at.isoformat(),
        "filters": {"low_utilization_rate": low_utilization_rate, "top_rank": top_rank},
        "summary": {
            "generation_count": len(rows),
            "no_retrieval_count": sum(1 for row in rows if row["utilization_bucket"] == "no_retrievals"),
            "low_utilization_count": sum(1 for row in rows if row["utilization_bucket"] == "low_utilization"),
            "partial_utilization_count": sum(1 for row in rows if row["utilization_bucket"] == "partial_utilization"),
            "high_utilization_count": sum(1 for row in rows if row["utilization_bucket"] == "high_utilization"),
        },
        "rows": rows,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_knowledge_search_utilization_report_from_db(
    db_or_conn: Any,
    *,
    low_utilization_rate: float = DEFAULT_LOW_UTILIZATION_RATE,
    top_rank: int = DEFAULT_TOP_RANK,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load generation, retrieval, and source-use rows from SQLite."""
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    return build_knowledge_search_utilization_report(
        _load_generations(conn, schema),
        _load_retrievals(conn, schema),
        _load_used_sources(conn, schema),
        low_utilization_rate=low_utilization_rate,
        top_rank=top_rank,
        now=now,
        schema_gaps=gaps,
    )


def format_knowledge_search_utilization_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_search_utilization_table(report: dict[str, Any]) -> str:
    lines = [
        "Knowledge Search Utilization",
        f"Generated: {report['generated_at']}",
        (
            "Totals: "
            f"generations={report['summary']['generation_count']} "
            f"no_retrievals={report['summary']['no_retrieval_count']} "
            f"low={report['summary']['low_utilization_count']} "
            f"partial={report['summary']['partial_utilization_count']} "
            f"high={report['summary']['high_utilization_count']}"
        ),
    ]
    if not report["rows"]:
        lines.extend(["", "No generated content rows found."])
        return "\n".join(lines)
    lines.extend(["", "content  type          retrieved  used  rate   bucket               unused_top"])
    for row in report["rows"]:
        lines.append(
            f"{row['generation_id']:<8} "
            f"{(row['content_type'] or '-')[:13]:<13} "
            f"{row['retrieved_source_count']:<10} "
            f"{row['used_source_count']:<5} "
            f"{row['utilization_rate']:<6.2f} "
            f"{row['utilization_bucket']:<20} "
            f"{','.join(row['unused_top_result_ids']) or '-'}"
        )
    return "\n".join(lines)


def _generation_row(
    row: Mapping[str, Any],
    *,
    retrievals: dict[str, list[dict[str, Any]]],
    used: dict[str, set[str]],
    low_utilization_rate: float,
    top_rank: int,
) -> dict[str, Any]:
    data = _row_dict(row)
    generation_id = str(_first(data, "generation_id", "content_id", "id"))
    retrieved = retrievals.get(generation_id, [])
    retrieved_ids = {item["source_id"] for item in retrieved}
    used_ids = used.get(generation_id, set())
    used_retrieved = retrieved_ids & used_ids
    rate = round(len(used_retrieved) / len(retrieved_ids), 4) if retrieved_ids else 0.0
    unused_top = [
        item["source_id"]
        for item in sorted(retrieved, key=lambda item: item["rank"])
        if item["rank"] <= top_rank and item["source_id"] not in used_ids
    ]
    bucket = _bucket(len(retrieved_ids), rate, low_utilization_rate)
    return {
        "generation_id": _int_or_text(generation_id),
        "content_type": _text(_first(data, "content_type", "generation_type")),
        "created_at": _text(_first(data, "created_at", "generated_at")),
        "retrieved_source_count": len(retrieved_ids),
        "used_source_count": len(used_retrieved),
        "utilization_rate": rate,
        "utilization_bucket": bucket,
        "unused_top_result_ids": sorted(dict.fromkeys(unused_top)),
    }


def _load_generations(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    columns = schema["generated_content"]
    selected = ["id AS generation_id"]
    for column in ("content_type", "created_at"):
        selected.append(column if column in columns else f"NULL AS {column}")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content ORDER BY id ASC").fetchall()]


def _load_retrievals(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("knowledge_search_results", "knowledge_search_events", "generation_knowledge_retrievals"):
        if table not in schema:
            continue
        columns = schema[table]
        generation = _first_existing(columns, "generation_id", "content_id", "generated_content_id")
        source = _first_existing(columns, "knowledge_id", "source_id", "result_id")
        if generation is None or source is None:
            continue
        rank = _first_existing(columns, "rank", "result_rank", "position")
        rank_expr = rank if rank else "1"
        return [
            dict(row)
            for row in conn.execute(
                f"""SELECT {generation} AS generation_id, {source} AS source_id, {rank_expr} AS rank
                    FROM {table}
                    ORDER BY {generation} ASC, {rank_expr} ASC"""
            ).fetchall()
        ]
    return []


def _load_used_sources(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if "content_knowledge_links" in schema:
        columns = schema["content_knowledge_links"]
        if {"content_id", "knowledge_id"}.issubset(columns):
            rows.extend(dict(row) for row in conn.execute("SELECT content_id AS generation_id, knowledge_id AS source_id FROM content_knowledge_links").fetchall())
    if "generated_content" in schema:
        columns = schema["generated_content"]
        source_columns = [column for column in ("source_knowledge_ids", "cited_knowledge_ids", "attached_knowledge_ids") if column in columns]
        if source_columns:
            selected = ", ".join(["id", *source_columns])
            for row in conn.execute(f"SELECT {selected} FROM generated_content").fetchall():
                data = dict(row)
                for column in source_columns:
                    for source_id in _json_list(data.get(column)):
                        rows.append({"generation_id": data["id"], "source_id": source_id})
    return rows


def _retrieval_index(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[str, str]] = set()
    for row in rows:
        data = _row_dict(row)
        generation_id = _text(_first(data, "generation_id", "content_id", "generated_content_id"))
        source_id = _text(_first(data, "source_id", "knowledge_id", "result_id"))
        if generation_id is None or source_id is None or (generation_id, source_id) in seen:
            continue
        seen.add((generation_id, source_id))
        index[generation_id].append({"source_id": source_id, "rank": _int_or_none(_first(data, "rank", "result_rank", "position")) or 999999})
    return index


def _used_index(rows: Iterable[Mapping[str, Any]]) -> dict[str, set[str]]:
    index: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        data = _row_dict(row)
        generation_id = _text(_first(data, "generation_id", "content_id", "generated_content_id"))
        source_id = _text(_first(data, "source_id", "knowledge_id"))
        if generation_id is not None and source_id is not None:
            index[generation_id].add(source_id)
    return index


def _bucket(retrieved_count: int, rate: float, threshold: float) -> str:
    if retrieved_count == 0:
        return "no_retrievals"
    if rate < threshold:
        return "low_utilization"
    if rate < 1:
        return "partial_utilization"
    return "high_utilization"


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    missing = []
    if "generated_content" not in schema:
        missing.append("generated_content")
    if not any(table in schema for table in ("knowledge_search_results", "knowledge_search_events", "generation_knowledge_retrievals")):
        missing.append("knowledge_search_results")
    return {"missing_tables": missing, "missing_columns": {}}


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _row_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return dict(row)


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _first_existing(columns: set[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _json_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_or_text(value: str) -> int | str:
    parsed = _int_or_none(value)
    return parsed if parsed is not None else value


def _sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    bucket_rank = {"no_retrievals": 0, "low_utilization": 1, "partial_utilization": 2, "high_utilization": 3}
    return (bucket_rank.get(row["utilization_bucket"], 9), row["utilization_rate"], row["generation_id"])
