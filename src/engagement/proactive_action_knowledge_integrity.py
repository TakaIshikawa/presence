"""Audit proactive_actions.knowledge_ids payload integrity."""

from __future__ import annotations

from collections import Counter
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_MIN_RELEVANCE = 0.35


def build_proactive_action_knowledge_integrity_report(
    rows: list[dict[str, Any]],
    *,
    known_knowledge_ids: set[str] | None = None,
    status: str = "all",
    action_type: str = "all",
    min_relevance: float = DEFAULT_MIN_RELEVANCE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if min_relevance < 0:
        raise ValueError("min_relevance must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    known = {str(value) for value in (known_knowledge_ids or set())}
    status_filter = _clean(status).lower() or "all"
    action_filter = _clean(action_type).lower() or "all"
    findings: list[dict[str, Any]] = []
    scanned = 0

    for row in rows:
        row_status = _clean(row.get("status"), "unknown").lower()
        row_type = _clean(row.get("action_type"), "unknown").lower()
        if status_filter != "all" and row_status != status_filter:
            continue
        if action_filter != "all" and row_type != action_filter:
            continue
        scanned += 1
        findings.extend(_findings_for_row(row, known_ids=known, min_relevance=min_relevance))

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": "proactive_action_knowledge_integrity",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "status": status_filter,
            "action_type": action_filter,
            "min_relevance": min_relevance,
            "limit": limit,
        },
        "findings": shown,
        "grouped_counts": _grouped_counts(findings),
        "summary": {
            "rows_scanned": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(Counter(item["issue_type"] for item in findings).items())),
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
    }


def build_proactive_action_knowledge_integrity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    rows = _load_actions(conn, schema, missing_tables, missing_columns)
    known = _known_knowledge_ids(conn, schema)
    if not known:
        if not any(table in schema for table in ("knowledge", "knowledge_items")):
            missing_tables.append("knowledge")
    return build_proactive_action_knowledge_integrity_report(
        rows,
        known_knowledge_ids=known,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_proactive_action_knowledge_integrity_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_action_knowledge_integrity_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    thresholds = report["thresholds"]
    lines = [
        "Proactive Action Knowledge Integrity",
        f"Generated: {report['generated_at']}",
        f"Status: {thresholds['status']}",
        f"Action type: {thresholds['action_type']}",
        f"Min relevance: {thresholds['min_relevance']}",
        f"Totals: scanned={summary['rows_scanned']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No proactive action knowledge integrity gaps found.")
        return "\n".join(lines)
    lines.extend(["", "status | action_type | issue_type | count"])
    for group in report["grouped_counts"]:
        lines.append(f"{group['status']} | {group['action_type']} | {group['issue_type']} | {group['count']}")
    lines.extend(["", "action_id | status | action_type | knowledge_id | relevance | issue"])
    for item in report["findings"]:
        lines.append(
            f"{item['action_id'] or '-'} | {item['status']} | {item['action_type']} | "
            f"{item['knowledge_id'] or '-'} | {item['relevance']} | {item['issue_type']}"
        )
    return "\n".join(lines)


def _findings_for_row(row: dict[str, Any], *, known_ids: set[str], min_relevance: float) -> list[dict[str, Any]]:
    action_id = _int_or_none(row.get("action_id") or row.get("id"))
    status = _clean(row.get("status"), "unknown").lower()
    action_type = _clean(row.get("action_type"), "unknown").lower()
    raw = row.get("knowledge_ids")
    base = {"action_id": action_id, "status": status, "action_type": action_type}
    if raw in (None, ""):
        return []
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, json.JSONDecodeError):
        return [{**base, "issue_type": "malformed_json", "knowledge_id": None, "relevance": None, "reference_index": None}]
    if not isinstance(payload, list):
        return [{**base, "issue_type": "non_list_payload", "knowledge_id": None, "relevance": None, "reference_index": None}]

    findings: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, ref in enumerate(payload):
        parsed = _parse_reference(ref)
        if parsed is None:
            findings.append({**base, "issue_type": "invalid_reference_shape", "knowledge_id": None, "relevance": None, "reference_index": index})
            continue
        knowledge_id, relevance = parsed
        item_base = {**base, "knowledge_id": knowledge_id, "relevance": relevance, "reference_index": index}
        if knowledge_id in seen:
            findings.append({**item_base, "issue_type": "duplicate_knowledge_id"})
        seen.add(knowledge_id)
        if known_ids and knowledge_id not in known_ids:
            findings.append({**item_base, "issue_type": "missing_knowledge"})
        if relevance is not None and relevance < min_relevance:
            findings.append({**item_base, "issue_type": "low_relevance"})
    return findings


def _parse_reference(ref: Any) -> tuple[str, float | None] | None:
    if isinstance(ref, (str, int)):
        text = str(ref).strip()
        return (text, None) if text else None
    if isinstance(ref, list) and 1 <= len(ref) <= 2:
        knowledge_id = str(ref[0]).strip() if ref[0] is not None else ""
        if not knowledge_id:
            return None
        relevance = _float_or_none(ref[1]) if len(ref) == 2 else None
        return (knowledge_id, relevance)
    if isinstance(ref, dict):
        raw_id = ref.get("knowledge_id", ref.get("id"))
        knowledge_id = str(raw_id).strip() if raw_id is not None else ""
        if not knowledge_id:
            return None
        relevance = _float_or_none(ref.get("relevance", ref.get("score")))
        return (knowledge_id, relevance)
    return None


def _load_actions(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> list[dict[str, Any]]:
    if "proactive_actions" not in schema:
        missing_tables.append("proactive_actions")
        return []
    cols = schema["proactive_actions"]
    if "id" not in cols or "knowledge_ids" not in cols:
        missing_columns["proactive_actions"] = sorted({"id", "knowledge_ids"} - cols)
        return []
    select = [
        "id AS action_id",
        _col(cols, "status", "'unknown'") + " AS status",
        _col(cols, "action_type", _col(cols, "type", "'unknown'")) + " AS action_type",
        "knowledge_ids AS knowledge_ids",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM proactive_actions ORDER BY rowid ASC")]


def _known_knowledge_ids(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> set[str]:
    table = "knowledge" if "knowledge" in schema else "knowledge_items" if "knowledge_items" in schema else None
    if table is None:
        return set()
    cols = schema[table]
    id_col = "id" if "id" in cols else "knowledge_id" if "knowledge_id" in cols else None
    if id_col is None:
        return set()
    return {str(row[0]) for row in conn.execute(f"SELECT {id_col} FROM {table}")}


def _grouped_counts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["status"], item["action_type"], item["issue_type"]) for item in items)
    return [
        {"status": status, "action_type": action_type, "issue_type": issue, "count": count}
        for (status, action_type, issue), count in sorted(counts.items())
    ]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["status"], item["action_type"], item["action_id"] or 0, item["reference_index"] if item["reference_index"] is not None else 9999, item["issue_type"])


def _col(columns: set[str], column: str, default: str) -> str:
    return column if column in columns else default


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
