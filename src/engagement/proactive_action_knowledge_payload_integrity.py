"""Audit proactive_actions.knowledge_ids payload integrity."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


ARTIFACT_TYPE = "proactive_action_knowledge_payload_integrity"
DEFAULT_LIMIT = 100
POSTED_SUPPORT_ACTION_TYPES = {"reply", "quote_tweet"}
RESOLVED_STATUSES = {"approved", "posted"}
REASON_ORDER = (
    "malformed_knowledge_ids_json",
    "missing_knowledge_id",
    "missing_relevance_score",
    "invalid_relevance_score",
    "missing_knowledge_reference",
    "restricted_knowledge_resolved_action",
    "posted_action_empty_knowledge_support",
)
REQUIRED_ACTION_COLUMNS = {"id", "status", "action_type", "knowledge_ids"}
REQUIRED_KNOWLEDGE_COLUMNS = {"id"}


def build_proactive_action_knowledge_payload_integrity_report(
    rows: list[dict[str, Any]],
    *,
    knowledge_rows: list[dict[str, Any]] | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic proactive action knowledge payload report."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    knowledge = _knowledge_by_id(knowledge_rows or [])
    findings: list[dict[str, Any]] = []

    for row in rows:
        findings.extend(_findings_for_row(_normalize_row(row), knowledge))

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "summary": {
            "action_count": len(rows),
            "knowledge_count": len(knowledge),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_reason": _counts_by_reason(findings),
        },
        "groups": _groups(findings),
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No proactive action knowledge payload integrity gaps found." if not findings else None,
        },
    }


def build_proactive_action_knowledge_payload_integrity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load proactive actions and knowledge rows from SQLite."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [table for table in ("proactive_actions", "knowledge") if table not in schema]
    missing_columns: dict[str, list[str]] = {}
    if "proactive_actions" in schema:
        missing = sorted(REQUIRED_ACTION_COLUMNS - schema["proactive_actions"])
        if missing:
            missing_columns["proactive_actions"] = missing
    if "knowledge" in schema:
        missing = sorted(REQUIRED_KNOWLEDGE_COLUMNS - schema["knowledge"])
        if missing:
            missing_columns["knowledge"] = missing
    if missing_tables or missing_columns:
        return build_proactive_action_knowledge_payload_integrity_report(
            [],
            knowledge_rows=[],
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            **kwargs,
        )
    return build_proactive_action_knowledge_payload_integrity_report(
        _load_actions(conn),
        knowledge_rows=_load_knowledge(conn, schema["knowledge"]),
        **kwargs,
    )


def format_proactive_action_knowledge_payload_integrity_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_action_knowledge_payload_integrity_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Proactive Action Knowledge Payload Integrity",
        f"Generated: {report['generated_at']}",
        f"Filters: limit={report['filters']['limit']}",
        (
            "Totals: "
            f"actions={summary['action_count']} "
            f"knowledge={summary['knowledge_count']} "
            f"findings={summary['finding_count']} "
            f"shown={summary['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "status | action_type | reason | count"])
    for group in report["groups"]:
        lines.append(f"{group['status']} | {group['action_type']} | {group['reason']} | {group['count']}")
    lines.extend(["", "action_id | status | action_type | reference_index | knowledge_id | relevance | reason"])
    for item in report["findings"]:
        lines.append(
            f"{_display(item['action_id'])} | {item['status']} | {item['action_type']} | "
            f"{_display(item['reference_index'])} | {_display(item['knowledge_id'])} | "
            f"{_display(item['relevance'])} | {item['reason']}"
        )
    return "\n".join(lines)


def _load_actions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT id AS action_id,
                  status,
                  action_type,
                  knowledge_ids
           FROM proactive_actions
           ORDER BY id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_knowledge(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    license_expr = "license" if "license" in columns else "NULL AS license"
    rows = conn.execute(f"SELECT id AS knowledge_id, {license_expr} FROM knowledge ORDER BY id ASC").fetchall()
    return [dict(row) for row in rows]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "action_id": _int_or_none(row.get("action_id") or row.get("id")),
        "status": _clean(row.get("status"), "unknown").lower(),
        "action_type": _clean(row.get("action_type"), "unknown").lower(),
        "knowledge_ids": row.get("knowledge_ids"),
    }


def _findings_for_row(row: dict[str, Any], knowledge: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    payload, error = _payload_list(row.get("knowledge_ids"))
    if error:
        return [_finding(row, "malformed_knowledge_ids_json", detail=error)]

    findings: list[dict[str, Any]] = []
    if row["status"] == "posted" and row["action_type"] in POSTED_SUPPORT_ACTION_TYPES and not payload:
        findings.append(_finding(row, "posted_action_empty_knowledge_support"))

    for index, item in enumerate(payload):
        reference = _reference(item)
        if reference.missing_id:
            findings.append(_finding(row, "missing_knowledge_id", reference_index=index, relevance=reference.relevance))
            continue
        if reference.missing_relevance:
            findings.append(_finding(row, "missing_relevance_score", reference_index=index, knowledge_id=reference.knowledge_id))
        if reference.invalid_relevance:
            findings.append(
                _finding(
                    row,
                    "invalid_relevance_score",
                    reference_index=index,
                    knowledge_id=reference.knowledge_id,
                    relevance=reference.relevance,
                )
            )
        if reference.knowledge_id and reference.knowledge_id not in knowledge:
            findings.append(
                _finding(
                    row,
                    "missing_knowledge_reference",
                    reference_index=index,
                    knowledge_id=reference.knowledge_id,
                    relevance=reference.relevance,
                )
            )
        if (
            reference.knowledge_id
            and reference.knowledge_id in knowledge
            and row["status"] in RESOLVED_STATUSES
            and _clean(knowledge[reference.knowledge_id].get("license")).lower() == "restricted"
        ):
            findings.append(
                _finding(
                    row,
                    "restricted_knowledge_resolved_action",
                    reference_index=index,
                    knowledge_id=reference.knowledge_id,
                    relevance=reference.relevance,
                    knowledge_license="restricted",
                )
            )
    return findings


class _Reference:
    def __init__(
        self,
        *,
        knowledge_id: str | None = None,
        relevance: float | str | None = None,
        missing_id: bool = False,
        missing_relevance: bool = False,
        invalid_relevance: bool = False,
    ) -> None:
        self.knowledge_id = knowledge_id
        self.relevance = relevance
        self.missing_id = missing_id
        self.missing_relevance = missing_relevance
        self.invalid_relevance = invalid_relevance


def _reference(item: Any) -> _Reference:
    if isinstance(item, (str, int)):
        text = str(item).strip()
        return _Reference(knowledge_id=text or None, missing_id=not text)
    if isinstance(item, (list, tuple)):
        knowledge_id = str(item[0]).strip() if item and item[0] is not None else ""
        if not knowledge_id:
            return _Reference(missing_id=True, relevance=_score_value(item[1]) if len(item) > 1 else None)
        if len(item) < 2:
            return _Reference(knowledge_id=knowledge_id, missing_relevance=True)
        return _with_score(knowledge_id, item[1])
    if isinstance(item, dict):
        raw_id = item.get("knowledge_id", item.get("id"))
        knowledge_id = str(raw_id).strip() if raw_id is not None else ""
        raw_score = item.get("relevance", item.get("score"))
        if not knowledge_id:
            return _Reference(missing_id=True, relevance=_score_value(raw_score))
        if raw_score is None or str(raw_score).strip() == "":
            return _Reference(knowledge_id=knowledge_id, missing_relevance=True)
        return _with_score(knowledge_id, raw_score)
    return _Reference(missing_id=True)


def _with_score(knowledge_id: str, raw_score: Any) -> _Reference:
    score = _score_value(raw_score)
    if not isinstance(score, float):
        return _Reference(knowledge_id=knowledge_id, relevance=score, invalid_relevance=True)
    return _Reference(knowledge_id=knowledge_id, relevance=score, invalid_relevance=score < 0 or score > 1)


def _score_value(value: Any) -> float | str | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)


def _payload_list(raw: Any) -> tuple[list[Any], str | None]:
    if raw is None or _clean(raw) == "":
        return [], None
    if isinstance(raw, list):
        return raw, None
    try:
        parsed = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError) as exc:
        return [], f"knowledge_ids is not valid JSON: {exc}"
    if not isinstance(parsed, list):
        return [], "knowledge_ids must be a JSON list"
    return parsed, None


def _finding(row: dict[str, Any], reason: str, **extra: Any) -> dict[str, Any]:
    return {
        "action_id": row["action_id"],
        "status": row["status"],
        "action_type": row["action_type"],
        "reference_index": None,
        "knowledge_id": None,
        "relevance": None,
        "reason": reason,
        **extra,
    }


def _knowledge_by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        knowledge_id = _clean(row.get("knowledge_id") or row.get("id"))
        if knowledge_id:
            result[knowledge_id] = row
    return result


def _counts_by_reason(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(item["reason"] for item in findings)
    return {reason: counts[reason] for reason in REASON_ORDER}


def _groups(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["status"], item["action_type"], item["reason"]) for item in findings)
    return [
        {"status": status, "action_type": action_type, "reason": reason, "count": count}
        for (status, action_type, reason), count in sorted(
            counts.items(),
            key=lambda item: (item[0][0], item[0][1], _reason_rank(item[0][2])),
        )
    ]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    index = item["reference_index"] if item["reference_index"] is not None else 9999
    return (_reason_rank(item["reason"]), item["status"], item["action_type"], item["action_id"] or 0, index)


def _reason_rank(reason: str) -> int:
    return REASON_ORDER.index(reason) if reason in REASON_ORDER else len(REASON_ORDER)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None and str(value).strip() else None
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
