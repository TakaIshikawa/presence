"""Check queued reply drafts for citation sufficiency."""

from __future__ import annotations

import json
import re
import sqlite3
from statistics import mean
from typing import Any, Iterable


DEFAULT_STATUS = "pending"
DEFAULT_LIMIT = 50
ADEQUATE_LINK_SCORE = 0.7

STATUS_SUFFICIENT = "sufficient"
STATUS_THIN = "thin_evidence"
STATUS_MISSING = "missing_evidence"

ACTION_ADD_LINK = "add_knowledge_link"
ACTION_SOFTEN = "soften_claim"
ACTION_MANUAL = "route_for_manual_review"
ACTION_READY = "ready_for_review"

ASSERTIVE_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\b(always|never|must|should|need(?:s|ed)? to|have to|cannot|can't)\b",
        r"\b(will|won't|does|doesn't|is|isn't|are|aren't|means|proves|shows)\b",
        r"\b(the key is|the reason is|in practice|the best|the only)\b",
        r"\b\d+(?:\.\d+)?\s*(?:%|percent|x|times|days?|weeks?|months?)\b",
    )
)

HEDGE_RE = re.compile(
    r"\b(might|may|could|can|often|usually|sometimes|roughly|probably|I think|"
    r"I'd|I would|it depends|one way|a way)\b",
    re.IGNORECASE,
)

KNOWLEDGE_TERMS = {
    "architecture",
    "benchmark",
    "build",
    "cache",
    "customer",
    "data",
    "deploy",
    "docs",
    "incident",
    "latency",
    "metric",
    "model",
    "performance",
    "production",
    "release",
    "reliability",
    "research",
    "sdk",
    "test",
    "tradeoff",
    "workflow",
}


def build_reply_citation_sufficiency_report(
    db: Any,
    *,
    status: str | Iterable[str] = DEFAULT_STATUS,
    limit: int = DEFAULT_LIMIT,
) -> dict[str, Any]:
    """Build a deterministic citation sufficiency report for reply drafts."""
    if limit < 0:
        raise ValueError("limit must be non-negative")

    conn = _connection(db)
    statuses = _normalize_status_filter(status)
    schema = _schema(conn)
    reply_columns = schema.get("reply_queue", set())
    if not reply_columns:
        return _empty_report(statuses, limit)

    rows = _reply_rows(conn, reply_columns, statuses, limit)
    reply_ids = [int(row["id"]) for row in rows if row.get("id") is not None]
    link_stats = _knowledge_link_stats(conn, schema, reply_ids)
    findings = [
        inspect_reply_citation_sufficiency(row, link_stats.get(int(row.get("id") or 0), {}))
        for row in rows
    ]
    findings.sort(key=_finding_sort_key)
    return {
        "filters": {
            "status": list(statuses),
            "limit": limit,
        },
        "totals": _totals(findings),
        "findings": findings,
    }


def inspect_reply_citation_sufficiency(
    row: dict[str, Any],
    link_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Classify one reply_queue-style row for evidence sufficiency."""
    stats = link_stats or {}
    draft = str(row.get("draft_text") or "")
    claim_score, claim_reasons = _claim_score(draft)
    link_count = int(stats.get("link_count") or 0)
    average_relevance = stats.get("average_relevance")
    strong_link_count = int(stats.get("strong_link_count") or 0)
    status = _classification(claim_score, link_count, strong_link_count)
    actions = _suggested_actions(status, claim_score, link_count)

    return {
        "id": _int_or_none(row.get("id")),
        "reply_id": row.get("inbound_tweet_id"),
        "status": row.get("status") or "pending",
        "platform": row.get("platform") or "x",
        "author": row.get("inbound_author_handle"),
        "citation_status": status,
        "claim_score": claim_score,
        "claim_reasons": claim_reasons,
        "knowledge_link_count": link_count,
        "strong_knowledge_link_count": strong_link_count,
        "average_relevance_score": average_relevance,
        "suggested_actions": actions,
        "draft_preview": _preview(draft),
    }


def format_reply_citation_sufficiency_json(report: dict[str, Any]) -> str:
    """Serialize the report as stable JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_citation_sufficiency_text(report: dict[str, Any]) -> str:
    """Format a concise human-readable citation sufficiency report."""
    totals = report["totals"]
    filters = report["filters"]
    lines = [
        "Reply Citation Sufficiency Report",
        f"Status: {', '.join(filters['status'])}",
        f"Limit: {filters['limit']}",
        (
            f"Rows: scanned={totals['replies_scanned']} "
            f"sufficient={totals['sufficient']} "
            f"thin_evidence={totals['thin_evidence']} "
            f"missing_evidence={totals['missing_evidence']}"
        ),
        "",
    ]
    if not report["findings"]:
        lines.append("No reply drafts matched.")
        return "\n".join(lines).rstrip()

    lines.append("Findings:")
    for item in report["findings"]:
        actions = ", ".join(item["suggested_actions"]) or ACTION_READY
        reasons = ", ".join(item["claim_reasons"]) or "low claim density"
        lines.append(
            f"#{item['id']} {item['citation_status']} links={item['knowledge_link_count']} "
            f"strong={item['strong_knowledge_link_count']} score={item['claim_score']} "
            f"@{item['author'] or 'unknown'}"
        )
        lines.append(f"  reasons: {reasons}")
        lines.append(f"  actions: {actions}")
        if item["draft_preview"]:
            lines.append(f"  draft: {item['draft_preview']}")
    return "\n".join(lines).rstrip()


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    schema: dict[str, set[str]] = {}
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    except sqlite3.Error:
        return schema
    for row in rows:
        table = str(row[0])
        try:
            schema[table] = {str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")}
        except sqlite3.Error:
            schema[table] = set()
    return schema


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    statuses: tuple[str, ...],
    limit: int,
) -> list[dict[str, Any]]:
    if "id" not in columns:
        return []
    filters: list[str] = []
    params: list[Any] = []
    if statuses and "all" not in statuses and "status" in columns:
        placeholders = ", ".join("?" for _ in statuses)
        filters.append(f"LOWER(COALESCE(status, 'pending')) IN ({placeholders})")
        params.extend(statuses)

    query = "SELECT * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns)
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    cursor = conn.execute(query, params)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _knowledge_link_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    reply_ids: list[int],
) -> dict[int, dict[str, Any]]:
    columns = schema.get("reply_knowledge_links", set())
    if not reply_ids or "reply_queue_id" not in columns:
        return {}
    relevance_expr = "relevance_score" if "relevance_score" in columns else "NULL"
    placeholders = ", ".join("?" for _ in reply_ids)
    cursor = conn.execute(
        f"""SELECT reply_queue_id, {relevance_expr} AS relevance_score
            FROM reply_knowledge_links
            WHERE reply_queue_id IN ({placeholders})
            ORDER BY reply_queue_id ASC, id ASC""",
        reply_ids,
    )
    grouped: dict[int, list[float | None]] = {}
    for row in cursor.fetchall():
        reply_id = int(row[0])
        grouped.setdefault(reply_id, []).append(_float_or_none(row[1]))
    return {
        reply_id: {
            "link_count": len(scores),
            "strong_link_count": sum(1 for score in scores if score is None or score >= ADEQUATE_LINK_SCORE),
            "average_relevance": round(mean(score for score in scores if score is not None), 3)
            if any(score is not None for score in scores)
            else None,
        }
        for reply_id, scores in grouped.items()
    }


def _claim_score(text: str) -> tuple[int, list[str]]:
    normalized = text.strip()
    if not normalized:
        return 0, ["empty_draft"]
    reasons: list[str] = []
    score = 0
    for pattern in ASSERTIVE_PATTERNS:
        if pattern.search(normalized):
            score += 1
    if score:
        reasons.append("assertive_language")
    sentence_count = max(1, len(re.findall(r"[.!?]+", normalized)) or 1)
    if sentence_count >= 2 and score:
        score += 1
        reasons.append("multi_sentence_claim")
    if set(re.findall(r"[a-z0-9][a-z0-9_-]*", normalized.lower())) & KNOWLEDGE_TERMS:
        score += 1
        reasons.append("knowledge_backed_topic")
    if HEDGE_RE.search(normalized) and score > 0:
        score -= 1
        reasons.append("softened_language")
    return max(0, score), reasons


def _classification(claim_score: int, link_count: int, strong_link_count: int) -> str:
    if claim_score <= 1:
        return STATUS_SUFFICIENT
    if link_count == 0:
        return STATUS_MISSING
    if strong_link_count == 0 or (claim_score >= 3 and strong_link_count < 2):
        return STATUS_THIN
    return STATUS_SUFFICIENT


def _suggested_actions(status: str, claim_score: int, link_count: int) -> list[str]:
    if status == STATUS_SUFFICIENT:
        return [ACTION_READY]
    actions = []
    if link_count == 0:
        actions.append(ACTION_ADD_LINK)
    if claim_score >= 2:
        actions.append(ACTION_SOFTEN)
    if status == STATUS_THIN:
        actions.append(ACTION_ADD_LINK)
    actions.append(ACTION_MANUAL)
    return sorted(dict.fromkeys(actions))


def _totals(findings: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "replies_scanned": len(findings),
        STATUS_SUFFICIENT: sum(1 for item in findings if item["citation_status"] == STATUS_SUFFICIENT),
        STATUS_THIN: sum(1 for item in findings if item["citation_status"] == STATUS_THIN),
        STATUS_MISSING: sum(1 for item in findings if item["citation_status"] == STATUS_MISSING),
    }


def _empty_report(statuses: tuple[str, ...], limit: int) -> dict[str, Any]:
    return {
        "filters": {"status": list(statuses), "limit": limit},
        "totals": _totals([]),
        "findings": [],
    }


def _normalize_status_filter(status: str | Iterable[str]) -> tuple[str, ...]:
    raw = status.split(",") if isinstance(status, str) else list(status)
    statuses = tuple(sorted({str(item).strip().lower() for item in raw if str(item).strip()}))
    if not statuses:
        raise ValueError("status must not be empty")
    return statuses


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    parts.append("id ASC")
    return ", ".join(parts)


def _finding_sort_key(item: dict[str, Any]) -> tuple[int, int]:
    priority = {STATUS_MISSING: 0, STATUS_THIN: 1, STATUS_SUFFICIENT: 2}
    return (priority.get(str(item["citation_status"]), 99), int(item.get("id") or 0))


def _preview(value: str, limit: int = 120) -> str:
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1].rstrip() + "..."


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
