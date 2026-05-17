"""Classify reply drafts by source and context grounding."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_MIN_OVERLAP = 0.18
DEFAULT_LIMIT = 100
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = {"about", "after", "again", "because", "from", "have", "into", "that", "the", "this", "with", "your", "you", "and", "for"}


def build_reply_source_grounding_report(
    db_or_conn: Any,
    *,
    min_overlap: float = DEFAULT_MIN_OVERLAP,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not 0 <= min_overlap <= 1:
        raise ValueError("min_overlap must be between 0 and 1")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_reply_rows(conn, schema)
    findings = [_classify(row, min_overlap) for row in rows]
    findings.sort(key=lambda item: (_status_rank(item["grounding_status"]), item["reply_id"]))
    weak_count = sum(1 for item in findings if item["grounding_status"] != "grounded")
    return {
        "artifact_type": "reply_source_grounding",
        "generated_at": generated_at.isoformat(),
        "filters": {"min_overlap": min_overlap, "limit": limit},
        "totals": {
            "reply_count": len(findings),
            "grounded_count": sum(1 for item in findings if item["grounding_status"] == "grounded"),
            "weak_overlap_count": sum(1 for item in findings if item["grounding_status"] == "weak_overlap"),
            "missing_context_count": sum(1 for item in findings if item["grounding_status"] == "missing_context"),
            "missing_evidence_count": sum(1 for item in findings if item["grounding_status"] == "missing_evidence"),
            "weak_grounding_rate": round(weak_count / len(findings), 4) if findings else 0.0,
        },
        "findings": findings[:limit],
        "weak_findings": [item for item in findings if item["grounding_status"] != "grounded"][:limit],
        "missing_tables": [] if "reply_queue" in schema else ["reply_queue"],
    }


def format_reply_source_grounding_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_source_grounding_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Reply Source Grounding",
        f"Generated: {report['generated_at']}",
        f"Filters: min_overlap={report['filters']['min_overlap']:g} limit={report['filters']['limit']}",
        (
            "Totals: "
            f"replies={totals['reply_count']} grounded={totals['grounded_count']} "
            f"weak_overlap={totals['weak_overlap_count']} missing_context={totals['missing_context_count']} "
            f"missing_evidence={totals['missing_evidence_count']} weak_rate={totals['weak_grounding_rate']:.1%}"
        ),
    ]
    if not report["findings"]:
        lines.append("No reply drafts found.")
        return "\n".join(lines)
    lines.extend(["", "Findings:", "status            overlap  evidence  reply"])
    for item in report["findings"]:
        lines.append(
            f"{item['grounding_status']:<16}  {item['overlap_score']:>7.2f}  "
            f"{str(item['has_source_evidence']):<8}  #{item['reply_id']} {item['draft_preview']}"
        )
    return "\n".join(lines)


def _load_reply_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("reply_queue", set())
    if "id" not in columns:
        return []
    selected = [
        "id",
        _select(columns, ("draft_text", "reply_text", "text"), "draft_text"),
        _select(columns, ("mention_text", "inbound_text", "original_text"), "mention_text"),
        _select(columns, ("context", "context_text", "available_context"), "context_text"),
        _select(columns, ("source_url", "source_urls", "source_id", "source_ids", "source_evidence"), "source_evidence"),
        _select(columns, ("relationship", "relationship_context", "author_relationship"), "relationship_context"),
        _select(columns, ("status",), "status"),
    ]
    rows = conn.execute(f"SELECT {', '.join(selected)} FROM reply_queue").fetchall()
    return [dict(row) for row in rows]


def _classify(row: dict[str, Any], min_overlap: float) -> dict[str, Any]:
    draft = _text(row.get("draft_text"))
    context_text = " ".join(
        value for value in [_text(row.get("mention_text")), _text(row.get("context_text"))] if value
    )
    source_evidence = _text(row.get("source_evidence"))
    relationship = _text(row.get("relationship_context"))
    context_tokens = _tokens(context_text)
    draft_tokens = _tokens(draft)
    overlap = len(context_tokens & draft_tokens) / len(context_tokens) if context_tokens else 0.0
    has_context = bool(context_tokens)
    has_evidence = bool(source_evidence or relationship)
    if not has_context:
        status = "missing_context"
    elif not has_evidence:
        status = "missing_evidence"
    elif overlap < min_overlap:
        status = "weak_overlap"
    else:
        status = "grounded"
    return {
        "reply_id": _text(row.get("id")),
        "status": _text(row.get("status")),
        "grounding_status": status,
        "overlap_score": round(overlap, 4),
        "has_context": has_context,
        "has_source_evidence": has_evidence,
        "context_token_count": len(context_tokens),
        "overlap_token_count": len(context_tokens & draft_tokens),
        "draft_preview": draft[:120],
        "reasons": _reasons(status, has_context, has_evidence, overlap, min_overlap),
    }


def _reasons(status: str, has_context: bool, has_evidence: bool, overlap: float, min_overlap: float) -> list[str]:
    if status == "grounded":
        return ["context_overlap", "source_or_relationship_evidence"]
    reasons = []
    if not has_context:
        reasons.append("no_available_context")
    if not has_evidence:
        reasons.append("missing_source_or_relationship_evidence")
    if has_context and overlap < min_overlap:
        reasons.append("weak_context_overlap")
    return reasons


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


def _tokens(value: str) -> set[str]:
    return {token for token in _TOKEN_RE.findall(value.lower()) if len(token) > 2 and token not in _STOPWORDS}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _status_rank(status: str) -> int:
    return {"missing_context": 0, "missing_evidence": 1, "weak_overlap": 2, "grounded": 3}.get(status, 4)
