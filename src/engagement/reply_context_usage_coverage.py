"""Report whether reply drafts used available relationship and action context."""

from __future__ import annotations

from collections import defaultdict
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_LOW_COVERAGE_THRESHOLD = 0.5
CONTEXT_FIELDS = (
    "relationship_notes",
    "prior_interaction_summary",
    "target_tweet_text",
    "strategic_action_metadata",
)
_TOKEN_RE = re.compile(r"[a-z0-9_@#]+", re.I)
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "for",
    "from",
    "has",
    "have",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "our",
    "that",
    "the",
    "this",
    "to",
    "with",
    "you",
    "your",
}


def build_reply_context_usage_coverage_report(
    reply_rows: list[dict[str, Any]],
    *,
    low_coverage_threshold: float = DEFAULT_LOW_COVERAGE_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
) -> dict[str, Any]:
    """Return per-draft context usage coverage from in-memory reply rows."""
    if not 0 <= low_coverage_threshold <= 1:
        raise ValueError("low_coverage_threshold must be between 0 and 1")
    if limit <= 0:
        raise ValueError("limit must be positive")

    records = []
    for row in reply_rows:
        draft_text = _text(row.get("draft_text") or row.get("draft") or row.get("reply_text"))
        if not draft_text:
            continue
        context = _context_values(row)
        available = [field for field in CONTEXT_FIELDS if _present(context.get(field))]
        used = [field for field in available if _field_used(field, context[field], draft_text)]
        missing = [field for field in available if field not in used]
        coverage_ratio = round(len(used) / len(available), 4) if available else 1.0
        records.append(
            {
                "reply_id": _text(row.get("reply_id") or row.get("id")),
                "author_handle": _text(row.get("author_handle") or row.get("inbound_author_handle")),
                "strategic_action_type": _strategic_action_type(context.get("strategic_action_metadata"), row),
                "available_context_fields": available,
                "used_context_fields": used,
                "missing_context_fields": missing,
                "coverage_ratio": coverage_ratio,
                "is_low_coverage": bool(available) and coverage_ratio < low_coverage_threshold,
            }
        )

    records.sort(key=_sort_key)
    ranked = records[:limit]
    totals = {
        "draft_count": len(records),
        "record_count": len(ranked),
        "low_coverage": sum(1 for item in records if item["is_low_coverage"]),
        "no_available_context": sum(1 for item in records if not item["available_context_fields"]),
        "average_coverage_ratio": _average(item["coverage_ratio"] for item in records),
    }
    return {
        "artifact_type": "reply_context_usage_coverage",
        "filters": {
            "low_coverage_threshold": low_coverage_threshold,
            "limit": limit,
        },
        "totals": totals,
        "replies": ranked,
        "aggregates": {
            "by_author_handle": _aggregate(records, "author_handle"),
            "by_strategic_action_type": _aggregate(records, "strategic_action_type"),
        },
        "empty_state": {
            "is_empty": not records,
            "message": "No reply drafts found." if not records else None,
        },
    }


def build_reply_context_usage_coverage_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_reply_context_usage_coverage_report(_load_replies(conn, schema), **kwargs)


def format_reply_context_usage_coverage_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_context_usage_coverage_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Context Usage Coverage",
        (
            "Totals: "
            f"drafts={report['totals']['draft_count']} low={report['totals']['low_coverage']} "
            f"avg={report['totals']['average_coverage_ratio']:.2f}"
        ),
    ]
    if not report["replies"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Replies:", "low  ratio  available used missing reply"])
    for item in report["replies"]:
        lines.append(
            f"{str(item['is_low_coverage']):<4} {item['coverage_ratio']:<6.2f} "
            f"{len(item['available_context_fields']):<9} {len(item['used_context_fields']):<4} "
            f"{len(item['missing_context_fields']):<7} {item['reply_id']}"
        )
    return "\n".join(lines)


def _context_values(row: dict[str, Any]) -> dict[str, Any]:
    relationship_context = _parse_json_object(row.get("relationship_context"))
    platform_metadata = _parse_json_object(row.get("platform_metadata"))
    strategic_metadata = _parse_json_object(
        row.get("strategic_action_metadata") or row.get("action_metadata") or row.get("strategic_metadata")
    )
    if not strategic_metadata and _present(row.get("strategic_action_type") or row.get("action_type")):
        strategic_metadata = {"action_type": row.get("strategic_action_type") or row.get("action_type")}
    return {
        "relationship_notes": _first_present(
            row.get("relationship_notes"),
            relationship_context.get("relationship_notes"),
            relationship_context.get("notes"),
            relationship_context.get("relationship_summary"),
        ),
        "prior_interaction_summary": _first_present(
            row.get("prior_interaction_summary"),
            row.get("recent_interactions"),
            relationship_context.get("prior_interaction_summary"),
            relationship_context.get("recent_interactions"),
        ),
        "target_tweet_text": _first_present(
            row.get("target_tweet_text"),
            row.get("inbound_text"),
            row.get("tweet_text"),
            platform_metadata.get("target_tweet_text"),
            platform_metadata.get("parent_post_text"),
        ),
        "strategic_action_metadata": strategic_metadata,
    }


def _field_used(field: str, value: Any, draft_text: str) -> bool:
    if field == "strategic_action_metadata":
        metadata = _parse_json_object(value)
        action_type = _text(metadata.get("action_type") or metadata.get("type") or metadata.get("name"))
        haystack = _normal_tokens(draft_text)
        if action_type and _normalise_token(action_type) in haystack:
            return True
        return _token_overlap(_metadata_text(metadata), draft_text)
    return _token_overlap(_text(value), draft_text)


def _token_overlap(value: str, draft_text: str) -> bool:
    needles = _normal_tokens(value)
    if not needles:
        return False
    haystack = _normal_tokens(draft_text)
    matches = needles & haystack
    required = 1 if len(needles) <= 2 else 2
    return len(matches) >= required


def _normal_tokens(value: str) -> set[str]:
    return {
        token
        for token in (_normalise_token(raw) for raw in _TOKEN_RE.findall(value.lower()))
        if len(token) >= 4 and token not in _STOPWORDS
    }


def _normalise_token(value: str) -> str:
    return value.strip().lower().replace("-", "_")


def _metadata_text(metadata: dict[str, Any]) -> str:
    parts = []
    for value in metadata.values():
        if isinstance(value, (str, int, float)):
            parts.append(str(value))
        elif isinstance(value, list):
            parts.extend(str(item) for item in value if isinstance(item, (str, int, float)))
    return " ".join(parts)


def _strategic_action_type(metadata: Any, row: dict[str, Any]) -> str:
    parsed = _parse_json_object(metadata)
    return _text(
        row.get("strategic_action_type")
        or row.get("action_type")
        or parsed.get("action_type")
        or parsed.get("type")
    )


def _load_replies(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("reply_drafts", "reply_queue", "reply_reviews"):
        columns = schema.get(table)
        if not columns:
            continue
        draft_col = "draft_text" if "draft_text" in columns else "draft" if "draft" in columns else None
        if not draft_col:
            continue
        selected = [
            "id",
            f"{draft_col} AS draft_text",
            _select(columns, ("author_handle", "inbound_author_handle"), "author_handle"),
            _select(columns, ("relationship_notes",), "relationship_notes"),
            _select(columns, ("prior_interaction_summary", "recent_interactions"), "prior_interaction_summary"),
            _select(columns, ("target_tweet_text", "inbound_text", "tweet_text"), "target_tweet_text"),
            _select(columns, ("relationship_context",), "relationship_context"),
            _select(columns, ("platform_metadata",), "platform_metadata"),
            _select(columns, ("strategic_action_metadata", "action_metadata", "strategic_metadata"), "strategic_action_metadata"),
            _select(columns, ("strategic_action_type", "action_type"), "strategic_action_type"),
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    return []


def _select(columns: set[str], names: tuple[str, ...], alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{name} AS {alias}"
    return f"NULL AS {alias}"


def _aggregate(records: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "total_ratio": 0.0, "low_coverage": 0})
    for record in records:
        key = record.get(field) or ""
        if not key:
            continue
        group = groups[key]
        group["count"] += 1
        group["total_ratio"] += record["coverage_ratio"]
        group["low_coverage"] += int(record["is_low_coverage"])
    rows = [
        {
            field: key,
            "count": value["count"],
            "average_coverage_ratio": round(value["total_ratio"] / value["count"], 4),
            "low_coverage": value["low_coverage"],
        }
        for key, value in groups.items()
    ]
    rows.sort(key=lambda item: (-item["low_coverage"], item["average_coverage_ratio"], item[field]))
    return rows


def _average(values: Any) -> float:
    items = list(values)
    return round(sum(items) / len(items), 4) if items else 0.0


def _sort_key(item: dict[str, Any]) -> tuple[int, float, str]:
    return (-int(item["is_low_coverage"]), item["coverage_ratio"], item["reply_id"])


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first_present(*values: Any) -> Any:
    for value in values:
        if _present(value):
            return value
    return None


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, dict):
        return bool(value)
    if isinstance(value, list):
        return bool(value)
    return bool(str(value).strip())


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}
