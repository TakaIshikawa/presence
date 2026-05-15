"""Measure drift between drafted replies and reviewer-edited finals."""

from __future__ import annotations

from collections import defaultdict
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
HEDGING_TERMS = ("maybe", "might", "could", "likely", "probably", "seems", "appears", "roughly")
TONE_MARKERS = ("!", "thanks", "sorry", "appreciate", "great", "please")
_CLAIM_RE = re.compile(r"[^.!?]*(?:\d+%|\b\d+\b|because|will|must|always|never|guarantee)[^.!?]*[.!?]?", re.I)
_TOKEN_RE = re.compile(r"\w+|[^\w\s]", re.UNICODE)


def build_reply_draft_revision_drift_report(
    reply_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
) -> dict[str, Any]:
    """Return per-reply revision drift and optional metadata aggregates."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    records = []
    for row in reply_rows:
        draft = _text(row.get("draft_text"))
        final = _text(row.get("final_text") or row.get("reviewed_text"))
        if not draft and not final:
            continue
        metrics = _drift_metrics(draft, final)
        records.append(
            {
                "reply_id": _text(row.get("reply_id") or row.get("id")),
                "author_handle": _text(row.get("author_handle") or row.get("inbound_author_handle")),
                "topic": _text(row.get("topic")),
                **metrics,
            }
        )
    records.sort(key=_sort_key)
    ranked = records[:limit]
    aggregates = {
        "by_author_handle": _aggregate(records, "author_handle"),
        "by_topic": _aggregate(records, "topic"),
    }
    totals = {
        "reply_count": len(records),
        "record_count": len(ranked),
        "unchanged": sum(1 for item in records if item["drift_bucket"] == "unchanged"),
        "light_edit": sum(1 for item in records if item["drift_bucket"] == "light_edit"),
        "substantial_edit": sum(1 for item in records if item["drift_bucket"] == "substantial_edit"),
        "rewrite": sum(1 for item in records if item["drift_bucket"] == "rewrite"),
    }
    return {
        "artifact_type": "reply_draft_revision_drift",
        "totals": totals,
        "replies": ranked,
        "aggregates": aggregates,
        "empty_state": {
            "is_empty": not records,
            "message": "No reply draft/final rows found." if not records else None,
        },
    }


def build_reply_draft_revision_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_reply_draft_revision_drift_report(_load_replies(conn, schema), **kwargs)


def format_reply_draft_revision_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_draft_revision_drift_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Draft Revision Drift",
        (
            "Totals: "
            f"replies={report['totals']['reply_count']} rewrite={report['totals']['rewrite']} "
            f"substantial={report['totals']['substantial_edit']} light={report['totals']['light_edit']}"
        ),
    ]
    if not report["replies"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Replies:", "bucket            dist  ratio  removed  hedge  reply"])
    for item in report["replies"]:
        lines.append(
            f"{item['drift_bucket']:<16} {item['edit_distance']:<5} "
            f"{item['edit_ratio']:<5.2f} {len(item['removed_claims']):<8} "
            f"{item['added_hedging_count']:<6} {item['reply_id']}"
        )
    return "\n".join(lines)


def _drift_metrics(draft: str, final: str) -> dict[str, Any]:
    distance = _levenshtein(_tokens(draft), _tokens(final))
    base = max(len(_tokens(draft)), 1)
    ratio = round(distance / base, 4)
    removed_claims = _removed_claims(draft, final)
    added_hedging = _added_terms(draft, final, HEDGING_TERMS)
    draft_tone = _term_counts(draft, TONE_MARKERS)
    final_tone = _term_counts(final, TONE_MARKERS)
    changed_tone = sorted(term for term in set(draft_tone) | set(final_tone) if draft_tone.get(term, 0) != final_tone.get(term, 0))
    return {
        "edit_distance": distance,
        "edit_ratio": ratio,
        "removed_claims": removed_claims,
        "added_hedging": added_hedging,
        "added_hedging_count": sum(added_hedging.values()),
        "changed_tone_markers": changed_tone,
        "drift_bucket": _bucket(ratio, draft, final),
    }


def _load_replies(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("reply_reviews", "reply_drafts", "reply_queue"):
        columns = schema.get(table)
        if not columns:
            continue
        draft_col = "draft_text" if "draft_text" in columns else "draft" if "draft" in columns else None
        final_col = "final_text" if "final_text" in columns else "reviewed_text" if "reviewed_text" in columns else None
        if not draft_col or not final_col:
            continue
        selected = [
            "id",
            f"{draft_col} AS draft_text",
            f"{final_col} AS final_text",
            "author_handle" if "author_handle" in columns else "inbound_author_handle" if "inbound_author_handle" in columns else "NULL AS author_handle",
            "topic" if "topic" in columns else "NULL AS topic",
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    return []


def _bucket(ratio: float, draft: str, final: str) -> str:
    if draft == final:
        return "unchanged"
    if _levenshtein(_tokens(draft), _tokens(final)) <= 2:
        return "light_edit"
    if ratio < 0.15:
        return "light_edit"
    if ratio < 0.8:
        return "substantial_edit"
    return "rewrite"


def _removed_claims(draft: str, final: str) -> list[str]:
    final_norm = _normalize(final)
    removed = []
    for match in _CLAIM_RE.findall(draft):
        claim = " ".join(match.split())
        if claim and _normalize(claim) not in final_norm:
            removed.append(claim)
    return removed


def _added_terms(draft: str, final: str, terms: tuple[str, ...]) -> dict[str, int]:
    draft_counts = _term_counts(draft, terms)
    final_counts = _term_counts(final, terms)
    return {term: final_counts.get(term, 0) - draft_counts.get(term, 0) for term in terms if final_counts.get(term, 0) > draft_counts.get(term, 0)}


def _term_counts(text: str, terms: tuple[str, ...]) -> dict[str, int]:
    lowered = text.lower()
    return {term: len(re.findall(rf"\b{re.escape(term)}\b", lowered)) if term != "!" else lowered.count("!") for term in terms}


def _aggregate(records: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "total_ratio": 0.0, "rewrite": 0, "substantial_edit": 0})
    for record in records:
        key = record.get(field) or ""
        if not key:
            continue
        group = groups[key]
        group["count"] += 1
        group["total_ratio"] += record["edit_ratio"]
        if record["drift_bucket"] in {"rewrite", "substantial_edit"}:
            group[record["drift_bucket"]] += 1
    rows = [
        {
            field: key,
            "count": value["count"],
            "average_edit_ratio": round(value["total_ratio"] / value["count"], 4),
            "substantial_edit": value["substantial_edit"],
            "rewrite": value["rewrite"],
        }
        for key, value in groups.items()
    ]
    rows.sort(key=lambda item: (-(item["rewrite"] + item["substantial_edit"]), -item["average_edit_ratio"], item[field]))
    return rows


def _levenshtein(a: list[str], b: list[str]) -> int:
    previous = list(range(len(b) + 1))
    for i, token_a in enumerate(a, 1):
        current = [i]
        for j, token_b in enumerate(b, 1):
            current.append(min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + (token_a != token_b)))
        previous = current
    return previous[-1]


def _tokens(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


def _normalize(text: str) -> str:
    return " ".join(_tokens(text))


def _sort_key(item: dict[str, Any]) -> tuple[int, float, str]:
    rank = {"rewrite": 3, "substantial_edit": 2, "light_edit": 1, "unchanged": 0}
    return (-rank[item["drift_bucket"]], -item["edit_ratio"], item["reply_id"])


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
