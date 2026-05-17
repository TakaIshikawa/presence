"""Detect repeated or near-repeated newsletter introductions."""

from __future__ import annotations

from datetime import datetime, timezone
from difflib import SequenceMatcher
import json
import re
import sqlite3
from typing import Any


DEFAULT_THRESHOLD = 0.82
DEFAULT_OPENING_WORDS = 18
DEFAULT_LIMIT = 50


def build_newsletter_intro_repetition_risk_report(
    rows: list[dict[str, Any]],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    opening_words: int = DEFAULT_OPENING_WORDS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return repeated intro opening pairs from newsletter rows."""
    if threshold < 0 or threshold > 1:
        raise ValueError("threshold must be between 0 and 1")
    if opening_words <= 0:
        raise ValueError("opening_words must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    intros = [_normalize_row(row, opening_words) for row in rows]
    usable = [intro for intro in intros if intro["normalized_opening"]]
    missing = [intro["newsletter_id"] for intro in intros if not intro["normalized_opening"]]
    pairs: list[dict[str, Any]] = []

    for index, left in enumerate(usable):
        for right in usable[index + 1 :]:
            score = round(SequenceMatcher(None, left["normalized_opening"], right["normalized_opening"]).ratio(), 4)
            if score >= threshold:
                pairs.append(
                    {
                        "left_newsletter_id": left["newsletter_id"],
                        "right_newsletter_id": right["newsletter_id"],
                        "affected_newsletter_ids": [left["newsletter_id"], right["newsletter_id"]],
                        "left_opening": left["opening"],
                        "right_opening": right["opening"],
                        "similarity": score,
                        "risk_level": _risk_level(score),
                        "repeated_clause": _shared_prefix(left["normalized_opening"], right["normalized_opening"]),
                    }
                )

    pairs.sort(key=lambda pair: (-pair["similarity"], pair["left_newsletter_id"], pair["right_newsletter_id"]))
    groups = _groups(pairs)
    return {
        "artifact_type": "newsletter_intro_repetition_risk",
        "generated_at": generated_at.isoformat(),
        "filters": {"threshold": threshold, "opening_words": opening_words},
        "totals": {
            "rows_scanned": len(rows),
            "intro_count": len(usable),
            "missing_intro_count": len(missing),
            "flagged_pair_count": len(pairs),
            "group_count": len(groups),
        },
        "pairs": pairs,
        "groups": groups,
        "missing_intro_newsletter_ids": missing,
        "empty_state": {
            "is_empty": not pairs,
            "message": "No repeated newsletter introductions found." if not pairs else None,
        },
    }


def build_newsletter_intro_repetition_risk_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    return build_newsletter_intro_repetition_risk_report(_load_rows(conn, _schema(conn)), **kwargs)


def format_newsletter_intro_repetition_risk_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_intro_repetition_risk_table(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Intro Repetition Risk",
        f"Generated: {report['generated_at']}",
        f"Threshold: {report['filters']['threshold']} opening_words={report['filters']['opening_words']}",
        (
            "Totals: "
            f"intros={report['totals']['intro_count']} "
            f"pairs={report['totals']['flagged_pair_count']} "
            f"groups={report['totals']['group_count']} "
            f"missing={report['totals']['missing_intro_count']}"
        ),
    ]
    if not report["pairs"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "left_id | right_id | similarity | risk | repeated_clause"])
    for pair in report["pairs"]:
        clause = pair["repeated_clause"] or "-"
        lines.append(
            f"{pair['left_newsletter_id']} | {pair['right_newsletter_id']} | "
            f"{pair['similarity']:.4f} | {pair['risk_level']} | {clause}"
        )
    return "\n".join(lines)


format_newsletter_intro_repetition_risk_text = format_newsletter_intro_repetition_risk_table


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "newsletter_sends" if "newsletter_sends" in schema else "generated_content" if "generated_content" in schema else None
    if table is None:
        return []
    columns = schema[table]
    selected = [
        _col(columns, "id", "newsletter_id", "issue_id", default="rowid") + " AS newsletter_id",
        _col(columns, "issue_id", "subject", default="NULL") + " AS issue_id",
        _col(columns, "intro", "introduction", "opening", "preview_text", "preheader", "summary", "content", default="NULL") + " AS intro",
        _col(columns, "sent_at", "created_at", "updated_at", default="NULL") + " AS timestamp",
    ]
    where = ""
    if table == "generated_content" and "content_type" in columns:
        where = " WHERE LOWER(COALESCE(content_type, '')) LIKE '%newsletter%'"
    order = " ORDER BY datetime(timestamp) DESC" if any(col in columns for col in ("sent_at", "created_at", "updated_at")) else ""
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}{where}{order} LIMIT {DEFAULT_LIMIT}").fetchall()]


def _normalize_row(row: dict[str, Any], opening_words: int) -> dict[str, str]:
    intro = _first(row, "intro", "introduction", "opening", "preview_text", "preheader", "summary", "content")
    opening = _opening(_strip_html(_text(intro)), opening_words)
    return {
        "newsletter_id": _text(_first(row, "newsletter_id", "id", "issue_id")) or "unknown",
        "opening": opening,
        "normalized_opening": _normalize_text(opening),
    }


def _groups(pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: list[set[str]] = []
    for pair in pairs:
        ids = set(pair["affected_newsletter_ids"])
        matches = [group for group in grouped if group & ids]
        if matches:
            merged = ids | set().union(*matches)
            grouped = [group for group in grouped if group not in matches]
            grouped.append(merged)
        else:
            grouped.append(ids)
    output = []
    for group in grouped:
        group_pairs = [pair for pair in pairs if set(pair["affected_newsletter_ids"]) <= group]
        output.append(
            {
                "affected_newsletter_ids": sorted(group),
                "pair_count": len(group_pairs),
                "max_similarity": max(pair["similarity"] for pair in group_pairs),
                "risk_level": _risk_level(max(pair["similarity"] for pair in group_pairs)),
            }
        )
    return sorted(output, key=lambda group: (-group["max_similarity"], group["affected_newsletter_ids"]))


def _risk_level(score: float) -> str:
    if score >= 0.95:
        return "high"
    if score >= 0.88:
        return "medium"
    return "low"


def _shared_prefix(left: str, right: str) -> str:
    words = []
    for left_word, right_word in zip(left.split(), right.split()):
        if left_word != right_word:
            break
        words.append(left_word)
    return " ".join(words[:12])


def _opening(text: str, opening_words: int) -> str:
    return " ".join(text.split()[:opening_words])


def _strip_html(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", " ", value.lower())).strip()


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
