"""Find inbound question-like mentions without a queued, approved, or posted reply."""
from __future__ import annotations

import re
from typing import Any

from ._batch_report_common import *

ARTIFACT_TYPE = "reply_unanswered_question_gaps"
DEFAULT_MIN_AGE_HOURS = 24
QUESTION_RE = re.compile(r"\b(who|what|when|where|why|how|can|could|would|should|do|does|did|is|are|will|anyone|help)\b", re.I)
ANSWERED_STATUSES = {"approved", "queued", "posted", "published", "sent"}


def build_reply_unanswered_question_gaps_report(
    mention_rows: list[dict[str, Any]],
    reply_rows: list[dict[str, Any]] | None = None,
    *,
    min_age_hours: int = DEFAULT_MIN_AGE_HOURS,
    platform: str | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now=None,
) -> dict[str, Any]:
    positive("min_age_hours", min_age_hours)
    generated = now_value(now)
    platform_filter = lower(platform)
    answered = _answered_mentions(reply_rows or [])
    findings: list[dict[str, Any]] = []
    for row in mention_rows:
        mention_id = clean(row.get("mention_id") or row.get("id") or row.get("inbound_id") or row.get("inbound_tweet_id"))
        row_platform = clean(row.get("platform"), "unknown")
        if platform_filter and lower(row_platform) != platform_filter:
            continue
        received = dt(row.get("received_at") or row.get("created_at") or row.get("inbound_created_at"))
        age_hours = round((generated - received).total_seconds() / 3600, 2) if received else None
        if age_hours is not None and age_hours < min_age_hours:
            continue
        signal = _question_signal(row.get("text") or row.get("body") or row.get("inbound_text") or row.get("content"))
        if not signal:
            continue
        keys = {mention_id, clean(row.get("url") or row.get("inbound_url")), clean(row.get("cid") or row.get("inbound_cid"))}
        if answered & {k for k in keys if k}:
            continue
        findings.append(
            {
                "mention_id": mention_id or "unknown",
                "author": clean(row.get("author") or row.get("author_handle") or row.get("inbound_author_handle"), "unknown"),
                "received_at": received.isoformat() if received else clean(row.get("received_at") or row.get("created_at")),
                "age_hours": age_hours,
                "platform": row_platform,
                "question_signal": signal,
            }
        )
    findings.sort(key=lambda f: (-(f["age_hours"] or 0), f["platform"], f["mention_id"]))
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated.isoformat(),
        "filters": {"min_age_hours": min_age_hours, "platform": platform},
        "totals": {"mentions": len(mention_rows), "replies": len(reply_rows or []), "findings": len(findings)},
        "findings": findings,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
        "empty_state": empty_state(findings, "No unanswered question gaps found.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_reply_unanswered_question_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    s = schema(conn)
    missing_tables: list[str] = []
    mentions: list[dict[str, Any]] = []
    replies: list[dict[str, Any]] = []
    mtable = "inbound_mentions" if "inbound_mentions" in s else "mentions" if "mentions" in s else None
    if not mtable:
        missing_tables.append("inbound_mentions")
    else:
        mentions = load_table(conn, mtable, s[mtable], {
            "mention_id": ("mention_id", "id", "inbound_id", "tweet_id"),
            "author": ("author", "author_handle", "inbound_author_handle"),
            "text": ("text", "body", "content", "inbound_text"),
            "received_at": ("received_at", "created_at", "inbound_created_at"),
            "platform": ("platform", "network"),
            "url": ("url", "inbound_url"),
            "cid": ("cid", "inbound_cid"),
        })
    if "reply_queue" in s:
        replies.extend(_load_replies(conn, "reply_queue", s["reply_queue"]))
    if "reply_drafts" in s:
        replies.extend(_load_replies(conn, "reply_drafts", s["reply_drafts"]))
    if "replies" in s:
        replies.extend(_load_replies(conn, "replies", s["replies"]))
    return build_reply_unanswered_question_gaps_report(mentions, replies, missing_tables=missing_tables, **kwargs)


def format_reply_unanswered_question_gaps_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_reply_unanswered_question_gaps_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Unanswered Question Gaps",
        f"Generated: {report['generated_at']}",
        f"Totals: mentions={report['totals']['mentions']} replies={report['totals']['replies']} findings={report['totals']['findings']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "mention_id | platform | author | age_hours | question_signal"]
    for f in report["findings"]:
        lines.append(f"{f['mention_id']} | {f['platform']} | {f['author']} | {f['age_hours']} | {f['question_signal']}")
    return "\n".join(lines)


def _question_signal(value: Any) -> str | None:
    text_value = clean(value)
    if not text_value:
        return None
    if "?" in text_value:
        return "question_mark"
    match = QUESTION_RE.search(text_value)
    return f"interrogative:{match.group(1).lower()}" if match else None


def _answered_mentions(rows: list[dict[str, Any]]) -> set[str]:
    answered: set[str] = set()
    for row in rows:
        status = lower(row.get("status") or row.get("state"))
        if status not in ANSWERED_STATUSES and not row.get("posted_at") and not row.get("approved_at") and not row.get("queued_at"):
            continue
        for key in ("mention_id", "target_mention_id", "inbound_tweet_id", "inbound_id", "inbound_url", "inbound_cid"):
            value = clean(row.get(key))
            if value:
                answered.add(value)
    return answered


def _load_replies(conn: Any, table: str, cols: set[str]) -> list[dict[str, Any]]:
    return load_table(conn, table, cols, {
        "status": ("status", "state"),
        "mention_id": ("mention_id", "target_mention_id"),
        "inbound_tweet_id": ("inbound_tweet_id", "inbound_id"),
        "inbound_url": ("inbound_url", "target_url"),
        "inbound_cid": ("inbound_cid", "target_cid"),
        "posted_at": ("posted_at", "published_at", "sent_at"),
        "approved_at": ("approved_at",),
        "queued_at": ("queued_at", "scheduled_at"),
    })
