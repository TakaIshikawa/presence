"""Flag reply drafts that appear to leave inbound questions unanswered."""
from __future__ import annotations

import re
from typing import Any

from ._batch_report_common import *


ARTIFACT_TYPE = "reply_draft_unanswered_question_gaps"
DEFAULT_LIMIT = 50
DETAIL_TERMS = re.compile(r"\b(link|url|source|details|example|where|which|how|why|what|when)\b", re.I)
LINK_RE = re.compile(r"https?://|\bwww\.", re.I)


def build_reply_draft_unanswered_question_gaps_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now=None,
) -> dict[str, Any]:
    positive("limit", limit)
    gen = now_value(now)
    findings: list[dict[str, Any]] = []
    for row in rows:
        inbound = clean(row.get("inbound_text") or row.get("target_text") or row.get("mention_text"))
        reply = clean(row.get("reply_text") or row.get("draft_text") or row.get("content"))
        questions = _questions(inbound)
        if not questions:
            continue
        base = {
            "mention_id": clean(row.get("mention_id") or row.get("inbound_id") or row.get("inbound_tweet_id")),
            "reply_id": clean(row.get("reply_id") or row.get("id")),
            "evidence_snippet": _snippet(inbound),
        }
        if len(questions) > 1 and _thin(reply):
            findings.append({**base, "issue_code": "multiple_questions_thin_reply", "severity": "medium"})
        if _link_request(inbound) and not LINK_RE.search(reply):
            findings.append({**base, "issue_code": "link_or_detail_request_missing", "severity": "high"})
        if not _answers(inbound, reply):
            findings.append({**base, "issue_code": "question_appears_unanswered", "severity": "high"})
    findings.sort(key=lambda f: (f["mention_id"], f["reply_id"], f["issue_code"]))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {"limit": limit},
        "summary": {
            "reply_count": len(rows),
            "question_reply_count": sum(1 for row in rows if _questions(clean(row.get("inbound_text") or row.get("target_text") or row.get("mention_text")))),
            "finding_count": len(findings),
            "shown": len(shown),
            "issue_counts": dict(sorted(Counter(f["issue_code"] for f in findings).items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
        "empty_state": empty_state(findings, "No reply draft unanswered question gaps found.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_reply_draft_unanswered_question_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = next((t for t in ("reply_drafts", "reply_queue") if t in sch), None)
    if not table:
        return build_reply_draft_unanswered_question_gaps_report([], missing_tables=["reply_drafts|reply_queue"], **kwargs)
    cols = sch[table]
    missing: dict[str, list[str]] = {}
    if not ({"inbound_text", "target_text", "mention_text"} & cols):
        missing[table] = ["inbound_text|target_text|mention_text"]
    if not ({"reply_text", "draft_text", "content"} & cols):
        missing.setdefault(table, []).append("reply_text|draft_text|content")
    if missing:
        return build_reply_draft_unanswered_question_gaps_report([], missing_columns=missing, **kwargs)
    rows = load_table(
        conn,
        table,
        cols,
        {
            "reply_id": ("id", "reply_id"),
            "mention_id": ("mention_id", "inbound_id", "inbound_tweet_id"),
            "inbound_text": ("inbound_text", "target_text", "mention_text"),
            "reply_text": ("reply_text", "draft_text", "content"),
        },
    )
    return build_reply_draft_unanswered_question_gaps_report(rows, **kwargs)


def format_reply_draft_unanswered_question_gaps_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_reply_draft_unanswered_question_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Reply Draft Unanswered Question Gaps",
        f"Generated: {report['generated_at']}",
        f"Totals: replies={summary['reply_count']} question_replies={summary['question_reply_count']} findings={summary['finding_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "mention_id | reply_id | issue_code | severity | evidence"]
    for item in report["findings"]:
        lines.append(f"{item['mention_id']} | {item['reply_id']} | {item['issue_code']} | {item['severity']} | {item['evidence_snippet']}")
    return "\n".join(lines)


def _questions(text: str) -> list[str]:
    return [p.strip() for p in re.split(r"(?<=[?])\s+", text) if "?" in p or re.match(r"(?i)^(how|why|what|where|when|which|can|could|do|does|is|are)\b", p.strip())]


def _answers(inbound: str, reply: str) -> bool:
    if _thin(reply):
        return False
    return bool(tokens(inbound) & tokens(reply))


def _thin(reply: str) -> bool:
    return len(tokens(reply)) < 4 or lower(reply) in {"thanks", "thank you", "great question", "good question"}


def _link_request(text: str) -> bool:
    return bool(DETAIL_TERMS.search(text)) and ("?" in text or re.search(r"(?i)\b(can|could|please|share|send)\b", text))


def _snippet(text: str) -> str:
    return text[:160]
