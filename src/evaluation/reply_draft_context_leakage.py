"""Detect internal context leaked into reply drafts."""
from __future__ import annotations
from collections import Counter
import re
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "reply_draft_context_leakage"
DEFAULT_LIMIT = 100
_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@(internal|crm|private|corp|company)\.[A-Z]{2,}\b", re.I)
_NOTE_RE = re.compile(r"\b(internal note|private note|relationship note|do not mention|for operator|crm note)\b", re.I)
_TAG_RE = re.compile(r"\b(CRM|SEGMENT|TAG|LEAD_SCORE|LIFECYCLE)[:=][A-Za-z0-9_.-]+", re.I)
_BRACKET_RE = re.compile(r"\[(?:internal|operator|agent|do not mention|private)[^\]]+\]", re.I)


def build_reply_draft_context_leakage_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None, now=None) -> dict[str, Any]:
    positive("limit", limit); gen = now_value(now); issues = []
    for row in rows:
        rid = clean(row.get("reply_id") or row.get("id") or row.get("draft_id"))
        text = clean(row.get("body") or row.get("content") or row.get("draft"))
        for reason, evidence in _findings(text):
            issues.append({"reply_id": rid, "target_id": clean(row.get("target_id") or row.get("post_id")), "reason": reason, "evidence": evidence, "recommendation": "Remove internal context before sending the reply."})
    issues.sort(key=lambda i: (i["reply_id"], i["reason"], i["evidence"]))
    shown = issues[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"limit": limit}, "summary": {"replies": len(rows), "leak_count": len(issues), "shown": len(shown), "reason_counts": dict(sorted(Counter(i["reason"] for i in issues).items()))}, "issues": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(issues, "No reply draft context leakage found.", schema_gap=bool(missing_tables or missing_columns))}


def build_reply_draft_context_leakage_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); sch = schema(conn)
    table = next((t for t in ("reply_drafts", "replies", "reply_queue") if t in sch), None)
    if not table: return build_reply_draft_context_leakage_report([], missing_tables=["reply_drafts|replies|reply_queue"], **kwargs)
    cols = sch[table]
    if not ({"body", "content", "draft"} & cols): return build_reply_draft_context_leakage_report([], missing_columns={table: ["body|content|draft"]}, **kwargs)
    rows = load_table(conn, table, cols, {"reply_id": ("id", "reply_id", "draft_id"), "target_id": ("target_id", "post_id"), "body": ("body", "content", "draft")})
    return build_reply_draft_context_leakage_report(rows, **kwargs)


def format_reply_draft_context_leakage_json(report: dict[str, Any]) -> str: return json_dumps(report)


def format_reply_draft_context_leakage_text(report: dict[str, Any]) -> str:
    s = report["summary"]; lines = ["Reply Draft Context Leakage", f"Generated: {report['generated_at']}", f"Totals: replies={s['replies']} leaks={s['leak_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]: lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["issues"]: lines.append(report["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "reply_id | reason | evidence | recommendation"]
    for i in report["issues"]: lines.append(f"{i['reply_id']} | {i['reason']} | {i['evidence']} | {i['recommendation']}")
    return "\n".join(lines)


def _findings(text: str) -> list[tuple[str, str]]:
    checks = [("internal_note_marker", _NOTE_RE), ("crm_tag", _TAG_RE), ("private_identifier", _EMAIL_RE), ("operator_instruction", _BRACKET_RE)]
    return [(reason, m.group(0)) for reason, rx in checks for m in rx.finditer(text)]
