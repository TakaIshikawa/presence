"""Report reply drafts with weak question balance."""
from __future__ import annotations

from collections import Counter
from typing import Any
import re

from ._batch_report_common import clean, connection, empty_state, flatten_missing, json_dumps, now_value, pick, positive, schema


ARTIFACT_TYPE = "reply_draft_question_balance"
DEFAULT_MAX_QUESTIONS = 1
DEFAULT_LIMIT = 100
TABLES = ("reply_drafts", "reply_queue", "reply_draft_queue")
DRAFT_COLUMNS = ("draft_text", "content", "body", "reply_text")


def build_reply_draft_question_balance_report(
    rows: list[dict[str, Any]],
    *,
    max_questions: int = DEFAULT_MAX_QUESTIONS,
    limit: int = DEFAULT_LIMIT,
    now: Any = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    positive("max_questions", max_questions)
    positive("limit", limit)
    generated_at = now_value(now)
    findings: list[dict[str, Any]] = []
    for row in rows:
        text = clean(row.get("draft_text"))
        question_count = text.count("?")
        issue_codes = _issue_codes(text, question_count, max_questions)
        if not issue_codes:
            continue
        findings.append(
            {
                "draft_id": row.get("draft_id") or row.get("id"),
                "target_author": clean(row.get("target_author")) or None,
                "target_id": clean(row.get("target_id")) or None,
                "question_count": question_count,
                "issue_codes": issue_codes,
                "recommendation": _recommendation(issue_codes),
            }
        )
    findings.sort(key=lambda item: (-len(item["issue_codes"]), -item["question_count"], _sort_id(item["draft_id"])))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"max_questions": max_questions, "limit": limit},
        "summary": {
            "draft_count": len(rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "issue_counts": dict(sorted(Counter(code for item in findings for code in item["issue_codes"]).items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items()) if cols},
        "empty_state": empty_state(findings, "No reply draft question balance issues found.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_reply_draft_question_balance_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    table = next((name for name in TABLES if name in db_schema), None)
    if not table:
        return build_reply_draft_question_balance_report([], missing_tables=["reply_drafts|reply_queue"], **kwargs)
    cols = db_schema[table]
    missing = []
    if "id" not in cols:
        missing.append("id")
    if not set(DRAFT_COLUMNS) & cols:
        missing.append("|".join(DRAFT_COLUMNS))
    rows = [] if missing else _load_rows(conn, table, cols)
    return build_reply_draft_question_balance_report(rows, missing_columns={table: missing} if missing else None, **kwargs)


def format_reply_draft_question_balance_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_reply_draft_question_balance_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = [
        "Reply Draft Question Balance",
        f"Generated: {report['generated_at']}",
        f"Totals: drafts={s['draft_count']} findings={s['finding_count']} shown={s['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "draft_id | target_author | target_id | questions | issues | recommendation"])
    for item in report["findings"]:
        lines.append(
            f"{item['draft_id']} | {item['target_author'] or '-'} | {item['target_id'] or '-'} | "
            f"{item['question_count']} | {', '.join(item['issue_codes'])} | {item['recommendation']}"
        )
    return "\n".join(lines)


def _load_rows(conn: Any, table: str, cols: set[str]) -> list[dict[str, Any]]:
    select = [
        pick(cols, "id", out="draft_id"),
        pick(cols, *DRAFT_COLUMNS, out="draft_text"),
        pick(cols, "target_author", "author_username", "screen_name", out="target_author"),
        pick(cols, "target_id", "target_post_id", "mention_id", "in_reply_to_id", out="target_id"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY id ASC")]


def _issue_codes(text: str, question_count: int, max_questions: int) -> list[str]:
    codes = []
    if question_count and _is_question_only(text):
        codes.append("question_only")
    if question_count > max_questions:
        codes.append("excessive_questions")
    if question_count and not _answer_before_question(text):
        codes.append("no_answer_before_question")
    return codes


def _is_question_only(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    without_questions = re.sub(r"[^A-Za-z0-9]+", " ", stripped.replace("?", " ")).strip()
    return stripped.endswith("?") and bool(without_questions) and len(re.split(r"[.!]", stripped.rstrip("?"))) <= 1


def _answer_before_question(text: str) -> bool:
    before = text.split("?", 1)[0]
    return len(re.findall(r"\b[\w']+\b", before)) >= 5 and bool(re.search(r"[.!:;]", before))


def _recommendation(codes: list[str]) -> str:
    if "question_only" in codes:
        return "Add a direct answer or useful context before asking a follow-up question."
    if "no_answer_before_question" in codes:
        return "Lead with answer content, then ask at most one focused follow-up question."
    return "Reduce the number of questions and keep only the most useful follow-up."


def _sort_id(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, clean(value))
