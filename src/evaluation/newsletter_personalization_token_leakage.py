"""Detect unresolved personalization tokens in newsletter drafts."""
from __future__ import annotations

from collections import Counter, defaultdict
import re
from typing import Any

from ._batch_report_common import *


ARTIFACT_TYPE = "newsletter_personalization_token_leakage"
DEFAULT_LIMIT = 100
_TOKEN_RE = re.compile(r"(?<!\\)(\{\{\s*[\w. -]+\s*\}\}|%\s*[A-Z][A-Z0-9_ -]+\s*%|\[\[\s*[\w. -]+\s*\]\])")
_MALFORMED_RE = re.compile(r"(?<!\\)(\{\{[^}\n]{1,80}(?!\}\})|(?<!\[)\[[\w. -]{2,80}\]\](?!\])|%[A-Z][A-Z0-9_ -]{1,80}(?!%))")
_FENCE_RE = re.compile(r"```.*?```", re.S)
_INLINE_CODE_RE = re.compile(r"`[^`]*`")


def build_newsletter_personalization_token_leakage_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now=None,
) -> dict[str, Any]:
    positive("limit", limit)
    gen = now_value(now)
    issues = []
    for row in rows:
        nid = clean(row.get("newsletter_id") or row.get("id") or row.get("draft_id"))
        title = clean(row.get("title") or row.get("name") or row.get("subject"))
        for field in ("subject", "preheader", "body"):
            for token, issue_type in _tokens(row.get(field)):
                issues.append(
                    {
                        "newsletter_id": nid,
                        "title": title,
                        "field": field,
                        "token": token,
                        "issue_type": issue_type,
                        "recommendation": "Resolve or escape this personalization token before sending.",
                    }
                )
    issues.sort(key=lambda i: (i["newsletter_id"], i["field"], i["token"]))
    shown = issues[:limit]
    grouped = defaultdict(list)
    for item in shown:
        grouped[item["newsletter_id"]].append(item)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {"limit": limit},
        "summary": {
            "newsletters": len(rows),
            "leakage_count": len(issues),
            "shown": len(shown),
            "issue_counts": dict(sorted(Counter(i["issue_type"] for i in issues).items())),
        },
        "issues": shown,
        "issues_by_newsletter": dict(sorted(grouped.items())),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
        "empty_state": empty_state(issues, "No newsletter personalization token leakage found.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_newsletter_personalization_token_leakage_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = next((t for t in ("newsletter_drafts", "newsletters", "newsletter_issues") if t in sch), None)
    if not table:
        return build_newsletter_personalization_token_leakage_report([], missing_tables=["newsletter_drafts|newsletters|newsletter_issues"], **kwargs)
    cols = sch[table]
    if not ({"subject", "preheader", "body", "content", "html"} & cols):
        return build_newsletter_personalization_token_leakage_report([], missing_columns={table: ["subject|preheader|body|content|html"]}, **kwargs)
    rows = load_table(conn, table, cols, {
        "newsletter_id": ("id", "newsletter_id", "draft_id"),
        "title": ("title", "name", "subject"),
        "subject": ("subject",),
        "preheader": ("preheader", "preview_text"),
        "body": ("body", "content", "html"),
    })
    return build_newsletter_personalization_token_leakage_report(rows, **kwargs)


def format_newsletter_personalization_token_leakage_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_newsletter_personalization_token_leakage_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = ["Newsletter Personalization Token Leakage", f"Generated: {report['generated_at']}", f"Totals: newsletters={s['newsletters']} leakage={s['leakage_count']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["issues"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "newsletter_id | field | token | issue_type | recommendation"]
    for item in report["issues"]:
        lines.append(f"{item['newsletter_id']} | {item['field']} | {item['token']} | {item['issue_type']} | {item['recommendation']}")
    return "\n".join(lines)


def _tokens(value: Any) -> list[tuple[str, str]]:
    text = _INLINE_CODE_RE.sub(" ", _FENCE_RE.sub(" ", clean(value)))
    found = [(m.group(1).strip(), "unresolved_token") for m in _TOKEN_RE.finditer(text)]
    spans = [m.span(1) for m in _TOKEN_RE.finditer(text)]
    for m in _MALFORMED_RE.finditer(text):
        if any(a <= m.start() < b for a, b in spans):
            continue
        found.append((m.group(1).strip(), "malformed_token"))
    return found
