"""Score generated content for clear call-to-action language."""
from __future__ import annotations
from collections import Counter
import re
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "generated_content_cta_clarity"
DEFAULT_LIMIT = 100
_LINK_RE = re.compile(r"https?://\S+|\[[^\]]+\]\((https?://[^)]+)\)")
_CTA_PATTERNS = (
    r"\b(read|download|register|subscribe|reply|book|schedule|join|start|visit|learn|watch|save|share)\b",
    r"\bsign up\b", r"\bget started\b", r"\btell us\b",
)
_VAGUE_RE = re.compile(r"\b(check it out|click here|learn more|read more|see more)\b", re.I)


def build_generated_content_cta_clarity_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None, now=None) -> dict[str, Any]:
    positive("limit", limit); gen = now_value(now); items = []
    for row in rows:
        text = clean(row.get("content") or row.get("body") or row.get("snippet"))
        ctas = _ctas(text); issues = _issues(text, ctas); score = max(0, 100 - 25 * len(issues))
        items.append({"content_id": clean(row.get("content_id") or row.get("id") or row.get("draft_id")), "content_type": clean(row.get("content_type") or row.get("type"), "generated"), "clarity_score": score, "cta_phrases": ctas, "issue_reasons": issues, "recommendation": _recommendation(issues)})
    findings = [i for i in items if i["issue_reasons"]]
    findings.sort(key=lambda i: (i["clarity_score"], i["content_id"]))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"limit": limit}, "summary": {"items": len(rows), "issue_items": len(findings), "shown": len(shown), "average_clarity_score": round(sum(i["clarity_score"] for i in items) / len(items), 2) if items else 0.0, "reason_counts": dict(sorted(Counter(r for i in findings for r in i["issue_reasons"]).items()))}, "items": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(findings, "No generated content CTA clarity issues found.", schema_gap=bool(missing_tables or missing_columns))}


def build_generated_content_cta_clarity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); sch = schema(conn)
    if "generated_content" not in sch:
        return build_generated_content_cta_clarity_report([], missing_tables=["generated_content"], **kwargs)
    cols = sch["generated_content"]
    if not ({"content", "body", "snippet"} & cols):
        return build_generated_content_cta_clarity_report([], missing_columns={"generated_content": ["content|body|snippet"]}, **kwargs)
    rows = load_table(conn, "generated_content", cols, {"content_id": ("id", "content_id", "draft_id"), "content_type": ("content_type", "type"), "content": ("content", "body", "snippet")})
    return build_generated_content_cta_clarity_report(rows, **kwargs)


def format_generated_content_cta_clarity_json(report: dict[str, Any]) -> str: return json_dumps(report)


def format_generated_content_cta_clarity_text(report: dict[str, Any]) -> str:
    s = report["summary"]; lines = ["Generated Content CTA Clarity", f"Generated: {report['generated_at']}", f"Totals: items={s['items']} issue_items={s['issue_items']} avg_score={s['average_clarity_score']}"]
    if report["missing_tables"]: lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]: lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["items"]: lines.append(report["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "content_id | score | reasons | ctas | recommendation"]
    for i in report["items"]: lines.append(f"{i['content_id']} | {i['clarity_score']} | {', '.join(i['issue_reasons'])} | {', '.join(i['cta_phrases']) or '-'} | {i['recommendation']}")
    return "\n".join(lines)


def _ctas(text: str) -> list[str]:
    found = []
    for pat in _CTA_PATTERNS:
        found += [m.group(0).lower() for m in re.finditer(pat, text, re.I)]
    return sorted(set(found))


def _issues(text: str, ctas: list[str]) -> list[str]:
    issues = []
    if not ctas: issues.append("missing_cta")
    if len(ctas) > 1: issues.append("multiple_competing_ctas")
    if _VAGUE_RE.search(text): issues.append("vague_cta")
    if _LINK_RE.search(text) and not ctas: issues.append("missing_destination_context")
    return issues


def _recommendation(issues: list[str]) -> str:
    if not issues: return "CTA is clear."
    if "multiple_competing_ctas" in issues: return "Keep one primary CTA and remove competing asks."
    if "vague_cta" in issues: return "Replace vague CTA text with a specific action and destination."
    return "Add one specific CTA that explains what the reader will get."
