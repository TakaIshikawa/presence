"""Find fenced code blocks in blog drafts with missing, invalid, or mismatched language labels."""
from __future__ import annotations

from typing import Any

from ._batch_report_common import *


ARTIFACT_TYPE = "blog_draft_code_block_language_gaps"
DEFAULT_ALLOWED_LANGUAGES = "python,javascript,typescript,bash,sh,json,yaml,html,css,sql,markdown,go,rust"
DEFAULT_LIMIT = 100
FENCE_RE = re.compile(r"```([^\n`]*)\n(.*?)```", re.S)


def build_blog_draft_code_block_language_gaps_report(
    rows: list[dict[str, Any]],
    *,
    allowed_languages: str | list[str] = DEFAULT_ALLOWED_LANGUAGES,
    draft_id=None,
    limit: int = DEFAULT_LIMIT,
    now=None,
    missing_tables=None,
    missing_columns=None,
) -> dict[str, Any]:
    positive("limit", limit)
    allowed = {lower(x) for x in (allowed_languages if isinstance(allowed_languages, list) else clean(allowed_languages).split(",")) if lower(x)}
    gen = now_value(now)
    issues = []
    for row in rows:
        did = row.get("draft_id") or row.get("id")
        if draft_id is not None and str(did) != str(draft_id):
            continue
        body = clean(row.get("body") or row.get("content") or row.get("markdown"))
        for idx, match in enumerate(FENCE_RE.finditer(body), 1):
            raw = clean(match.group(1))
            lang = lower(raw.split()[0] if raw else "")
            code = match.group(2).strip()
            reason = None
            severity = "medium"
            evidence = ""
            recommendation = ""
            if not lang:
                reason = "missing_language"
                evidence = _snippet(code)
                recommendation = "Add a language identifier to the fenced code block."
            elif allowed and lang not in allowed:
                reason = "unsupported_language"
                evidence = f"language tag `{lang}` is not in the allowed list"
                recommendation = "Use a supported language tag or add this language to the allowed list."
            else:
                likely = _likely_language(code)
                if likely and lang != likely and not (lang == "sh" and likely == "bash"):
                    reason = "likely_language_mismatch"
                    severity = "low"
                    evidence = f"tag `{lang}` looks like `{likely}` based on code content"
                    recommendation = "Change the fence language tag or verify the code sample content."
            if reason:
                issues.append(
                    {
                        "draft_id": did,
                        "block_index": idx,
                        "language": lang or None,
                        "issue_type": reason,
                        "evidence": evidence,
                        "severity": severity,
                        "recommendation": recommendation,
                    }
                )
    issues.sort(key=lambda item: (_sid(item["draft_id"]), item["block_index"], item["issue_type"]))
    shown = issues[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {"allowed_languages": sorted(allowed), "draft_id": draft_id, "limit": limit},
        "summary": {"draft_count": len(rows), "issue_count": len(issues), "shown_count": len(shown), "issue_counts": dict(sorted(Counter(i["issue_type"] for i in issues).items()))},
        "issues": shown,
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v},
        "empty_state": empty_state(issues, "No blog draft code block language gaps found.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_blog_draft_code_block_language_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    if "blog_drafts" not in sch:
        return build_blog_draft_code_block_language_gaps_report([], missing_tables=["blog_drafts"], **kwargs)
    cols = sch["blog_drafts"]
    miss = []
    if not {"id", "draft_id", "slug"} & cols:
        miss.append("id|draft_id|slug")
    if not {"body", "content", "markdown"} & cols:
        miss.append("body|content|markdown")
    if miss:
        return build_blog_draft_code_block_language_gaps_report([], missing_columns={"blog_drafts": miss}, **kwargs)
    rows = load_table(conn, "blog_drafts", cols, {"draft_id": ("id", "draft_id", "slug"), "body": ("body", "content", "markdown")})
    return build_blog_draft_code_block_language_gaps_report(rows, **kwargs)


def format_blog_draft_code_block_language_gaps_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_blog_draft_code_block_language_gaps_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = ["Blog Draft Code Block Language Gaps", f"Generated: {report['generated_at']}", f"Totals: drafts={s['draft_count']} issues={s['issue_count']} shown={s['shown_count']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["issues"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "draft_id | block | severity | issue_type | language | evidence | recommendation"]
    for issue in report["issues"]:
        lines.append(f"{issue['draft_id']} | {issue['block_index']} | {issue['severity']} | {issue['issue_type']} | {issue['language'] or '-'} | {issue['evidence']} | {issue['recommendation']}")
    return "\n".join(lines)


def _likely_language(code: str) -> str | None:
    text = code.strip()
    if re.search(r"^\s*(SELECT|INSERT|UPDATE|DELETE)\b", text, re.I):
        return "sql"
    if re.search(r"\b(def|import|print)\b|if __name__ == ['\"]__main__['\"]", text):
        return "python"
    if re.search(r"\b(const|let|function)\b|=>|console\.log", text):
        return "javascript"
    if re.search(r"^\s*(curl|grep|echo|cd|export)\b", text, re.M):
        return "bash"
    if text.startswith("{") or text.startswith("["):
        try:
            json.loads(text)
        except Exception:
            pass
        else:
            return "json"
    if re.search(r"<[a-z][\w-]*(\s|>)", text, re.I):
        return "html"
    return None


def _snippet(code: str) -> str:
    return re.sub(r"\s+", " ", code.strip())[:80]


def _sid(value: Any):
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, clean(value))
