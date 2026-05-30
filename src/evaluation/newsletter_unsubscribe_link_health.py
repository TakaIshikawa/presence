"""Audit newsletter drafts for unsubscribe link health."""
from __future__ import annotations
from collections import Counter
import re
from typing import Any
from urllib.parse import urlparse
from ._batch_report_common import *

ARTIFACT_TYPE = "newsletter_unsubscribe_link_health"
DEFAULT_LIMIT = 100
_MD_LINK_RE = re.compile(r"\[([^\]]*unsubscribe[^\]]*)\]\(([^)\s]+)\)", re.I)
_A_RE = re.compile(r"<a\b[^>]*href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>", re.I | re.S)
_PLACEHOLDER_RE = re.compile(r"(example\.com|localhost|unsubscribe_url|unsub_url|replace-me|placeholder|%UNSUB|\{\{.*unsub)", re.I)


def build_newsletter_unsubscribe_link_health_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None, now=None) -> dict[str, Any]:
    positive("limit", limit)
    gen = now_value(now)
    issues = []
    for row in rows:
        nid = clean(row.get("newsletter_id") or row.get("id") or row.get("draft_id"))
        title = clean(row.get("title") or row.get("name") or row.get("subject"))
        urls = sorted(set(_urls(row.get("body"))))
        for issue_type in _issue_types(urls):
            issues.append({"newsletter_id": nid, "title": title, "detected_urls": urls, "issue_type": issue_type, "severity": _severity(issue_type), "recommendation": _recommendation(issue_type)})
    issues.sort(key=lambda i: (i["newsletter_id"], i["issue_type"]))
    shown = issues[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"limit": limit}, "summary": {"newsletters": len(rows), "issue_count": len(issues), "shown": len(shown), "issue_counts": dict(sorted(Counter(i["issue_type"] for i in issues).items()))}, "issues": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(issues, "No newsletter unsubscribe link health issues found.", schema_gap=bool(missing_tables or missing_columns))}


def build_newsletter_unsubscribe_link_health_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); sch = schema(conn)
    table = next((t for t in ("newsletter_drafts", "newsletters", "newsletter_issues") if t in sch), None)
    if not table:
        return build_newsletter_unsubscribe_link_health_report([], missing_tables=["newsletter_drafts|newsletters|newsletter_issues"], **kwargs)
    cols = sch[table]
    if not ({"body", "content", "html"} & cols):
        return build_newsletter_unsubscribe_link_health_report([], missing_columns={table: ["body|content|html"]}, **kwargs)
    rows = load_table(conn, table, cols, {"newsletter_id": ("id", "newsletter_id", "draft_id"), "title": ("title", "name", "subject"), "body": ("body", "content", "html")})
    return build_newsletter_unsubscribe_link_health_report(rows, **kwargs)


def format_newsletter_unsubscribe_link_health_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_newsletter_unsubscribe_link_health_text(report: dict[str, Any]) -> str:
    s = report["summary"]; lines = ["Newsletter Unsubscribe Link Health", f"Generated: {report['generated_at']}", f"Totals: newsletters={s['newsletters']} issues={s['issue_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]: lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["issues"]:
        lines.append(report["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "newsletter_id | issue_type | severity | detected_urls | recommendation"]
    for i in report["issues"]:
        lines.append(f"{i['newsletter_id']} | {i['issue_type']} | {i['severity']} | {', '.join(i['detected_urls']) or '-'} | {i['recommendation']}")
    return "\n".join(lines)


def _urls(value: Any) -> list[str]:
    text = clean(value)
    urls = [url.strip() for label, url in _MD_LINK_RE.findall(text) if "unsubscribe" in label.lower() or "unsub" in url.lower()]
    urls += [url.strip() for url, label in _A_RE.findall(text) if "unsubscribe" in re.sub("<[^>]+>", "", label).lower() or "unsub" in url.lower()]
    return urls


def _issue_types(urls: list[str]) -> list[str]:
    if not urls: return ["missing_unsubscribe_link"]
    issues = []
    if any(urlparse(u).scheme.lower() != "https" for u in urls): issues.append("non_https_unsubscribe_url")
    if any(_PLACEHOLDER_RE.search(u) for u in urls): issues.append("placeholder_unsubscribe_url")
    hosts_paths = {((urlparse(u).hostname or "").lower(), urlparse(u).path.rstrip("/")) for u in urls}
    if len(hosts_paths) > 1: issues.append("conflicting_unsubscribe_destinations")
    return issues


def _severity(issue_type: str) -> str:
    return "high" if issue_type in {"missing_unsubscribe_link", "placeholder_unsubscribe_url"} else "medium"


def _recommendation(issue_type: str) -> str:
    if issue_type == "missing_unsubscribe_link": return "Add one clear unsubscribe link to the draft footer."
    if issue_type == "non_https_unsubscribe_url": return "Use an HTTPS unsubscribe URL."
    if issue_type == "placeholder_unsubscribe_url": return "Replace placeholder unsubscribe URLs with production destinations."
    return "Use one canonical unsubscribe destination for this draft."
