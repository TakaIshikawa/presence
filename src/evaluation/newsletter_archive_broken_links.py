"""Report broken newsletter archive links from stored content and crawl results."""
from __future__ import annotations

from collections import Counter
from typing import Any
from urllib.parse import urljoin, urlparse

from ._batch_report_common import *


ARTIFACT_TYPE = "newsletter_archive_broken_links"
DEFAULT_LIMIT = 100
DEFAULT_BAD_STATUS_CODES = "400,401,403,404,410,429,500,502,503,504"
URL_RE = re.compile(r"https?://[^\s<>)\"']+|href=[\"']([^\"']+)[\"']|\[[^\]]+\]\(([^)\s]+)\)", re.I)


def build_newsletter_archive_broken_links_report(
    archive_rows: list[dict[str, Any]],
    crawl_result_rows: list[dict[str, Any]] | None = None,
    *,
    bad_status_codes: str | list[int] = DEFAULT_BAD_STATUS_CODES,
    limit: int = DEFAULT_LIMIT,
    now=None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    positive("limit", limit)
    bad_codes = {int(c) for c in (bad_status_codes if isinstance(bad_status_codes, list) else clean(bad_status_codes).split(",")) if str(c).strip()}
    gen = now_value(now)
    crawled = {_norm_url(r.get("url") or r.get("link_url")): r for r in (crawl_result_rows or []) if _norm_url(r.get("url") or r.get("link_url"))}
    archives = [_archive(r) for r in archive_rows]
    archive_urls = {_norm_url(a["archive_url"]) for a in archives if _norm_url(a["archive_url"])}
    issues: list[dict[str, Any]] = []
    for archive in archives:
        if not archive["canonical_url"]:
            issues.append(_issue(archive, None, None, "missing_canonical_url", "high", "archive has no canonical URL", "Add a canonical URL for the archived newsletter."))
        for link in archive["links"]:
            normalized = _norm_url(urljoin(archive["archive_url"] or "", link))
            parsed = urlparse(normalized)
            if archive_urls and _looks_internal(normalized, archive_urls) and normalized not in archive_urls:
                issues.append(_issue(archive, normalized, None, "broken_internal_archive_link", "high", f"internal archive link {normalized} is not in supplied archives", "Update or remove the internal archive link."))
            result = crawled.get(normalized)
            status = to_int(result.get("status_code") or result.get("status"), 0) if result else 0
            if result and status in bad_codes:
                issues.append(_issue(archive, normalized, status, "bad_crawl_status", "high" if status in {404, 410, 500, 502, 503, 504} else "medium", f"crawl result returned status {status}", "Replace the URL or repair the destination before publishing archive links."))
            elif parsed.scheme in {"http", "https"} and result and lower(result.get("ok")) in {"false", "0", "no"}:
                issues.append(_issue(archive, normalized, status or None, "failed_crawl_result", "medium", "crawl result marked the URL as failed", "Review the stored crawl result and repair the destination."))
    issues.sort(key=lambda i: (-{"high": 3, "medium": 2, "low": 1}[i["severity"]], i["archive_id"], i["url"] or "", i["issue_type"]))
    shown = issues[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {"bad_status_codes": sorted(bad_codes), "limit": limit},
        "summary": {"archive_count": len(archives), "crawl_result_count": len(crawled), "issue_count": len(issues), "shown_count": len(shown), "issue_counts": dict(sorted(Counter(i["issue_type"] for i in issues).items()))},
        "issues": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v},
        "empty_state": empty_state(issues, "No newsletter archive broken links found.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_newsletter_archive_broken_links_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    archive_table = next((t for t in ("newsletter_archives", "newsletter_archive", "newsletter_archive_manifest", "newsletter_sends") if t in sch), None)
    if not archive_table:
        return build_newsletter_archive_broken_links_report([], missing_tables=["newsletter_archives|newsletter_archive|newsletter_archive_manifest|newsletter_sends"], **kwargs)
    cols = sch[archive_table]
    miss = []
    if not {"archive_url", "url", "published_url"} & cols:
        miss.append("archive_url|url|published_url")
    if not {"body", "html", "content", "markdown", "text"} & cols:
        miss.append("body|html|content|markdown|text")
    if miss:
        return build_newsletter_archive_broken_links_report([], missing_columns={archive_table: miss}, **kwargs)
    archives = load_table(conn, archive_table, cols, {"archive_id": ("id", "archive_id", "issue_id", "send_id"), "archive_url": ("archive_url", "url", "published_url"), "canonical_url": ("canonical_url", "canonical"), "content": ("body", "html", "content", "markdown", "text")})
    crawl_table = next((t for t in ("newsletter_archive_crawl_results", "link_crawl_results", "crawl_results") if t in sch), None)
    crawls = []
    if crawl_table:
        c = sch[crawl_table]
        crawls = load_table(conn, crawl_table, c, {"url": ("url", "link_url"), "status_code": ("status_code", "status"), "ok": ("ok", "success")})
    return build_newsletter_archive_broken_links_report(archives, crawls, **kwargs)


def format_newsletter_archive_broken_links_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_newsletter_archive_broken_links_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = ["Newsletter Archive Broken Links", f"Generated: {report['generated_at']}", f"Totals: archives={s['archive_count']} crawl_results={s['crawl_result_count']} issues={s['issue_count']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["issues"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "archive_id | severity | issue_type | url | status | evidence | recommendation"]
    for item in report["issues"]:
        lines.append(f"{item['archive_id']} | {item['severity']} | {item['issue_type']} | {item['url'] or '-'} | {item['status'] or '-'} | {item['evidence']} | {item['recommendation']}")
    return "\n".join(lines)


def _archive(row: dict[str, Any]) -> dict[str, Any]:
    content = clean(row.get("content") or row.get("body") or row.get("html") or row.get("markdown") or row.get("text"))
    return {"archive_id": clean(row.get("archive_id") or row.get("id") or row.get("issue_id"), "unknown"), "archive_url": clean(row.get("archive_url") or row.get("url") or row.get("published_url")), "canonical_url": clean(row.get("canonical_url") or row.get("canonical")), "links": _links(content)}


def _links(content: str) -> list[str]:
    links: list[str] = []
    for match in URL_RE.finditer(content):
        links.append(next((g for g in match.groups() if g), match.group(0)))
    return sorted(set(link.strip() for link in links if link and not link.startswith("#") and not link.startswith("mailto:")))


def _issue(archive: dict[str, Any], url: str | None, status: int | None, issue_type: str, severity: str, evidence: str, recommendation: str) -> dict[str, Any]:
    return {"archive_id": archive["archive_id"], "archive_url": archive["archive_url"], "url": url, "status": status, "issue_type": issue_type, "severity": severity, "evidence": evidence, "recommendation": recommendation}


def _norm_url(value: Any) -> str:
    return clean(value).rstrip("/").lower()


def _looks_internal(url: str, archive_urls: set[str]) -> bool:
    host = urlparse(url).hostname
    return bool(host and any(urlparse(a).hostname == host for a in archive_urls))
