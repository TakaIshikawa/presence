"""Report publishable blog drafts with too few internal links."""
from __future__ import annotations

from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse
import re

from ._batch_report_common import *


ARTIFACT_TYPE = "blog_draft_internal_link_gaps"
DEFAULT_MIN_INTERNAL_LINKS = 2
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 50
DEFAULT_ALLOWED_DOMAINS = ("example.com",)
_MD_LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")


class _HrefParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(value)


def build_blog_draft_internal_link_gaps_report(
    rows: list[dict[str, Any]],
    *,
    min_internal_links: int = DEFAULT_MIN_INTERNAL_LINKS,
    allowed_domains: list[str] | tuple[str, ...] = DEFAULT_ALLOWED_DOMAINS,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now=None,
) -> dict[str, Any]:
    positive("min_internal_links", min_internal_links)
    positive("lookback_days", lookback_days)
    positive("limit", limit)
    gen = now_value(now)
    cutoff = gen - timedelta(days=lookback_days)
    domains = {_norm_domain(d) for d in allowed_domains if _norm_domain(d)}
    gaps: list[dict[str, Any]] = []
    eligible = 0
    for row in rows:
        if not _eligible(row):
            continue
        updated = dt(row.get("updated_at") or row.get("created_at") or row.get("drafted_at"))
        if updated and updated < cutoff:
            continue
        eligible += 1
        links = extract_links(row.get("body") or row.get("content") or row.get("markdown") or row.get("html") or "")
        internal = sum(1 for url in links if _is_internal(url, domains))
        external = sum(1 for url in links if _is_external(url, domains))
        if internal < min_internal_links:
            missing = min_internal_links - internal
            gaps.append(
                {
                    "draft_id": clean(row.get("id") or row.get("draft_id")),
                    "title": clean(row.get("title")),
                    "slug": clean(row.get("slug")),
                    "internal_link_count": internal,
                    "external_link_count": external,
                    "missing_link_reason": f"needs {missing} more internal link{'s' if missing != 1 else ''}",
                }
            )
    gaps.sort(key=lambda item: (item["internal_link_count"], -item["external_link_count"], item["draft_id"]))
    shown = gaps[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {"min_internal_links": min_internal_links, "allowed_domains": sorted(domains), "lookback_days": lookback_days, "limit": limit},
        "summary": {"eligible_drafts": eligible, "gap_count": len(gaps), "shown": len(shown)},
        "gaps": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
        "empty_state": empty_state(gaps, "No blog draft internal link gaps found.", schema_gap=bool(missing_tables or missing_columns)),
    }


def extract_links(text: Any) -> list[str]:
    body = clean(text)
    parser = _HrefParser()
    try:
        parser.feed(body)
    except Exception:
        pass
    return [clean(u) for u in [*_MD_LINK_RE.findall(body), *parser.hrefs] if clean(u) and not clean(u).startswith("#")]


def build_blog_draft_internal_link_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    if "blog_drafts" not in sch:
        return build_blog_draft_internal_link_gaps_report([], missing_tables=["blog_drafts"], **kwargs)
    cols = sch["blog_drafts"]
    missing: dict[str, list[str]] = {}
    if not ({"body", "content", "markdown", "html"} & cols):
        missing["blog_drafts"] = ["body|content|markdown|html"]
    if missing:
        return build_blog_draft_internal_link_gaps_report([], missing_columns=missing, **kwargs)
    rows = load_table(
        conn,
        "blog_drafts",
        cols,
        {
            "id": ("id", "draft_id"),
            "title": ("title",),
            "slug": ("slug",),
            "body": ("body", "content", "markdown", "html"),
            "status": ("status", "state"),
            "publishable": ("publishable", "is_publishable", "ready_to_publish"),
            "updated_at": ("updated_at", "created_at", "drafted_at"),
        },
    )
    return build_blog_draft_internal_link_gaps_report(rows, **kwargs)


def format_blog_draft_internal_link_gaps_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_blog_draft_internal_link_gaps_text(report: dict[str, Any]) -> str:
    lines = ["Blog Draft Internal Link Gaps", f"Generated: {report['generated_at']}", f"Totals: eligible={report['summary']['eligible_drafts']} gaps={report['summary']['gap_count']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["gaps"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "draft_id | title | slug | internal | external | reason"]
    for item in report["gaps"]:
        lines.append(f"{item['draft_id']} | {item['title']} | {item['slug']} | {item['internal_link_count']} | {item['external_link_count']} | {item['missing_link_reason']}")
    return "\n".join(lines)


def _eligible(row: dict[str, Any]) -> bool:
    status = lower(row.get("status") or row.get("state"))
    flag = lower(row.get("publishable") or row.get("is_publishable") or row.get("ready_to_publish"))
    return status in {"", "draft", "ready", "publishable", "approved"} and flag not in {"0", "false", "no"}


def _norm_domain(value: Any) -> str:
    return domain(value)


def _is_internal(url: str, allowed_domains: set[str]) -> bool:
    parsed = urlparse(url)
    if not parsed.scheme and not parsed.netloc:
        return url.startswith("/")
    host = domain(url)
    return bool(host and (host in allowed_domains or any(host.endswith("." + d) for d in allowed_domains)))


def _is_external(url: str, allowed_domains: set[str]) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and not _is_internal(url, allowed_domains)
