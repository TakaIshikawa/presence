"""Evaluate newsletter link anchor text quality."""
from __future__ import annotations

from collections import defaultdict
from typing import Any
import re

from ._batch_report_common import (
    clean,
    connection,
    domain,
    empty_state,
    flatten_missing,
    json_dumps,
    load_table,
    now_value,
    schema,
)

ARTIFACT_TYPE = "newsletter_link_anchor_text_quality"
DEFAULT_LIMIT = 100
DEFAULT_MAX_ANCHOR_LENGTH = 80
GENERIC_ANCHORS = {
    "click here",
    "here",
    "read more",
    "learn more",
    "more",
    "link",
    "this link",
    "view",
    "open",
}
DOMAIN_RE = re.compile(r"\b(?:[a-z0-9-]+\.)+[a-z]{2,}\b", re.I)


def build_newsletter_link_anchor_text_quality_report(
    rows: list[dict[str, Any]],
    *,
    max_anchor_length: int = DEFAULT_MAX_ANCHOR_LENGTH,
    limit: int = DEFAULT_LIMIT,
    now=None,
    missing_tables=None,
    missing_columns=None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    if max_anchor_length <= 0:
        raise ValueError("max_anchor_length must be positive")
    gen = now_value(now)
    links = [_link(row) for row in rows]
    issues: list[dict[str, Any]] = []
    duplicate_domains = _duplicate_anchor_domains(links)
    for link in links:
        anchor = link["anchor_text"]
        anchor_key = _anchor_key(anchor)
        issue_codes: list[str] = []
        if _is_bare_url(anchor):
            issue_codes.append("bare_url_anchor")
        if anchor_key in GENERIC_ANCHORS:
            issue_codes.append("generic_anchor")
        if len(anchor) > max_anchor_length:
            issue_codes.append("overly_long_anchor")
        if _anchor_domain_mismatch(anchor, link["link_url"]):
            issue_codes.append("anchor_domain_mismatch")
        if anchor_key and len(duplicate_domains.get((link["issue_id"], anchor_key), set())) > 1:
            issue_codes.append("duplicate_anchor_cross_domain")
        for code in issue_codes:
            issues.append(
                {
                    "issue_id": link["issue_id"],
                    "link_url": link["link_url"],
                    "anchor_text": anchor,
                    "issue_code": code,
                    "severity": _severity(code),
                    "suggested_anchor": _suggested_anchor(link, code),
                }
            )
    issues.sort(key=lambda row: (clean(row["issue_id"]), clean(row["link_url"]), clean(row["issue_code"])))
    shown = issues[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {"max_anchor_length": max_anchor_length, "limit": limit},
        "summary": {"link_count": len(links), "issue_count": len(issues), "shown_count": len(shown)},
        "issues": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v},
        "empty_state": empty_state(
            issues,
            "No newsletter link anchor text quality issues found.",
            schema_gap=bool(missing_tables or missing_columns),
        ),
    }


def build_newsletter_link_anchor_text_quality_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    table = next((name for name in ("newsletter_link_inventory", "newsletter_links", "newsletter_issue_links") if name in sch), None)
    rows: list[dict[str, Any]] = []
    if not table:
        missing_tables.append("newsletter_link_inventory|newsletter_links|newsletter_issue_links")
    else:
        cols = sch[table]
        missing = []
        if not ({"issue_id", "newsletter_issue_id", "campaign_id"} & cols):
            missing.append("issue_id|newsletter_issue_id|campaign_id")
        if not ({"url", "link_url", "destination_url"} & cols):
            missing.append("url|link_url|destination_url")
        if not ({"link_text", "anchor_text", "text", "label"} & cols):
            missing.append("link_text|anchor_text|text|label")
        if missing:
            missing_columns[table] = missing
        else:
            rows = load_table(
                conn,
                table,
                cols,
                {
                    "issue_id": ("issue_id", "newsletter_issue_id", "campaign_id"),
                    "link_url": ("url", "link_url", "destination_url"),
                    "anchor_text": ("link_text", "anchor_text", "text", "label"),
                },
            )
    return build_newsletter_link_anchor_text_quality_report(
        rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_newsletter_link_anchor_text_quality_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_newsletter_link_anchor_text_quality_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Newsletter Link Anchor Text Quality",
        f"Generated: {report['generated_at']}",
        f"Totals: links={summary['link_count']} issues={summary['issue_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["issues"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("")
    lines.append("issue_id | severity | code | anchor | url | suggested_anchor")
    for issue in report["issues"]:
        lines.append(
            f"{issue['issue_id']} | {issue['severity']} | {issue['issue_code']} | "
            f"{issue['anchor_text']} | {issue['link_url']} | {issue['suggested_anchor']}"
        )
    return "\n".join(lines)


def _link(row: dict[str, Any]) -> dict[str, str]:
    return {
        "issue_id": clean(row.get("issue_id") or row.get("newsletter_issue_id") or row.get("campaign_id")),
        "link_url": clean(row.get("link_url") or row.get("url") or row.get("destination_url")),
        "anchor_text": clean(row.get("anchor_text") or row.get("link_text") or row.get("text") or row.get("label")),
    }


def _duplicate_anchor_domains(links: list[dict[str, str]]) -> dict[tuple[str, str], set[str]]:
    grouped: dict[tuple[str, str], set[str]] = defaultdict(set)
    for link in links:
        anchor = _anchor_key(link["anchor_text"])
        if anchor:
            grouped[(link["issue_id"], anchor)].add(domain(link["link_url"]))
    return grouped


def _anchor_key(anchor: str) -> str:
    return re.sub(r"\s+", " ", anchor.lower()).strip()


def _is_bare_url(anchor: str) -> bool:
    text = anchor.strip()
    if not text or " " in text:
        return False
    return bool(text.startswith(("http://", "https://")) or DOMAIN_RE.fullmatch(text))


def _anchor_domain_mismatch(anchor: str, url: str) -> bool:
    destination = domain(url)
    mentioned = [domain(match.group(0)) for match in DOMAIN_RE.finditer(anchor)]
    return bool(destination and mentioned and destination not in mentioned)


def _severity(code: str) -> str:
    return "high" if code in {"anchor_domain_mismatch", "duplicate_anchor_cross_domain"} else "medium"


def _suggested_anchor(link: dict[str, str], code: str) -> str:
    host = domain(link["link_url"]) or "destination"
    if code == "overly_long_anchor":
        return f"Concise description for {host}"
    if code == "duplicate_anchor_cross_domain":
        return f"Specific label for {host}"
    return f"Descriptive text for {host}"
