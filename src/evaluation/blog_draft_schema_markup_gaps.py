"""Find publishable blog drafts missing JSON-LD schema metadata."""
from __future__ import annotations
from collections import Counter
from typing import Any
from urllib.parse import urlparse
from ._batch_report_common import *

ARTIFACT_TYPE = "blog_draft_schema_markup_gaps"
DEFAULT_LIMIT = 50
VALID_SCHEMA = {"article", "blogposting", "newsarticle", "techarticle"}
PUBLISHABLE = {"publishable", "ready", "approved", "scheduled", "published"}


def _meta(row: dict[str, Any]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for key in ("metadata", "schema_json", "structured_data", "frontmatter"):
        raw = row.get(key)
        if not raw:
            continue
        if isinstance(raw, dict):
            data.update(raw)
            continue
        try:
            parsed = json.loads(str(raw))
        except (TypeError, ValueError):
            continue
        if isinstance(parsed, dict):
            data.update(parsed)
    return data


def _field(row: dict[str, Any], meta: dict[str, Any], *names: str) -> str:
    for name in names:
        value = row.get(name)
        if clean(value):
            return clean(value)
        value = meta.get(name)
        if clean(value):
            return clean(value)
    return ""


def _publishable(row: dict[str, Any]) -> bool:
    status = lower(row.get("status") or row.get("publication_status") or row.get("state"))
    if status:
        return status in PUBLISHABLE
    return bool(row.get("publishable") in (1, True, "1", "true", "yes"))


def _url_ok(value: str) -> bool:
    p = urlparse(value)
    return p.scheme in {"http", "https"} and bool(p.netloc)


def build_blog_draft_schema_markup_gaps_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, missing_tables=None, missing_columns=None, now=None):
    positive("limit", limit)
    findings = []
    for row in rows:
        if not _publishable(row):
            continue
        meta = _meta(row)
        draft_id = row.get("draft_id") or row.get("id")
        title = _field(row, meta, "title", "headline", "name")
        canonical = _field(row, meta, "canonical_url", "url", "canonical")
        published = _field(row, meta, "published_at", "datePublished", "date_published", "published")
        author = _field(row, meta, "author", "author_name", "byline")
        image = _field(row, meta, "image", "image_url", "social_image", "og_image")
        schema_type = _field(row, meta, "schema_type", "@type", "type")
        reasons = []
        if not schema_type:
            reasons.append("missing_schema_type")
        elif schema_type.lower() not in VALID_SCHEMA:
            reasons.append("malformed_schema_type")
        if not canonical:
            reasons.append("missing_canonical_url")
        elif not _url_ok(canonical):
            reasons.append("malformed_canonical_url")
        if not title:
            reasons.append("missing_title")
        if not published:
            reasons.append("missing_datePublished")
        elif not dt(published):
            reasons.append("malformed_datePublished")
        if not author:
            reasons.append("missing_author")
        if not image:
            reasons.append("missing_image")
        elif not _url_ok(image):
            reasons.append("malformed_image")
        if reasons:
            severity = "high" if len(reasons) >= 4 or any(r.startswith("malformed") for r in reasons) else ("medium" if len(reasons) >= 2 else "low")
            findings.append({"draft_id": draft_id, "title": title or clean(row.get("title"), "Untitled"), "severity": severity, "reason": reasons[0], "reasons": reasons, "recommended_fix": "populate Article/BlogPosting JSON-LD fields before publication"})
    rank = {"high": 0, "medium": 1, "low": 2}
    findings.sort(key=lambda f: (rank[f["severity"]], str(f["draft_id"])))
    shown = findings[:limit]
    by = Counter(f["severity"] for f in findings)
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_iso(now), "filters": {"limit": limit}, "summary": {"draft_count": len(rows), "publishable_count": sum(1 for r in rows if _publishable(r)), "gap_count": len(findings), "shown": len(shown), "by_severity": dict(sorted(by.items()))}, "schema_gaps": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(findings, "No blog draft schema markup gaps found.", schema_gap=bool(missing_tables or missing_columns))}


def build_blog_draft_schema_markup_gaps_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; rows = []
    table = "blog_drafts" if "blog_drafts" in s else ("blog_posts" if "blog_posts" in s else None)
    if not table:
        mt.append("blog_drafts|blog_posts")
    else:
        c = s[table]
        useful = {"status", "publication_status", "state", "publishable", "schema_type", "canonical_url", "title", "headline", "published_at", "datePublished", "author", "image", "metadata", "schema_json", "structured_data", "frontmatter"}
        if not (useful & c):
            mc[table] = sorted(useful)
        rows = load_table(conn, table, c, {"draft_id": ("id", "draft_id", "post_id"), "title": ("title", "headline"), "headline": ("headline",), "status": ("status", "publication_status", "state"), "publishable": ("publishable",), "schema_type": ("schema_type",), "canonical_url": ("canonical_url", "url", "canonical"), "published_at": ("published_at", "datePublished", "date_published"), "author": ("author", "author_name", "byline"), "image": ("image", "image_url", "social_image", "og_image"), "metadata": ("metadata", "meta_json"), "schema_json": ("schema_json", "structured_data"), "frontmatter": ("frontmatter",)})
    return build_blog_draft_schema_markup_gaps_report(rows, missing_tables=mt, missing_columns=mc, **kw)


def format_blog_draft_schema_markup_gaps_json(report): return json_dumps(report)


def format_blog_draft_schema_markup_gaps_text(report):
    s = report["summary"]
    lines = ["Blog Draft Schema Markup Gaps", f"Generated: {report['generated_at']}", f"Totals: drafts={s['draft_count']} publishable={s['publishable_count']} gaps={s['gap_count']} shown={s['shown']}"]
    if report["missing_tables"]: lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]: lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["schema_gaps"]:
        lines.append(report["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "draft_id | title | severity | reason"]
    for f in report["schema_gaps"]:
        lines.append(f"{f['draft_id']} | {f['title']} | {f['severity']} | {', '.join(f['reasons'])}")
    return "\n".join(lines)
