"""Find newsletter destination URLs with fragmented UTM tagging."""

from __future__ import annotations

from collections import defaultdict
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ._report_utils import clean, connection, expr, json_dumps, positive, schema, now_iso


ARTIFACT_TYPE = "newsletter_link_utm_fragmentation"
DEFAULT_LIMIT = 50
DEFAULT_MIN_VARIANTS = 2
UTM_KEYS = ("utm_source", "utm_medium", "utm_campaign")


def build_newsletter_link_utm_fragmentation_report(rows: list[dict[str, Any]], *, min_variants: int = DEFAULT_MIN_VARIANTS, limit: int = DEFAULT_LIMIT, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None, now: Any = None) -> dict[str, Any]:
    positive("min_variants", min_variants); positive("limit", limit)
    groups: dict[str, dict[tuple[str, str, str], list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        url = clean(row.get("url"))
        if not url:
            continue
        canonical, combo = _split_utm(url)
        groups[canonical][combo].append(row)
    findings = []
    for canonical, variants in groups.items():
        if len(variants) >= min_variants:
            findings.append({"canonical_url": canonical, "variant_count": len(variants), "occurrence_count": sum(len(v) for v in variants.values()), "utm_variants": [{"utm_source": k[0], "utm_medium": k[1], "utm_campaign": k[2], "count": len(v), "examples": [_example(x) for x in v[:3]]} for k, v in sorted(variants.items())]})
    findings.sort(key=lambda r: (-r["variant_count"], -r["occurrence_count"], r["canonical_url"]))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_iso(now), "thresholds": {"min_variants": min_variants, "limit": limit}, "summary": {"link_count": len(rows), "finding_count": len(findings), "shown_count": len(shown)}, "findings": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}}


def build_newsletter_link_utm_fragmentation_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); s = schema(conn); table = "newsletter_links"; missing_tables = [] if table in s else [table]; missing_columns = {}
    rows = []
    if table in s:
        cols = s[table]
        if "url" not in cols:
            missing_columns[table] = ["url"]
        else:
            rows = [dict(r) for r in conn.execute(f"SELECT {expr(cols,'id',default='rowid',out='link_id')}, {expr(cols,'issue_id',default='NULL',out='issue_id')}, {expr(cols,'section',default='NULL',out='section')}, {expr(cols,'variant',default='NULL',out='variant')}, url FROM {table} ORDER BY rowid")]
    return build_newsletter_link_utm_fragmentation_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_newsletter_link_utm_fragmentation_json(report: dict[str, Any]) -> str: return json_dumps(report)


def format_newsletter_link_utm_fragmentation_text(report: dict[str, Any]) -> str:
    lines = ["Newsletter Link UTM Fragmentation", f"Generated: {report['generated_at']}", f"Totals: links={report['summary']['link_count']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]: lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["findings"]: lines.append("No newsletter link UTM fragmentation found."); return "\n".join(lines)
    for r in report["findings"]: lines.append(f"- {r['canonical_url']} variants={r['variant_count']} occurrences={r['occurrence_count']}")
    return "\n".join(lines)


def _split_utm(url: str) -> tuple[str, tuple[str, str, str]]:
    p = urlsplit(url); pairs = parse_qsl(p.query, keep_blank_values=True); values = {k: v for k, v in pairs}
    canonical_q = urlencode([(k, v) for k, v in pairs if not k.lower().startswith("utm_")])
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path.rstrip("/") or "/", canonical_q, "")), tuple(values.get(k, "") for k in UTM_KEYS)  # type: ignore[return-value]


def _example(row: dict[str, Any]) -> dict[str, Any]:
    return {"link_id": clean(row.get("link_id")) or None, "issue_id": clean(row.get("issue_id")) or None, "section": clean(row.get("section")) or None, "variant": clean(row.get("variant")) or None}
