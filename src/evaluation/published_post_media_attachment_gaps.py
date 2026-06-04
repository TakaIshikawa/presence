"""Find published posts that appear to expect media but have no attachments."""
from __future__ import annotations
from typing import Any
import re
from ._batch_report_common import *

ARTIFACT_TYPE = "published_post_media_attachment_gaps"
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 50
DEFAULT_MARKERS = ("![", "<img", "image:", "video:", "media_expected", "hero_image", "thumbnail")

def build_published_post_media_attachment_gaps_report(rows: list[dict[str, Any]], *, platform: str | None = None, lookback_days: int = DEFAULT_LOOKBACK_DAYS, expected_media_markers: tuple[str, ...] | list[str] = DEFAULT_MARKERS, limit: int = DEFAULT_LIMIT, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None, now=None) -> dict[str, Any]:
    positive("lookback_days", lookback_days); positive("limit", limit)
    gen = now_value(now); cutoff = gen - timedelta(days=lookback_days); plat = lower(platform)
    gaps = []; scanned = 0
    for row in rows:
        row_platform = lower(row.get("platform"))
        if plat and row_platform != plat: continue
        if lower(row.get("status")) not in {"published", "sent", "posted"}: continue
        published = dt(row.get("published_at") or row.get("posted_at") or row.get("created_at"))
        if published and published < cutoff: continue
        scanned += 1
        evidence = _media_evidence(row, expected_media_markers)
        attachments = to_int(row.get("attachment_count") or row.get("media_attachment_count") or row.get("media_count"), 0)
        if evidence and attachments <= 0:
            gaps.append({"post_id": clean(row.get("id") or row.get("post_id") or row.get("content_id")), "platform": row_platform or "unknown", "published_at": published.isoformat() if published else clean(row.get("published_at")), "marker_evidence": evidence, "attachment_count": attachments})
    gaps.sort(key=lambda i: (i["platform"], i["post_id"]))
    shown = gaps[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"platform": platform, "lookback_days": lookback_days, "expected_media_markers": list(expected_media_markers), "limit": limit}, "summary": {"published_posts_scanned": scanned, "gap_count": len(gaps), "shown": len(shown)}, "gaps": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(gaps, "No published post media attachment gaps found.", schema_gap=bool(missing_tables or missing_columns))}

def build_published_post_media_attachment_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); sch = schema(conn)
    if "published_posts" not in sch: return build_published_post_media_attachment_gaps_report([], missing_tables=["published_posts"], **kwargs)
    cols = sch["published_posts"]
    rows = load_table(conn, "published_posts", cols, {"id": ("id","post_id","content_id"), "platform": ("platform","channel"), "status": ("status",), "published_at": ("published_at","posted_at","created_at"), "body": ("body","content","text","html","markdown"), "metadata": ("metadata","meta","payload"), "attachment_count": ("attachment_count","media_attachment_count","media_count")})
    return build_published_post_media_attachment_gaps_report(rows, **kwargs)

def format_published_post_media_attachment_gaps_json(report: dict[str, Any]) -> str: return json_dumps(report)
def format_published_post_media_attachment_gaps_text(report: dict[str, Any]) -> str:
    lines = ["Published Post Media Attachment Gaps", f"Generated: {report['generated_at']}", f"Totals: scanned={report['summary']['published_posts_scanned']} gaps={report['summary']['gap_count']}"]
    if not report["gaps"]: lines.append(report["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "post_id | platform | published_at | evidence | attachments"]
    lines += [f"{g['post_id']} | {g['platform']} | {g['published_at']} | {', '.join(g['marker_evidence'])} | {g['attachment_count']}" for g in report["gaps"]]
    return "\n".join(lines)

def _media_evidence(row: dict[str, Any], markers: tuple[str, ...] | list[str]) -> list[str]:
    text = lower(" ".join(clean(row.get(k)) for k in ("body", "content", "metadata")))
    return [m for m in markers if lower(m) in text or (m.endswith(":") and re.search(rf"\b{re.escape(m[:-1].lower())}\b", text))]
