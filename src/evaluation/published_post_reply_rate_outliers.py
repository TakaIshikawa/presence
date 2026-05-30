"""Flag published posts with unusually high or low reply rates."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "published_post_reply_rate_outliers"
DEFAULT_LIMIT = 100
DEFAULT_MIN_SAMPLE = 3
DEFAULT_RATIO = 2.0


def build_published_post_reply_rate_outliers_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, min_sample: int = DEFAULT_MIN_SAMPLE, ratio: float = DEFAULT_RATIO, channel: str | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None, now=None) -> dict[str, Any]:
    positive("limit", limit); positive("min_sample", min_sample); positive("ratio", ratio)
    gen = now_value(now)
    prepared = [_prep(r) for r in rows if not channel or lower(r.get("channel")) == channel.lower()]
    groups = defaultdict(list)
    for item in prepared:
        groups[(item["channel"], item["content_type"])].append(item)
    baselines = []
    outliers = []
    for (chan, ctype), items in groups.items():
        sample = len(items); baseline = round(sum(i["reply_rate"] for i in items) / sample, 6) if sample else 0.0
        baselines.append({"channel": chan, "content_type": ctype, "sample_size": sample, "baseline_reply_rate": baseline})
        if sample < min_sample or baseline <= 0:
            continue
        for item in items:
            high = item["reply_rate"] >= baseline * ratio
            low = item["reply_rate"] <= baseline / ratio
            if high or low:
                outliers.append({**item, "baseline_reply_rate": baseline, "direction": "high" if high else "low", "ratio_to_baseline": round(item["reply_rate"] / baseline, 4), "recommendation": "Review audience fit, prompt wording, and moderation context for this post."})
    outliers.sort(key=lambda i: (i["channel"], i["content_type"], i["direction"], -abs(i["ratio_to_baseline"] - 1), i["post_id"]))
    shown = outliers[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"limit": limit, "min_sample": min_sample, "ratio": ratio, "channel": channel}, "summary": {"posts": len(prepared), "baseline_groups": len(baselines), "outlier_count": len(outliers), "shown": len(shown)}, "baselines": sorted(baselines, key=lambda b: (b["channel"], b["content_type"])), "outliers": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(outliers, "No published post reply rate outliers found.", schema_gap=bool(missing_tables or missing_columns))}


def build_published_post_reply_rate_outliers_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); sch = schema(conn)
    table = next((t for t in ("published_posts", "posts", "generated_content") if t in sch), None)
    if not table: return build_published_post_reply_rate_outliers_report([], missing_tables=["published_posts|posts|generated_content"], **kwargs)
    cols = sch[table]
    if not ({"reply_count", "replies"} & cols) or not ({"impressions", "engagements", "engagement_total"} & cols):
        return build_published_post_reply_rate_outliers_report([], missing_columns={table: ["reply_count|replies", "impressions|engagements|engagement_total"]}, **kwargs)
    rows = load_table(conn, table, cols, {"post_id": ("id", "post_id", "content_id"), "channel": ("channel", "platform"), "content_type": ("content_type", "type"), "reply_count": ("reply_count", "replies"), "impressions": ("impressions", "engagements", "engagement_total")})
    return build_published_post_reply_rate_outliers_report(rows, **kwargs)


def format_published_post_reply_rate_outliers_json(report: dict[str, Any]) -> str: return json_dumps(report)


def format_published_post_reply_rate_outliers_text(report: dict[str, Any]) -> str:
    s = report["summary"]; lines = ["Published Post Reply Rate Outliers", f"Generated: {report['generated_at']}", f"Totals: posts={s['posts']} groups={s['baseline_groups']} outliers={s['outlier_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]: lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["outliers"]: lines.append(report["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "post_id | channel | content_type | direction | reply_rate | baseline | recommendation"]
    for i in report["outliers"]: lines.append(f"{i['post_id']} | {i['channel']} | {i['content_type']} | {i['direction']} | {i['reply_rate']} | {i['baseline_reply_rate']} | {i['recommendation']}")
    return "\n".join(lines)


def _prep(row: dict[str, Any]) -> dict[str, Any]:
    replies = to_int(row.get("reply_count") or row.get("replies"), 0)
    denom = to_int(row.get("impressions") or row.get("engagements") or row.get("engagement_total"), 0)
    rate = round(replies / denom, 6) if denom > 0 else 0.0
    return {"post_id": clean(row.get("post_id") or row.get("id") or row.get("content_id")), "channel": clean(row.get("channel") or row.get("platform"), "unknown"), "content_type": clean(row.get("content_type") or row.get("type"), "unknown"), "reply_count": replies, "impressions": denom, "reply_rate": rate}
