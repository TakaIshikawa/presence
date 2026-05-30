"""Report planned content topic concentration risks."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import timedelta
from typing import Any

from ._batch_report_common import *


ARTIFACT_TYPE = "content_calendar_topic_concentration"
DEFAULT_WINDOW_DAYS = 7
DEFAULT_MAX_TOPIC_ITEMS = 3
DEFAULT_LIMIT = 100


def build_content_calendar_topic_concentration_report(
    rows: list[dict[str, Any]],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    max_topic_items: int = DEFAULT_MAX_TOPIC_ITEMS,
    required_categories: str | list[str] | None = None,
    limit: int = DEFAULT_LIMIT,
    now=None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    positive("window_days", window_days)
    positive("max_topic_items", max_topic_items)
    positive("limit", limit)
    gen = now_value(now)
    planned = sorted([_item(r) for r in rows if _item(r)["planned_at_dt"]], key=lambda r: (r["planned_at_dt"], r["item_id"]))
    issues: list[dict[str, Any]] = []
    for start in planned:
        end = start["planned_at_dt"] + timedelta(days=window_days)
        bucket = [item for item in planned if start["planned_at_dt"] <= item["planned_at_dt"] < end]
        by_topic: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in bucket:
            if item["topic"]:
                by_topic[item["topic"].lower()].append(item)
        for topic, items in by_topic.items():
            if len(items) > max_topic_items and start["item_id"] == items[0]["item_id"]:
                ids = [item["item_id"] for item in items]
                issues.append(
                    {
                        "issue_type": "topic_concentration",
                        "topic": topic,
                        "category": None,
                        "item_ids": ids,
                        "window_start": start["planned_at_dt"].date().isoformat(),
                        "window_end": end.date().isoformat(),
                        "evidence": f"{len(items)} planned items for {topic} in {window_days} days",
                        "severity": "high" if len(items) >= max_topic_items * 2 else "medium",
                        "recommendation": "Spread this topic across more calendar space or replace near-duplicate planned items.",
                    }
                )
    cats = [_norm(c) for c in (required_categories if isinstance(required_categories, list) else clean(required_categories).split(",")) if _norm(c)]
    planned_cats = {_norm(item["category"]) for item in planned if _norm(item["category"])}
    for category in sorted(set(cats) - planned_cats):
        issues.append(
            {
                "issue_type": "missing_category_coverage",
                "topic": None,
                "category": category,
                "item_ids": [],
                "window_start": None,
                "window_end": None,
                "evidence": f"no planned content for required category {category}",
                "severity": "medium",
                "recommendation": "Add at least one planned item for this required content category.",
            }
        )
    issues.sort(key=lambda i: (-{"high": 3, "medium": 2, "low": 1}[i["severity"]], i["issue_type"], i["topic"] or i["category"] or ""))
    shown = issues[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {"window_days": window_days, "max_topic_items": max_topic_items, "required_categories": cats, "limit": limit},
        "summary": {"planned_count": len(planned), "issue_count": len(issues), "shown_count": len(shown), "topic_count": len({i["topic"].lower() for i in planned if i["topic"]}), "category_counts": dict(sorted(Counter(_norm(i["category"]) for i in planned if _norm(i["category"])).items()))},
        "issues": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v},
        "empty_state": empty_state(issues, "No content calendar topic concentration findings.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_content_calendar_topic_concentration_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = next((t for t in ("content_calendar", "planned_content", "content_plan") if t in sch), None)
    if not table:
        return build_content_calendar_topic_concentration_report([], missing_tables=["content_calendar|planned_content|content_plan"], **kwargs)
    cols = sch[table]
    miss = []
    if not {"planned_at", "scheduled_at", "publish_at", "date"} & cols:
        miss.append("planned_at|scheduled_at|publish_at|date")
    if not {"topic", "theme", "title"} & cols:
        miss.append("topic|theme|title")
    if miss:
        return build_content_calendar_topic_concentration_report([], missing_columns={table: miss}, **kwargs)
    rows = load_table(conn, table, cols, {"item_id": ("id", "item_id", "slug"), "planned_at": ("planned_at", "scheduled_at", "publish_at", "date"), "topic": ("topic", "theme", "title"), "category": ("category", "content_category", "type"), "status": ("status", "state")})
    rows = [r for r in rows if lower(r.get("status"), "planned") not in {"published", "done", "cancelled", "canceled"}]
    return build_content_calendar_topic_concentration_report(rows, **kwargs)


def format_content_calendar_topic_concentration_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_content_calendar_topic_concentration_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = ["Content Calendar Topic Concentration", f"Generated: {report['generated_at']}", f"Totals: planned={s['planned_count']} issues={s['issue_count']} topics={s['topic_count']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["issues"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "severity | issue_type | topic/category | evidence | recommendation"]
    for item in report["issues"]:
        lines.append(f"{item['severity']} | {item['issue_type']} | {item['topic'] or item['category']} | {item['evidence']} | {item['recommendation']}")
    return "\n".join(lines)


def _item(row: dict[str, Any]) -> dict[str, Any]:
    planned_at = row.get("planned_at") or row.get("scheduled_at") or row.get("publish_at") or row.get("date")
    return {"item_id": clean(row.get("item_id") or row.get("id") or row.get("slug"), "unknown"), "planned_at": clean(planned_at), "planned_at_dt": dt(planned_at), "topic": clean(row.get("topic") or row.get("theme") or row.get("title")), "category": clean(row.get("category") or row.get("content_category") or row.get("type"))}


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", clean(value).lower())
