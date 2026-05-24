"""Export blog internal link opportunities."""

from __future__ import annotations

from typing import Any

from evaluation._batch_report_utils import connection, csv_rows, dump_json, first_table, json_load, pick, schema, text

FIELDS = ["source_post_id", "target_post_id", "anchor_suggestion", "shared_topics", "score", "reasons"]
DEFAULT_LIMIT = 100


def build_blog_internal_link_opportunity_export_from_db(db_or_conn: Any, *, min_score: float = 1.0, topic: str | None = None, limit: int = DEFAULT_LIMIT) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = first_table(sch, ("blog_posts", "generated_content"))
    missing_tables = [] if table else ["blog_posts|generated_content"]
    posts = _load_posts(conn, table, sch[table]) if table else []
    existing = _links(conn, sch)
    rows = []
    for source in posts:
        for target in posts:
            if source["post_id"] == target["post_id"] or (source["post_id"], target["post_id"]) in existing:
                continue
            shared = sorted(set(source["topics"]) & set(target["topics"]))
            if topic and topic not in shared:
                continue
            reasons = []
            score = 0.0
            if shared:
                score += len(shared) * 2
                reasons.append("shared_topics")
            if source.get("series") and source.get("series") == target.get("series"):
                score += 1
                reasons.append("same_series")
            if score >= min_score:
                rows.append({"source_post_id": source["post_id"], "target_post_id": target["post_id"], "anchor_suggestion": target["title"] or (shared[0] if shared else "related post"), "shared_topics": shared, "score": score, "reasons": reasons})
    rows.sort(key=lambda r: (-r["score"], str(r["source_post_id"]), str(r["target_post_id"])))
    return {"artifact_type": "blog_internal_link_opportunity_export", "filters": {"min_score": min_score, "topic": topic, "limit": limit}, "rows": rows[:limit], "missing_tables": missing_tables, "empty_state": {"is_empty": not rows, "message": "No blog internal link opportunities found." if not rows and not missing_tables else None}}


def build_blog_internal_link_opportunity_export(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return build_blog_internal_link_opportunity_export_from_db(*args, **kwargs)


def format_blog_internal_link_opportunity_export_json(export: dict[str, Any]) -> str:
    return dump_json(export)


def format_blog_internal_link_opportunity_export_csv(export: dict[str, Any]) -> str:
    return csv_rows(export["rows"], FIELDS)


def _load_posts(conn, table: str, cols: set[str]) -> list[dict[str, Any]]:
    select = [f"{pick(cols, 'id', 'post_id', default='rowid')} AS post_id", f"{pick(cols, 'title', default='NULL')} AS title", f"{pick(cols, 'topics', 'topic_tags', default='NULL')} AS topics", f"{pick(cols, 'series', 'series_id', default='NULL')} AS series"]
    rows = []
    for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY post_id ASC"):
        topics = json_load(row["topics"])
        if isinstance(topics, str):
            topics = [topics]
        if not isinstance(topics, list):
            topics = [part.strip() for part in text(row["topics"]).split(",") if part.strip()]
        rows.append({"post_id": row["post_id"], "title": row["title"], "topics": [text(t) for t in topics if text(t)], "series": row["series"]})
    return rows


def _links(conn, sch: dict[str, set[str]]) -> set[tuple[Any, Any]]:
    table = first_table(sch, ("blog_internal_links", "internal_links"))
    if not table:
        return set()
    cols = sch[table]
    if not {"source_post_id", "target_post_id"} <= cols:
        return set()
    return {(row["source_post_id"], row["target_post_id"]) for row in conn.execute(f"SELECT source_post_id, target_post_id FROM {table}")}
