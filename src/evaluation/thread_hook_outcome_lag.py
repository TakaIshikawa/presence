"""Report thread hook styles with stale or missing outcome feedback."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any

from evaluation.thread_hook_performance import classify_hook_style, extract_thread_opening


DEFAULT_DAYS = 90
DEFAULT_STALE_AFTER_DAYS = 21
DEFAULT_MIN_POSTS = 1


def build_thread_hook_outcome_lag_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    stale_after_days: int = DEFAULT_STALE_AFTER_DAYS,
    min_posts: int = DEFAULT_MIN_POSTS,
    now: datetime | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if stale_after_days < 0:
        raise ValueError("stale_after_days must be non-negative")
    if min_posts <= 0:
        raise ValueError("min_posts must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    posts = _load_thread_posts(conn, schema, cutoff, generated_at)
    metrics = _latest_metrics(conn, schema)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for post in posts:
        opening = extract_thread_opening(post["content"])
        if not opening:
            continue
        post["opening"] = opening
        post["style"] = classify_hook_style(opening)
        post["metric"] = metrics.get(post["id"])
        grouped[post["style"]].append(post)

    styles = []
    for style, rows in grouped.items():
        if len(rows) < min_posts:
            continue
        metric_rows = [row["metric"] for row in rows if row["metric"]]
        latest = max((metric["fetched_at"] for metric in metric_rows if metric["fetched_at"]), default=None)
        latest_age = round((generated_at - latest).total_seconds() / 86400, 2) if latest else None
        if not metric_rows:
            status = "missing"
        elif latest_age is not None and latest_age > stale_after_days:
            status = "stale"
        else:
            status = "current"
        styles.append(
            {
                "style": style,
                "post_count": len(rows),
                "metric_count": len(metric_rows),
                "missing_metric_count": len(rows) - len(metric_rows),
                "latest_outcome_at": latest.isoformat() if latest else None,
                "latest_outcome_age_days": latest_age,
                "status": status,
                "examples": [
                    {
                        "content_id": row["id"],
                        "opening": row["opening"],
                        "published_at": row["published_at"].isoformat() if row["published_at"] else None,
                    }
                    for row in sorted(rows, key=lambda item: item["published_at"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)[:3]
                ],
            }
        )
    styles.sort(key=lambda item: (_status_rank(item["status"]), -item["post_count"], item["style"]))
    return {
        "artifact_type": "thread_hook_outcome_lag",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "stale_after_days": stale_after_days, "min_posts": min_posts},
        "totals": {
            "style_count": len(styles),
            "post_count": sum(item["post_count"] for item in styles),
            "metric_count": sum(item["metric_count"] for item in styles),
            "missing_style_count": sum(1 for item in styles if item["status"] == "missing"),
            "stale_style_count": sum(1 for item in styles if item["status"] == "stale"),
            "current_style_count": sum(1 for item in styles if item["status"] == "current"),
        },
        "styles": styles,
        "attention_styles": [item for item in styles if item["status"] in {"missing", "stale"}],
        "missing_tables": [table for table in ("generated_content",) if table not in schema],
        "missing_optional_tables": [table for table in ("post_engagement", "bluesky_engagement") if table not in schema],
    }


def format_thread_hook_outcome_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_thread_hook_outcome_lag_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Thread Hook Outcome Lag",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} stale_after_days={report['filters']['stale_after_days']} min_posts={report['filters']['min_posts']}",
        (
            "Totals: "
            f"styles={totals['style_count']} posts={totals['post_count']} metrics={totals['metric_count']} "
            f"missing={totals['missing_style_count']} stale={totals['stale_style_count']} current={totals['current_style_count']}"
        ),
        "",
        "Styles:",
        "status    posts  metrics  latest                style",
    ]
    if not report["styles"]:
        lines.append("No hook styles met the filters.")
        return "\n".join(lines)
    for item in report["styles"]:
        lines.append(
            f"{item['status']:<8}  {item['post_count']:>5}  {item['metric_count']:>7}  "
            f"{(item['latest_outcome_at'] or '-')[:19]:19}  {item['style']}"
        )
    return "\n".join(lines)


def _load_thread_posts(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime, now: datetime) -> list[dict[str, Any]]:
    columns = schema.get("generated_content", set())
    if not {"id", "content_type", "content"}.issubset(columns):
        return []
    published_expr = "published_at" if "published_at" in columns else "created_at"
    published_filter = "AND COALESCE(published, 0) = 1" if "published" in columns else ""
    rows = conn.execute(
        f"""SELECT id, content, {published_expr} AS published_at
           FROM generated_content
           WHERE LOWER(content_type) LIKE '%thread%'
             {published_filter}
             AND datetime({published_expr}) >= datetime(?)
             AND datetime({published_expr}) <= datetime(?)""",
        (cutoff.isoformat(), now.isoformat()),
    ).fetchall()
    return [{"id": int(row["id"]), "content": str(row["content"] or ""), "published_at": _parse_dt(row["published_at"])} for row in rows]


def _latest_metrics(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, dict[str, Any]]:
    latest: dict[int, dict[str, Any]] = {}
    for table in ("post_engagement", "bluesky_engagement"):
        columns = schema.get(table, set())
        if "content_id" not in columns:
            continue
        date_col = "fetched_at" if "fetched_at" in columns else "created_at" if "created_at" in columns else None
        if not date_col:
            continue
        for row in conn.execute(f"SELECT content_id, {date_col} AS fetched_at FROM {table} WHERE content_id IS NOT NULL").fetchall():
            fetched_at = _parse_dt(row["fetched_at"])
            content_id = int(row["content_id"])
            if fetched_at and (content_id not in latest or fetched_at > latest[content_id]["fetched_at"]):
                latest[content_id] = {"fetched_at": fetched_at}
    return latest


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _utc(value)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _status_rank(status: str) -> int:
    return {"missing": 0, "stale": 1, "current": 2}.get(status, 3)
