"""Report prompt formats whose engagement outcomes are late."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_OUTCOME_WINDOW_DAYS = 3
DEFAULT_MIN_SAMPLE = 1
DEFAULT_LIMIT = 50


def build_prompt_format_outcome_lag_report(
    candidate_rows: list[dict[str, Any]],
    publication_rows: list[dict[str, Any]],
    metric_rows: list[dict[str, Any]],
    *,
    outcome_window_days: int = DEFAULT_OUTCOME_WINDOW_DAYS,
    min_sample: int = DEFAULT_MIN_SAMPLE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Group candidates by prompt format and rank late engagement outcomes."""
    if outcome_window_days <= 0:
        raise ValueError("outcome_window_days must be positive")
    if min_sample <= 0:
        raise ValueError("min_sample must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    publication_by_content = _publication_index(candidate_rows, publication_rows)
    metrics_by_content = _metric_index(metric_rows)
    groups: dict[str, dict[str, Any]] = {}

    for row in candidate_rows:
        content_id = _optional_int(_first(row, "content_id", "id"))
        if content_id is None:
            continue
        prompt_format = _format_name(row)
        group = groups.setdefault(prompt_format, _empty_group(prompt_format))
        group["candidate_count"] += 1
        group["candidate_ids"].append(content_id)

        publication = publication_by_content.get(content_id)
        if not publication:
            continue
        group["published"] += 1
        group["published_content_ids"].append(content_id)
        published_at = publication["published_at"]
        metric = metrics_by_content.get(content_id)
        if metric:
            group["metrics_fetched"] += 1
            group["metric_content_ids"].append(content_id)
            continue
        age_days = max((generated_at - published_at).total_seconds() / 86400, 0)
        pending_item = {
            "content_id": content_id,
            "published_at": published_at.isoformat(),
            "age_days": round(age_days, 2),
            "platform": publication["platform"],
        }
        group["pending"] += 1
        group["pending_content_ids"].append(content_id)
        group["pending_examples"].append(pending_item)
        if age_days > outcome_window_days:
            group["stale_pending"] += 1
            group["stale_pending_content_ids"].append(content_id)
            group["stale_examples"].append(pending_item)

    formats = [_finalize_group(group) for group in groups.values()]
    formats.sort(key=_format_sort_key)
    ranked = [item for item in formats if item["published"] >= min_sample][:limit]
    totals = {
        "candidate_count": sum(item["candidate_count"] for item in formats),
        "format_count": len(formats),
        "ranked_format_count": len(ranked),
        "published": sum(item["published"] for item in formats),
        "metrics_fetched": sum(item["metrics_fetched"] for item in formats),
        "pending": sum(item["pending"] for item in formats),
        "stale_pending": sum(item["stale_pending"] for item in formats),
    }
    return {
        "artifact_type": "prompt_format_outcome_lag",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "outcome_window_days": outcome_window_days,
            "min_sample": min_sample,
            "limit": limit,
        },
        "totals": totals,
        "formats": formats,
        "ranked_formats": ranked,
        "empty_state": {
            "is_empty": not formats,
            "message": "No prompt-format candidates found." if not formats else None,
        },
    }


def build_prompt_format_outcome_lag_report_from_db(
    db_or_conn: Any,
    *,
    outcome_window_days: int = DEFAULT_OUTCOME_WINDOW_DAYS,
    min_sample: int = DEFAULT_MIN_SAMPLE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load SQLite rows and build the prompt-format outcome lag report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_prompt_format_outcome_lag_report(
        _load_candidates(conn, schema),
        _load_publications(conn, schema),
        _load_metrics(conn, schema),
        outcome_window_days=outcome_window_days,
        min_sample=min_sample,
        limit=limit,
        now=now,
    )


def format_prompt_format_outcome_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_prompt_format_outcome_lag_text(report: dict[str, Any]) -> str:
    lines = [
        "Prompt Format Outcome Lag",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: outcome_window_days={report['filters']['outcome_window_days']} "
            f"min_sample={report['filters']['min_sample']} limit={report['filters']['limit']}"
        ),
        (
            "Totals: "
            f"formats={report['totals']['format_count']} "
            f"published={report['totals']['published']} "
            f"metrics={report['totals']['metrics_fetched']} "
            f"pending={report['totals']['pending']} "
            f"stale={report['totals']['stale_pending']}"
        ),
    ]
    if not report["ranked_formats"]:
        lines.extend(["", "No prompt formats meet the minimum sample threshold."])
        return "\n".join(lines)
    lines.extend(["", "Formats:", "format                cand  pub  metrics  pending  stale  late_rate"])
    for item in report["ranked_formats"]:
        lines.append(
            f"{item['prompt_format'][:20]:<20} "
            f"{item['candidate_count']:<5} "
            f"{item['published']:<4} "
            f"{item['metrics_fetched']:<7} "
            f"{item['pending']:<7} "
            f"{item['stale_pending']:<5} "
            f"{item['late_outcome_rate']:.2%}"
        )
    return "\n".join(lines)


def _load_candidates(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema or not {"id", "content_type"}.issubset(schema["generated_content"]):
        return []
    columns = schema["generated_content"]
    selected = [
        "id AS content_id",
        "content_format" if "content_format" in columns else "NULL AS content_format",
        "content_type",
        "published" if "published" in columns else "0 AS published",
        "published_at" if "published_at" in columns else "NULL AS published_at",
        "created_at" if "created_at" in columns else "NULL AS created_at",
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
           FROM generated_content
           ORDER BY created_at ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_publications(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "content_publications" not in schema:
        return []
    columns = schema["content_publications"]
    if not {"content_id", "status"}.issubset(columns):
        return []
    selected = [
        "content_id",
        "platform" if "platform" in columns else "'unknown' AS platform",
        "status",
        "published_at" if "published_at" in columns else "NULL AS published_at",
        "updated_at" if "updated_at" in columns else "NULL AS updated_at",
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
           FROM content_publications
           WHERE LOWER(COALESCE(status, '')) = 'published'
           ORDER BY published_at ASC, updated_at ASC, content_id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_metrics(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    metric_tables = (
        ("post_engagement", "x"),
        ("bluesky_engagement", "bluesky"),
        ("linkedin_engagement", "linkedin"),
        ("mastodon_engagement", "mastodon"),
    )
    for table, platform in metric_tables:
        columns = schema.get(table, set())
        if not {"content_id", "fetched_at"}.issubset(columns):
            continue
        fetched = conn.execute(
            f"""SELECT content_id, fetched_at, '{platform}' AS platform
               FROM {table}
               ORDER BY fetched_at ASC, content_id ASC"""
        ).fetchall()
        rows.extend(dict(row) for row in fetched)
    return rows


def _publication_index(
    candidate_rows: list[dict[str, Any]],
    publication_rows: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    indexed: dict[int, dict[str, Any]] = {}
    for row in candidate_rows:
        content_id = _optional_int(_first(row, "content_id", "id"))
        published_at = _parse_dt(row.get("published_at"))
        if content_id is None or not _truthy(row.get("published")) or published_at is None:
            continue
        indexed[content_id] = {"published_at": published_at, "platform": _text(row.get("platform")) or "generated_content"}
    for row in publication_rows:
        content_id = _optional_int(_first(row, "content_id", "id"))
        published_at = _parse_dt(_first(row, "published_at", "updated_at"))
        if content_id is None or published_at is None:
            continue
        existing = indexed.get(content_id)
        item = {"published_at": published_at, "platform": _text(row.get("platform")) or "unknown"}
        if existing is None or published_at < existing["published_at"]:
            indexed[content_id] = item
    return indexed


def _metric_index(metric_rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    indexed: dict[int, dict[str, Any]] = {}
    for row in metric_rows:
        content_id = _optional_int(_first(row, "content_id", "id"))
        fetched_at = _parse_dt(_first(row, "fetched_at", "created_at"))
        if content_id is None or fetched_at is None:
            continue
        current = indexed.get(content_id)
        if current is None or fetched_at > current["fetched_at"]:
            indexed[content_id] = {"fetched_at": fetched_at, "platform": _text(row.get("platform"))}
    return indexed


def _empty_group(prompt_format: str) -> dict[str, Any]:
    return {
        "prompt_format": prompt_format,
        "candidate_count": 0,
        "published": 0,
        "metrics_fetched": 0,
        "pending": 0,
        "stale_pending": 0,
        "candidate_ids": [],
        "published_content_ids": [],
        "metric_content_ids": [],
        "pending_content_ids": [],
        "stale_pending_content_ids": [],
        "pending_examples": [],
        "stale_examples": [],
    }


def _finalize_group(group: dict[str, Any]) -> dict[str, Any]:
    published = group["published"]
    group["metric_fetch_rate"] = round(group["metrics_fetched"] / published, 4) if published else 0.0
    group["late_outcome_rate"] = round(group["stale_pending"] / published, 4) if published else 0.0
    group["pending_rate"] = round(group["pending"] / published, 4) if published else 0.0
    for key in (
        "candidate_ids",
        "published_content_ids",
        "metric_content_ids",
        "pending_content_ids",
        "stale_pending_content_ids",
    ):
        group[key] = sorted(set(group[key]))
    group["pending_examples"].sort(key=lambda item: (-item["age_days"], item["content_id"]))
    group["stale_examples"].sort(key=lambda item: (-item["age_days"], item["content_id"]))
    group["pending_examples"] = group["pending_examples"][:3]
    group["stale_examples"] = group["stale_examples"][:3]
    return group


def _format_sort_key(item: dict[str, Any]) -> tuple[float, int, int, str]:
    return (-item["late_outcome_rate"], -item["stale_pending"], -item["pending"], item["prompt_format"])


def _format_name(row: dict[str, Any]) -> str:
    return _text(_first(row, "prompt_format", "content_format", "format")) or "unknown"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _text(value: Any) -> str:
    return str(value).strip() if value not in (None, "") else ""


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "published"}
    return bool(value)
