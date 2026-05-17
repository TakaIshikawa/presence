"""Report generated content format publish readiness."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_EVAL_SCORE = 7.0


def build_generation_format_publish_readiness_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_eval_score: float = DEFAULT_MIN_EVAL_SCORE,
    now: datetime | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "min_eval_score": min_eval_score, "window_start": cutoff.isoformat(), "window_end": generated_at.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty(generated_at, filters, missing_tables, missing_columns)
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    rows = _generated_rows(conn, schema, cutoff, generated_at)
    pub_statuses = _publication_statuses(conn, schema)
    for row in rows:
        key = (row["content_type"] or "unknown", row["content_format"] or "unknown")
        group = groups.setdefault(
            key,
            {
                "content_type": key[0],
                "content_format": key[1],
                "generated_count": 0,
                "high_score_count": 0,
                "queued_count": 0,
                "published_count": 0,
                "failed_count": 0,
                "unpublished_count": 0,
                "missing_publication_count": 0,
                "examples": [],
            },
        )
        group["generated_count"] += 1
        score = _float(row.get("evaluation_score"))
        if score is not None and score >= min_eval_score:
            group["high_score_count"] += 1
        statuses = pub_statuses.get(int(row["id"]), [])
        if not statuses:
            group["missing_publication_count"] += 1
            group["unpublished_count"] += 1
        else:
            status_set = {status.lower() for status in statuses}
            if "published" in status_set:
                group["published_count"] += 1
            if status_set & {"queued", "pending", "scheduled"}:
                group["queued_count"] += 1
            if status_set & {"failed", "error"}:
                group["failed_count"] += 1
            if "published" not in status_set:
                group["unpublished_count"] += 1
        if len(group["examples"]) < 3:
            group["examples"].append({"content_id": int(row["id"]), "evaluation_score": score, "publication_statuses": statuses})
    result_groups = []
    for group in groups.values():
        generated = group["generated_count"]
        group["readiness_rate"] = round(group["high_score_count"] / generated, 3) if generated else 0.0
        group["published_rate"] = round(group["published_count"] / generated, 3) if generated else 0.0
        result_groups.append(group)
    result_groups.sort(key=lambda item: (item["content_type"], item["content_format"]))
    totals = Counter()
    for group in result_groups:
        for key in ("generated_count", "high_score_count", "queued_count", "published_count", "failed_count", "unpublished_count", "missing_publication_count"):
            totals[key] += group[key]
    return {
        "artifact_type": "generation_format_publish_readiness",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": dict(totals),
        "format_groups": result_groups,
        "missing_tables": [],
        "missing_columns": {},
    }


def format_generation_format_publish_readiness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_generation_format_publish_readiness_text(report: dict[str, Any]) -> str:
    t = report["totals"]
    lines = [
        "Generation Format Publish Readiness",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} min_eval_score={report['filters']['min_eval_score']}",
        f"Totals: generated={t.get('generated_count', 0)} high_score={t.get('high_score_count', 0)} queued={t.get('queued_count', 0)} published={t.get('published_count', 0)} failed={t.get('failed_count', 0)} unpublished={t.get('unpublished_count', 0)} missing_publication={t.get('missing_publication_count', 0)}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["format_groups"]:
        lines.append("No generated content matched.")
        return "\n".join(lines)
    lines.extend(["", "Format groups:"])
    for group in report["format_groups"]:
        lines.append(
            f"- {group['content_type']}/{group['content_format']}: generated={group['generated_count']} "
            f"ready_rate={group['readiness_rate']} published={group['published_count']} failed={group['failed_count']}"
        )
    return "\n".join(lines)


format_generation_format_publish_readiness_table = format_generation_format_publish_readiness_text


def _generated_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime, generated_at: datetime) -> list[dict[str, Any]]:
    cols = schema["generated_content"]
    score_expr = "evaluation_score" if "evaluation_score" in cols else ("final_score" if "final_score" in cols else "NULL AS evaluation_score")
    fmt_expr = "content_format" if "content_format" in cols else "NULL AS content_format"
    rows = conn.execute(
        f"""SELECT id, content_type, {fmt_expr}, {score_expr}
            FROM generated_content
            WHERE created_at IS NOT NULL
              AND datetime(created_at) >= datetime(?)
              AND datetime(created_at) <= datetime(?)
            ORDER BY content_type ASC, content_format ASC, id ASC""",
        (cutoff.isoformat(), generated_at.isoformat()),
    ).fetchall()
    return [dict(row) for row in rows]


def _publication_statuses(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, list[str]]:
    if "content_publications" not in schema:
        return {}
    rows = conn.execute("SELECT content_id, status FROM content_publications ORDER BY content_id ASC, status ASC").fetchall()
    result: dict[int, list[str]] = defaultdict(list)
    for row in rows:
        result[int(row["content_id"])].append(str(row["status"] or "unknown"))
    return result


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    required = {"generated_content": {"id", "content_type", "created_at"}, "content_publications": {"content_id", "status"}}
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: sorted(cols - schema[table])
        for table, cols in required.items()
        if table in schema and cols - schema[table]
    }
    return missing_tables, missing_columns


def _empty(generated_at: datetime, filters: dict[str, Any], missing_tables: list[str], missing_columns: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "artifact_type": "generation_format_publish_readiness",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {},
        "format_groups": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
