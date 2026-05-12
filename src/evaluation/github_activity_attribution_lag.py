"""Report GitHub activity that has not been attributed in generated content."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100


def build_github_activity_attribution_lag_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Find recent github_activity rows unused by generated_content."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    used_refs, malformed_source_rows = _used_refs(conn, schema)
    rows = _load_activity(conn, schema, cutoff)

    used_count = 0
    unused: list[dict[str, Any]] = []
    label_counts: Counter[str] = Counter()
    activity_type_counts: Counter[str] = Counter()
    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    malformed_label_rows = 0

    for row in rows:
        item = _activity_item(row, generated_at)
        labels, bad_labels = _json_list(row.get("labels"))
        malformed_label_rows += int(bad_labels)
        if bad_labels:
            labels = ["malformed"]
        if not labels:
            labels = ["unlabeled"]
        is_used = str(item["id"]) in used_refs or item["activity_id"] in used_refs
        used_count += int(is_used)
        for label in labels:
            label_counts[str(label)] += 1
        activity_type_counts[item["activity_type"]] += 1
        if not is_used:
            item["labels"] = [str(label) for label in labels]
            unused.append(item)
            for label in labels:
                key = (item["activity_type"], item["repo_name"], str(label), item["age_bucket"])
                group = groups.setdefault(
                    key,
                    {
                        "activity_type": key[0],
                        "repo_name": key[1],
                        "label": key[2],
                        "age_bucket": key[3],
                        "unused_count": 0,
                        "representative_activity_ids": [],
                        "representative_urls": [],
                    },
                )
                group["unused_count"] += 1
                if len(group["representative_activity_ids"]) < 5:
                    group["representative_activity_ids"].append(item["id"])
                    group["representative_urls"].append(item["url"])

    unused.sort(key=lambda item: (-(item["age_days"]), item["id"]))
    group_rows = sorted(
        groups.values(),
        key=lambda item: (-item["unused_count"], item["activity_type"], item["repo_name"], item["label"], _age_sort(item["age_bucket"])),
    )
    return {
        "artifact_type": "github_activity_attribution_lag",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "lookback_start": cutoff.isoformat()},
        "totals": {
            "activity_count": len(rows),
            "unused_count": len(unused),
            "used_count": used_count,
            "malformed_source_activity_rows": malformed_source_rows,
            "malformed_label_rows": malformed_label_rows,
        },
        "label_counts": dict(sorted(label_counts.items())),
        "activity_type_counts": dict(sorted(activity_type_counts.items())),
        "groups": group_rows,
        "oldest_unused": unused[:limit],
        "missing_tables": [table for table in ("github_activity", "generated_content") if table not in schema],
        "missing_columns": _missing_columns(schema),
    }


def format_github_activity_attribution_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_github_activity_attribution_lag_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "GitHub Activity Attribution Lag",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days limit={report['filters']['limit']}",
        f"Totals: unused={totals['unused_count']} used={totals['used_count']} activity={totals['activity_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["oldest_unused"]:
        lines.extend(["", "No unattributed GitHub activity found."])
        return "\n".join(lines)
    lines.extend(["", "Oldest unused:"])
    for item in report["oldest_unused"]:
        lines.append(
            f"  - id={item['id']} activity_id={item['activity_id']} repo={item['repo_name']} "
            f"type={item['activity_type']} age={item['age_bucket']} url={item['url'] or '-'}"
        )
    return "\n".join(lines)


def _used_refs(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> tuple[set[str], int]:
    columns = schema.get("generated_content")
    if columns is None or "source_activity_ids" not in columns:
        return set(), 0
    refs: set[str] = set()
    malformed = 0
    rows = conn.execute(
        "SELECT source_activity_ids FROM generated_content WHERE source_activity_ids IS NOT NULL AND source_activity_ids != ''"
    ).fetchall()
    for row in rows:
        values, bad = _json_list(row["source_activity_ids"])
        malformed += int(bad)
        for value in values:
            refs.add(str(value))
    return refs, malformed


def _load_activity(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema.get("github_activity")
    if columns is None or "id" not in columns:
        return []
    where = []
    params: list[Any] = []
    if "updated_at" in columns:
        where.append("updated_at >= ?")
        params.append(cutoff.isoformat())
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT
               ga.id AS id,
               {_expr(columns, "repo_name", "ga", "repo_name")},
               {_expr(columns, "activity_type", "ga", "activity_type")},
               {_expr(columns, "number", "ga", "number")},
               {_expr(columns, "title", "ga", "title")},
               {_expr(columns, "url", "ga", "url")},
               {_expr(columns, "labels", "ga", "labels")},
               {_expr(columns, "updated_at", "ga", "updated_at")}
            FROM github_activity ga
            {where_sql}
            ORDER BY ga.updated_at ASC, ga.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _activity_item(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    updated = _parse_dt(row.get("updated_at"))
    age_days = int((now - updated).total_seconds() // 86400) if updated else 0
    repo = _clean(row.get("repo_name")) or "unknown"
    number = _clean(row.get("number")) or "unknown"
    activity_type = _clean(row.get("activity_type")) or "unknown"
    return {
        "id": row.get("id"),
        "activity_id": f"{repo}#{number}:{activity_type}",
        "repo_name": repo,
        "activity_type": activity_type,
        "number": number,
        "title": row.get("title"),
        "url": row.get("url"),
        "updated_at": row.get("updated_at"),
        "age_days": age_days,
        "age_bucket": _age_bucket(age_days, updated is None),
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    required = {
        "github_activity": {"id", "repo_name", "activity_type", "number", "updated_at"},
        "generated_content": {"source_activity_ids"},
    }
    missing: dict[str, list[str]] = {}
    for table, columns in required.items():
        if table in schema:
            gaps = sorted(columns - schema[table])
            if gaps:
                missing[table] = gaps
    return missing


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _json_list(value: Any) -> tuple[list[Any], bool]:
    if value in (None, ""):
        return [], False
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return [], True
    return (parsed, False) if isinstance(parsed, list) else ([], True)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _age_bucket(age_days: int, unknown: bool = False) -> str:
    if unknown:
        return "unknown"
    if age_days < 1:
        return "0-1d"
    if age_days < 3:
        return "1-3d"
    if age_days < 7:
        return "3-7d"
    if age_days < 14:
        return "7-14d"
    return "14d+"


def _age_sort(bucket: str) -> int:
    return {"0-1d": 0, "1-3d": 1, "3-7d": 2, "7-14d": 3, "14d+": 4}.get(bucket, 5)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _expr(columns: set[str], column: str, alias: str, output: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"NULL AS {output}"


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
