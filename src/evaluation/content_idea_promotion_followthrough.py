"""Report promoted content ideas without planning or generation followthrough."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_MAX_AGE_DAYS = 14
DEFAULT_LIMIT = 100
_PROMOTED_STATUSES = {"promoted", "approved", "planned"}
_ISSUE_RANK = {
    "malformed_source_metadata": 0,
    "stale_promoted_idea": 1,
    "missing_planned_topic": 2,
    "missing_generated_content": 3,
}


def build_content_idea_promotion_followthrough_report(
    rows: list[dict[str, Any]],
    *,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] | list[str] = (),
) -> dict[str, Any]:
    if max_age_days < 0:
        raise ValueError("max_age_days must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    stale_before = generated_at - timedelta(days=max_age_days)
    ideas, planned, generated = _split_rows(rows)
    issue_items: list[dict[str, Any]] = []
    for idea in ideas:
        if not _is_promoted(idea):
            continue
        parsed_metadata, malformed = _parse_metadata(idea.get("source_metadata"))
        planned_matches = _planned_matches(idea, parsed_metadata, planned)
        generated_matches = _generated_matches(idea, parsed_metadata, generated)
        base = {
            "content_idea_id": idea["content_idea_id"],
            "topic": idea["topic"],
            "status": idea["status"],
            "promoted_at": idea["promoted_at"],
            "age_days": _age_days(generated_at, idea["promoted_at"]),
            "planned_topic_ids": planned_matches,
            "generated_content_ids": generated_matches,
        }
        if malformed:
            issue_items.append({**base, "issue_type": "malformed_source_metadata"})
        if _is_stale(idea["promoted_at"], stale_before) and (not planned_matches or not generated_matches):
            issue_items.append({**base, "issue_type": "stale_promoted_idea"})
        if not planned_matches:
            issue_items.append({**base, "issue_type": "missing_planned_topic"})
        if not generated_matches:
            issue_items.append({**base, "issue_type": "missing_generated_content"})
    issue_items.sort(key=_issue_sort_key)
    shown = issue_items[:limit]
    return {
        "artifact_type": "content_idea_promotion_followthrough",
        "generated_at": generated_at.isoformat(),
        "missing_tables": sorted(missing_tables),
        "thresholds": {"max_age_days": max_age_days, "stale_before": stale_before.isoformat(), "limit": limit},
        "summary": {
            "ideas_scanned": len(ideas),
            "promoted_ideas_scanned": sum(1 for idea in ideas if _is_promoted(idea)),
            "issue_count": len(issue_items),
            "shown_count": len(shown),
        },
        "issue_items": shown,
    }


def build_content_idea_promotion_followthrough_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [table for table in ("content_ideas", "planned_topics", "generated_content") if table not in schema]
    rows = _load_rows(conn, schema)
    return build_content_idea_promotion_followthrough_report(rows, missing_tables=tuple(missing_tables), **kwargs)


def format_content_idea_promotion_followthrough_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_idea_promotion_followthrough_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Content Idea Promotion Followthrough",
        f"Generated: {report['generated_at']}",
        f"Max age days: {report['thresholds']['max_age_days']}",
        f"Totals: ideas={summary['ideas_scanned']} promoted={summary['promoted_ideas_scanned']} issues={summary['issue_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["issue_items"]:
        lines.append("No content idea promotion followthrough issues found.")
        return "\n".join(lines)
    lines.extend(["", "content_idea_id | issue | topic | age_days"])
    for item in report["issue_items"]:
        lines.append(f"{item['content_idea_id']} | {item['issue_type']} | {item['topic'] or '-'} | {item['age_days']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if "content_ideas" in schema:
        ci = schema["content_ideas"]
        select = [
            _expr(ci, "id", "content_idea_id", "rowid"),
            _expr(ci, "topic", "topic", "NULL"),
            _expr(ci, "status", "status", "'promoted'"),
            _expr(ci, "promoted_at", "promoted_at", "NULL"),
            _expr(ci, "updated_at", "updated_at", "NULL"),
            _expr(ci, "created_at", "created_at", "NULL"),
            _expr(ci, "source_metadata", "source_metadata", "NULL"),
        ]
        rows.extend({"row_type": "idea", **dict(row)} for row in conn.execute(f"SELECT {', '.join(select)} FROM content_ideas"))
    if "planned_topics" in schema:
        pt = schema["planned_topics"]
        select = [
            _expr(pt, "id", "planned_topic_id", "rowid"),
            _expr(pt, "topic", "topic", "NULL"),
            _expr(pt, "source_metadata", "source_metadata", "NULL"),
        ]
        rows.extend({"row_type": "planned", **dict(row)} for row in conn.execute(f"SELECT {', '.join(select)} FROM planned_topics"))
    if "generated_content" in schema:
        gc = schema["generated_content"]
        topic_expr = _expr(gc, "topic", "topic", _expr(gc, "title", "topic", "NULL"))
        select = [
            _expr(gc, "id", "content_id", "rowid"),
            topic_expr,
            _expr(gc, "source_metadata", "source_metadata", "NULL"),
        ]
        rows.extend({"row_type": "generated", **dict(row)} for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content"))
    return rows


def _split_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    ideas: list[dict[str, Any]] = []
    planned: list[dict[str, Any]] = []
    generated: list[dict[str, Any]] = []
    for row in rows:
        kind = row.get("row_type", "idea")
        if kind == "planned":
            planned.append(row)
        elif kind == "generated":
            generated.append(row)
        else:
            ideas.append(
                {
                    "content_idea_id": _int_or_none(row.get("content_idea_id") or row.get("id")),
                    "topic": _topic(row.get("topic")),
                    "status": _clean(row.get("status"), "promoted").lower(),
                    "promoted_at": row.get("promoted_at") or row.get("updated_at") or row.get("created_at"),
                    "source_metadata": row.get("source_metadata"),
                }
            )
    return ideas, planned, generated


def _planned_matches(idea: dict[str, Any], metadata: dict[str, Any], planned: list[dict[str, Any]]) -> list[int]:
    wanted_id = _int_or_none(metadata.get("planned_topic_id"))
    matches = []
    for row in planned:
        row_id = _int_or_none(row.get("planned_topic_id") or row.get("id"))
        if row_id is not None and row_id == wanted_id:
            matches.append(row_id)
        elif idea["topic"] and idea["topic"] == _topic(row.get("topic")):
            matches.append(row_id)
    return sorted({match for match in matches if match is not None})


def _generated_matches(idea: dict[str, Any], metadata: dict[str, Any], generated: list[dict[str, Any]]) -> list[int]:
    wanted_id = _int_or_none(metadata.get("content_id"))
    matches = []
    for row in generated:
        row_id = _int_or_none(row.get("content_id") or row.get("id"))
        if row_id is not None and row_id == wanted_id:
            matches.append(row_id)
        elif idea["topic"] and idea["topic"] == _topic(row.get("topic")):
            matches.append(row_id)
    return sorted({match for match in matches if match is not None})


def _parse_metadata(value: Any) -> tuple[dict[str, Any], bool]:
    if value in (None, ""):
        return {}, False
    if isinstance(value, dict):
        return value, False
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}, True
    return (parsed, False) if isinstance(parsed, dict) else ({}, False)


def _is_promoted(idea: dict[str, Any]) -> bool:
    return idea["status"] in _PROMOTED_STATUSES or idea["promoted_at"] is not None


def _is_stale(value: Any, stale_before: datetime) -> bool:
    parsed = _parse_dt(value)
    return bool(parsed and parsed < stale_before)


def _age_days(now: datetime, value: Any) -> int | None:
    parsed = _parse_dt(value)
    if not parsed:
        return None
    return max(0, int((now - parsed).total_seconds() // 86400))


def _issue_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    age = item["age_days"] if item["age_days"] is not None else -1
    return (_ISSUE_RANK[item["issue_type"]], -age, item["content_idea_id"] or 0)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _topic(value: Any) -> str:
    return _clean(value).casefold()


def _clean(value: Any, default: str = "") -> str:
    text = str(value).strip() if value is not None else ""
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
