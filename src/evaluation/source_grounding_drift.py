"""Detect generated-content drift toward weak or missing source grounding."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 20
SOURCE_FIELDS = ("source_commits", "source_messages", "source_activity_ids")
WEAK_FAMILIES = {"knowledge_links", "newsletter_refs"}


def build_source_grounding_drift_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic read-only source-grounding drift report."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_content(conn, schema, cutoff, generated_at)
    knowledge = _knowledge_links(conn, schema)
    newsletter = _newsletter_refs(conn, schema)
    items = [_analyze(row, schema, knowledge, newsletter) for row in rows]
    flagged = [item for item in items if item["flag_codes"]]
    flagged.sort(key=lambda item: (-item["risk_score"], item["week_start"], item["content_type"], item["content_id"]))
    return {
        "artifact_type": "source_grounding_drift",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit},
        "totals": {
            "rows_scanned": len(rows),
            "flagged_items": len(flagged),
            "by_flag_code": dict(sorted(Counter(code for item in flagged for code in item["flag_codes"]).items())),
        },
        "weekly_drift": _weekly(flagged, limit),
        "items": flagged[:limit],
        "missing_tables": [] if "generated_content" in schema else ["generated_content"],
    }


def format_source_grounding_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_source_grounding_drift_text(report: dict[str, Any]) -> str:
    lines = [
        "Source Grounding Drift",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} limit={report['filters']['limit']}",
        f"Totals: scanned={report['totals']['rows_scanned']} flagged={report['totals']['flagged_items']}",
    ]
    if not report["items"]:
        lines.extend(["", "No source-grounding drift found."])
        return "\n".join(lines)
    lines.extend(["", "Items:"])
    for item in report["items"]:
        lines.append(
            f"- content_id={item['content_id']} type={item['content_type']} "
            f"risk={item['risk_score']} flags={','.join(item['flag_codes'])} "
            f"families={','.join(item['evidence_families']) or '-'}"
        )
    return "\n".join(lines)


def _load_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    cols = schema["generated_content"]
    select = [
        "gc.id",
        "gc.content_type" if "content_type" in cols else "'unknown' AS content_type",
        "gc.created_at" if "created_at" in cols else "NULL AS created_at",
    ]
    for field in SOURCE_FIELDS:
        select.append(f"gc.{field}" if field in cols else f"NULL AS {field}")
    rows = conn.execute(
        f"SELECT {', '.join(select)} FROM generated_content gc ORDER BY gc.created_at ASC, gc.id ASC"
    ).fetchall()
    out = []
    for row in rows:
        created = _parse_dt(row["created_at"]) or now
        if cutoff <= created <= now:
            item = dict(row)
            item["created_at"] = created
            out.append(item)
    return out


def _analyze(
    row: dict[str, Any],
    schema: dict[str, set[str]],
    knowledge: dict[int, int],
    newsletter: dict[int, int],
) -> dict[str, Any]:
    content_id = int(row["id"])
    families: dict[str, int] = {}
    malformed: list[str] = []
    for field, family, table, column in (
        ("source_commits", "commits", "github_commits", "commit_sha"),
        ("source_messages", "messages", "claude_messages", "message_uuid"),
        ("source_activity_ids", "activity", "github_activity", "id"),
    ):
        values, bad = _json_list(row.get(field))
        if bad:
            malformed.append(field)
        if values:
            available = _available_refs(schema, table, column, values)
            families[family] = available
    if knowledge.get(content_id, 0):
        families["knowledge_links"] = knowledge[content_id]
    if newsletter.get(content_id, 0):
        families["newsletter_refs"] = newsletter[content_id]

    evidence_families = sorted(family for family, count in families.items() if count > 0)
    flags = []
    if malformed:
        flags.append("malformed_source_json")
    if not evidence_families:
        flags.append("no_source_evidence")
    elif len(evidence_families) == 1 and evidence_families[0] in WEAK_FAMILIES:
        flags.append("single_weak_source_family")
    risk = (50 if "no_source_evidence" in flags else 0) + (35 if "malformed_source_json" in flags else 0) + (25 if "single_weak_source_family" in flags else 0)
    created = row["created_at"]
    return {
        "content_id": content_id,
        "content_type": row.get("content_type") or "unknown",
        "created_at": created.isoformat(),
        "week_start": _week_start(created),
        "flag_codes": flags,
        "risk_score": risk,
        "evidence_families": evidence_families,
        "available_source_counts": {key: families[key] for key in sorted(families)},
        "malformed_source_fields": malformed,
    }


def _available_refs(schema: dict[str, set[str]], table: str, column: str, values: list[Any]) -> int:
    if table not in schema or column not in schema[table]:
        return 0
    return len({str(value) for value in values if str(value).strip()})


def _knowledge_links(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, int]:
    if "content_knowledge_links" not in schema:
        return {}
    return {
        int(row["content_id"]): int(row["count"])
        for row in conn.execute(
            "SELECT content_id, COUNT(*) AS count FROM content_knowledge_links GROUP BY content_id"
        )
    }


def _newsletter_refs(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, int]:
    if "newsletter_sends" not in schema or "source_content_ids" not in schema["newsletter_sends"]:
        return {}
    refs: Counter[int] = Counter()
    for row in conn.execute("SELECT source_content_ids FROM newsletter_sends"):
        values, _bad = _json_list(row["source_content_ids"])
        for value in values:
            try:
                refs[int(value)] += 1
            except (TypeError, ValueError):
                continue
    return dict(refs)


def _weekly(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[(item["week_start"], item["content_type"])].append(item)
    rows = []
    for (week, content_type), group in grouped.items():
        rows.append(
            {
                "week_start": week,
                "content_type": content_type,
                "flagged_count": len(group),
                "flag_counts": dict(sorted(Counter(code for item in group for code in item["flag_codes"]).items())),
                "representative_content_ids": sorted(item["content_id"] for item in group)[:limit],
            }
        )
    return sorted(rows, key=lambda row: (row["week_start"], row["content_type"]))


def _json_list(value: Any) -> tuple[list[Any], bool]:
    if value in (None, ""):
        return [], False
    if isinstance(value, list):
        return value, False
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return [], True
    return (parsed, False) if isinstance(parsed, list) else ([], True)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


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


def _week_start(value: datetime) -> str:
    return (value - timedelta(days=value.weekday())).date().isoformat()
