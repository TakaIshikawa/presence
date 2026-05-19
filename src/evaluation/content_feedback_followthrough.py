"""Report stale content feedback with no observable followthrough."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Iterable


DEFAULT_MIN_AGE_DAYS = 7
DEFAULT_LIMIT = 100
DEFAULT_FEEDBACK_TYPE = "all"
DEFAULT_PUBLISHED_STATE = "all"
FEEDBACK_TYPES = ("reject", "revise", "prefer")
PUBLISHED_STATES = ("unpublished", "published", "abandoned", "unknown")


def build_content_feedback_followthrough_report(
    feedback_rows: list[dict[str, Any]],
    *,
    min_age_days: int = DEFAULT_MIN_AGE_DAYS,
    feedback_type: str | Iterable[str] = DEFAULT_FEEDBACK_TYPE,
    published_state: str | Iterable[str] = DEFAULT_PUBLISHED_STATE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if min_age_days <= 0:
        raise ValueError("min_age_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    feedback_types = _normalize_filter(feedback_type, default=DEFAULT_FEEDBACK_TYPE)
    published_states = _normalize_filter(published_state, default=DEFAULT_PUBLISHED_STATE)
    cutoff = generated_at - timedelta(days=min_age_days)
    scanned = 0
    malformed_tags = 0
    findings: list[dict[str, Any]] = []
    group_counts: Counter[tuple[str, str]] = Counter()

    for row in feedback_rows:
        row_type = _clean(row.get("feedback_type")).lower() or "unknown"
        if feedback_types != ("all",) and row_type not in feedback_types:
            continue
        state = _published_state(row.get("content_published"))
        if published_states != ("all",) and state not in published_states:
            continue
        created_at = _parse_dt(row.get("created_at"))
        if created_at is None or created_at > cutoff:
            continue
        scanned += 1
        has_replacement = bool(_clean(row.get("replacement_text")))
        reuse_replacement = _truthy(row.get("newer_reuses_replacement_text"))
        reuse_topic = _truthy(row.get("newer_reuses_topic"))
        type_outcome = _type_specific_outcome(row_type, state)
        if reuse_replacement or reuse_topic or type_outcome:
            continue

        tags, malformed = _tags(row.get("tags"))
        malformed_tags += int(malformed)
        if not tags:
            tags = ["untagged"]
        age_days = int((generated_at - created_at).total_seconds() // 86400)
        finding = {
            "feedback_id": _first(row, "feedback_id", "id"),
            "content_id": row.get("content_id"),
            "feedback_type": row_type,
            "tags": tags,
            "created_at": created_at.isoformat(),
            "age_days": age_days,
            "has_replacement_text": has_replacement,
            "content_published": row.get("content_published"),
            "published_state": state,
            "newer_reuses_replacement_text": reuse_replacement,
            "newer_reuses_topic": reuse_topic,
            "replacement_text_preview": _snippet(row.get("replacement_text")),
            "notes_preview": _snippet(row.get("notes")),
        }
        findings.append(finding)
        for tag in tags:
            group_counts[(row_type, tag)] += 1

    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    groups = [
        {"feedback_type": feedback_type, "tag": tag, "feedback_count": count}
        for (feedback_type, tag), count in sorted(group_counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))
    ]
    return {
        "artifact_type": "content_feedback_followthrough",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "min_age_days": min_age_days,
            "feedback_type": list(feedback_types),
            "published_state": list(published_states),
            "limit": limit,
            "stale_before": cutoff.isoformat(),
        },
        "summary": {
            "scanned_feedback_count": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "malformed_tag_rows": malformed_tags,
            "by_feedback_type": dict(sorted(Counter(item["feedback_type"] for item in findings).items())),
        },
        "groups": groups,
        "missing_tables": sorted(missing_tables or []),
        "findings": shown,
        "empty_state": {
            "is_empty": not findings,
            "message": "No stale content feedback without followthrough found." if not findings else None,
        },
    }


def build_content_feedback_followthrough_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"content_feedback", "generated_content"}
    missing = sorted(required - set(schema))
    if missing:
        return build_content_feedback_followthrough_report([], missing_tables=missing, **kwargs)
    return build_content_feedback_followthrough_report(_load_feedback_rows(conn, schema), **kwargs)


def format_content_feedback_followthrough_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_feedback_followthrough_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Content Feedback Followthrough",
        f"Generated: {report['generated_at']}",
        f"Min age: {report['filters']['min_age_days']} days",
        f"Feedback type: {', '.join(report['filters']['feedback_type'])}",
        f"Published state: {', '.join(report['filters']['published_state'])}",
        f"Limit: {report['filters']['limit']}",
        (
            "Totals: "
            f"scanned={summary['scanned_feedback_count']} "
            f"findings={summary['finding_count']} "
            f"shown={summary['shown_count']} "
            f"malformed_tags={summary['malformed_tag_rows']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "Feedback without followthrough by type and tag:"])
    for group in report["groups"]:
        lines.append(f"  - type={group['feedback_type']} tag={group['tag']} count={group['feedback_count']}")
    lines.extend(["", "feedback_id | content_id | type | tags | age_days | replacement | published | newer_replacement | newer_topic"])
    for item in report["findings"]:
        lines.append(
            f"{item['feedback_id'] or '-'} | {item['content_id'] or '-'} | {item['feedback_type']} | "
            f"{','.join(item['tags'])} | {item['age_days']} | {_yes_no(item['has_replacement_text'])} | "
            f"{item['published_state']} | {_yes_no(item['newer_reuses_replacement_text'])} | {_yes_no(item['newer_reuses_topic'])}"
        )
    return "\n".join(lines)


def _load_feedback_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cf = schema["content_feedback"]
    gc = schema["generated_content"]
    if not {"id", "content_id"}.issubset(cf) or "id" not in gc:
        return []
    feedback_rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   cf.id AS feedback_id,
                   cf.content_id AS content_id,
                   {_expr(cf, "feedback_type", "cf", "feedback_type")},
                   {_expr(cf, "notes", "cf", "notes")},
                   {_expr(cf, "replacement_text", "cf", "replacement_text")},
                   {_expr(cf, "tags", "cf", "tags")},
                   {_expr(cf, "created_at", "cf", "created_at")},
                   {_expr(gc, "content", "gc", "content")},
                   {_expr(gc, "published", "gc", "content_published")}
               FROM content_feedback cf
               LEFT JOIN generated_content gc ON gc.id = cf.content_id
               WHERE {_column_filter(cf, "feedback_type", "cf.feedback_type IN ('reject', 'revise', 'prefer')")}
               ORDER BY cf.id ASC"""
        ).fetchall()
    ]
    content_rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   gc.id AS content_id,
                   {_expr(gc, "content", "gc", "content")},
                   {_expr(gc, "created_at", "gc", "created_at")}
               FROM generated_content gc
               ORDER BY gc.id ASC"""
        ).fetchall()
    ]
    for row in feedback_rows:
        newer = _newer_content(content_rows, row)
        replacement = _clean(row.get("replacement_text"))
        topic_terms = _topic_terms(row)
        row["newer_reuses_replacement_text"] = bool(replacement and any(_contains_text(item.get("content"), replacement) for item in newer))
        row["newer_reuses_topic"] = bool(topic_terms and any(_contains_any(item.get("content"), topic_terms) for item in newer))
    return feedback_rows


def _newer_content(content_rows: list[dict[str, Any]], feedback: dict[str, Any]) -> list[dict[str, Any]]:
    feedback_at = _parse_dt(feedback.get("created_at"))
    feedback_content_id = feedback.get("content_id")
    newer: list[dict[str, Any]] = []
    for row in content_rows:
        if row.get("content_id") == feedback_content_id:
            continue
        created_at = _parse_dt(row.get("created_at"))
        if feedback_at is not None and created_at is not None and created_at <= feedback_at:
            continue
        if feedback_at is None and _int_or_text(row.get("content_id")) <= _int_or_text(feedback_content_id):
            continue
        newer.append(row)
    return newer


def _topic_terms(row: dict[str, Any]) -> list[str]:
    tags, malformed = _tags(row.get("tags"))
    terms = [] if malformed else tags
    terms.extend(_keywords(row.get("notes")))
    if not terms:
        terms.extend(_keywords(row.get("content")))
    return sorted({term for term in terms if len(term) >= 4})[:8]


def _keywords(value: Any) -> list[str]:
    words = re.findall(r"[a-zA-Z][a-zA-Z0-9_'-]{3,}", _clean(value).lower())
    stop = {"this", "that", "with", "from", "into", "content", "copy", "post", "make", "more", "less"}
    return [word for word in words if word not in stop][:8]


def _contains_text(content: Any, text: str) -> bool:
    haystack = _normalize_search(content)
    needle = _normalize_search(text)
    return bool(needle and needle in haystack)


def _contains_any(content: Any, terms: list[str]) -> bool:
    haystack = _normalize_search(content)
    return any(_normalize_search(term) in haystack for term in terms)


def _normalize_search(value: Any) -> str:
    return re.sub(r"\s+", " ", _clean(value).lower()).strip()


def _type_specific_outcome(feedback_type: str, published_state: str) -> bool:
    return (feedback_type == "prefer" and published_state == "published") or (
        feedback_type == "reject" and published_state == "abandoned"
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _expr(columns: set[str], column: str, alias: str, output: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"NULL AS {output}"


def _column_filter(columns: set[str], column: str, expression: str) -> str:
    return expression if column in columns else "1 = 1"


def _normalize_filter(value: str | Iterable[str], *, default: str) -> tuple[str, ...]:
    if isinstance(value, str):
        values = value.split(",")
    else:
        values = list(value)
    normalized = tuple(item for item in (_clean(entry).lower() for entry in values) if item)
    return normalized or (default,)


def _tags(value: Any) -> tuple[list[str], bool]:
    if value in (None, ""):
        return [], False
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return ["malformed"], True
    if not isinstance(parsed, list):
        return ["malformed"], True
    tags = sorted({_clean(item) for item in parsed if _clean(item)})
    return tags, False


def _published_state(value: Any) -> str:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return "unknown"
    if parsed == 1:
        return "published"
    if parsed == -1:
        return "abandoned"
    return "unpublished"


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (-finding["age_days"], finding["feedback_type"], finding["tags"], _int_or_text(finding["feedback_id"]))


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _clean(value).lower() in {"1", "true", "yes", "y"}


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


def _snippet(value: Any, limit: int = 100) -> str:
    text = _clean(value)
    return text if len(text) <= limit else text[: limit - 1] + "..."


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _yes_no(value: Any) -> str:
    return "yes" if bool(value) else "no"


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))
