"""Detect repeated newsletter segments, openings, and source clusters."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LOOKBACK = 6
DEFAULT_MIN_REPEAT = 2
OPENING_WORDS = 8


def build_newsletter_segment_fatigue_report(
    rows: list[dict[str, Any]],
    *,
    lookback: int = DEFAULT_LOOKBACK,
    min_repeat: int = DEFAULT_MIN_REPEAT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a fatigue report from newsletter issue rows."""
    if lookback <= 0:
        raise ValueError("lookback must be positive")
    if min_repeat <= 1:
        raise ValueError("min_repeat must be greater than 1")

    generated_at = _utc(now or datetime.now(timezone.utc))
    issues = [_issue(row) for row in rows]
    issues.sort(key=lambda row: (row["sent_at"] or "", row["issue_id"]), reverse=True)
    issues = issues[:lookback]

    segment_counter: Counter[str] = Counter()
    opening_counter: Counter[str] = Counter()
    source_counter: Counter[tuple[str, ...]] = Counter()
    segment_issues: dict[str, list[str]] = defaultdict(list)
    opening_issues: dict[str, list[str]] = defaultdict(list)
    source_issues: dict[tuple[str, ...], list[str]] = defaultdict(list)

    for issue in issues:
        for segment in issue["segments"]:
            segment_counter[segment] += 1
            segment_issues[segment].append(issue["issue_id"])
        if issue["opening"]:
            opening_counter[issue["opening"]] += 1
            opening_issues[issue["opening"]].append(issue["issue_id"])
        if issue["source_cluster"]:
            cluster = tuple(issue["source_cluster"])
            source_counter[cluster] += 1
            source_issues[cluster].append(issue["issue_id"])

    repeated_segments = _repeated_payload(segment_counter, segment_issues, "segment", min_repeat)
    repeated_openings = _repeated_payload(opening_counter, opening_issues, "opening", min_repeat)
    repeated_source_clusters = [
        {
            "sources": list(cluster),
            "repeat_count": count,
            "issue_ids": source_issues[cluster],
        }
        for cluster, count in sorted(source_counter.items(), key=lambda item: (-item[1], item[0]))
        if count >= min_repeat
    ]

    issue_count = max(len(issues), 1)
    fatigue_score = round(
        min(
            1.0,
            (
                sum(row["repeat_count"] - 1 for row in repeated_segments)
                + sum(row["repeat_count"] - 1 for row in repeated_openings)
                + sum(row["repeat_count"] - 1 for row in repeated_source_clusters)
            )
            / issue_count,
        ),
        3,
    )

    recommendations = _recommendations(repeated_segments, repeated_openings, repeated_source_clusters)
    return {
        "artifact_type": "newsletter_segment_fatigue",
        "generated_at": generated_at.isoformat(),
        "filters": {"lookback": lookback, "min_repeat": min_repeat},
        "totals": {
            "issue_count": len(issues),
            "repeated_segment_count": len(repeated_segments),
            "repeated_opening_count": len(repeated_openings),
            "repeated_source_cluster_count": len(repeated_source_clusters),
        },
        "fatigue_score": fatigue_score,
        "repeated_segments": repeated_segments,
        "repeated_openings": repeated_openings,
        "repeated_source_clusters": repeated_source_clusters,
        "recommendations": recommendations,
    }


def build_newsletter_segment_fatigue_report_from_db(
    db_or_conn: Any,
    *,
    lookback: int = DEFAULT_LOOKBACK,
    min_repeat: int = DEFAULT_MIN_REPEAT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load recent newsletter draft/archive rows from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_db_rows(conn, schema, lookback)
    report = build_newsletter_segment_fatigue_report(
        rows,
        lookback=lookback,
        min_repeat=min_repeat,
        now=now,
    )
    report["missing_tables"] = [
        table
        for table in ("newsletter_sends", "newsletter_drafts")
        if table not in schema
    ]
    return report


def format_newsletter_segment_fatigue_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_segment_fatigue_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Newsletter Segment Fatigue",
        f"Generated: {report['generated_at']}",
        f"Lookback: {report['filters']['lookback']} issues min_repeat={report['filters']['min_repeat']}",
        (
            f"Totals: issues={totals['issue_count']} repeated_segments={totals['repeated_segment_count']} "
            f"repeated_openings={totals['repeated_opening_count']} "
            f"repeated_source_clusters={totals['repeated_source_cluster_count']}"
        ),
        f"Fatigue score: {report['fatigue_score']:.3f}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing optional tables: " + ", ".join(report["missing_tables"]))
    for key, title in (
        ("repeated_segments", "Repeated segments"),
        ("repeated_openings", "Repeated openings"),
    ):
        if report[key]:
            lines.append(title + ":")
            for row in report[key]:
                lines.append(f"- {row['value']}: count={row['repeat_count']} issues={','.join(row['issue_ids'])}")
    if report["repeated_source_clusters"]:
        lines.append("Repeated source clusters:")
        for row in report["repeated_source_clusters"]:
            lines.append(
                f"- {' + '.join(row['sources'])}: count={row['repeat_count']} issues={','.join(row['issue_ids'])}"
            )
    if report["recommendations"]:
        lines.append("Recommendations:")
        lines.extend(f"- {item}" for item in report["recommendations"])
    if not report["recommendations"]:
        lines.append("No newsletter segment fatigue found.")
    return "\n".join(lines)


def _load_db_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], lookback: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table in ("newsletter_sends", "newsletter_drafts", "newsletter_archive"):
        if table not in schema:
            continue
        columns = schema[table]
        body_col = _first_column(columns, "body", "content", "html", "markdown", "text")
        metadata_col = _first_column(columns, "metadata", "raw_metadata")
        sent_col = _first_column(columns, "sent_at", "published_at", "created_at", "updated_at")
        selected = [
            _select_expr(table, _first_column(columns, "issue_id", "id", "newsletter_send_id"), "issue_id"),
            _select_expr(table, _first_column(columns, "subject", "title"), "subject"),
            _select_expr(table, body_col, "body"),
            _select_expr(table, _first_column(columns, "source_content_ids", "source_ids"), "source_content_ids"),
            _select_expr(table, metadata_col, "metadata"),
            _select_expr(table, sent_col, "sent_at"),
        ]
        order = f"ORDER BY {sent_col} DESC" if sent_col else ""
        rows.extend(dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table} {order} LIMIT ?", (lookback,)))
    return rows


def _issue(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_object(_first(row, "metadata", "raw_metadata"))
    body = _text(_first(row, "body", "content", "html", "markdown", "text"))
    segments = _segments(row, metadata, body)
    opening = _opening(row, metadata, body)
    sources = _sources(row, metadata)
    issue_id = _text(_first(row, "issue_id", "id", "newsletter_send_id")) or _text(_first(row, "subject", "title")) or "unknown"
    return {
        "issue_id": issue_id,
        "sent_at": _datetime_text(_first(row, "sent_at", "published_at", "created_at")),
        "segments": segments,
        "opening": opening,
        "source_cluster": sources,
    }


def _segments(row: dict[str, Any], metadata: dict[str, Any], body: str) -> list[str]:
    values = _listish(_first(row, "segments", "segment_titles", "section_titles") or metadata.get("segments"))
    if values:
        return sorted({_normalize_label(value) for value in values if _normalize_label(value)})
    headings = re.findall(r"^\s{0,3}#{1,3}\s+(.+?)\s*$", body, flags=re.MULTILINE)
    return sorted({_normalize_label(value) for value in headings if _normalize_label(value)})


def _opening(row: dict[str, Any], metadata: dict[str, Any], body: str) -> str:
    explicit = _normalize_phrase(_first(row, "opening", "intro", "intro_phrase") or metadata.get("opening"))
    if explicit:
        return explicit
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", body)).strip()
    sentence = re.split(r"(?<=[.!?])\s+", text, maxsplit=1)[0] if text else ""
    words = sentence.split()[:OPENING_WORDS]
    return _normalize_phrase(" ".join(words))


def _sources(row: dict[str, Any], metadata: dict[str, Any]) -> list[str]:
    value = _first(row, "source_cluster", "sources", "source_content_ids", "source_ids") or metadata.get("sources")
    sources = [_normalize_label(item) for item in _listish(value)]
    return sorted({source for source in sources if source})


def _repeated_payload(
    counter: Counter[str],
    issue_map: dict[str, list[str]],
    key: str,
    min_repeat: int,
) -> list[dict[str, Any]]:
    return [
        {key: value, "value": value, "repeat_count": count, "issue_ids": issue_map[value]}
        for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
        if value and count >= min_repeat
    ]


def _recommendations(segments: list[dict[str, Any]], openings: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> list[str]:
    recommendations = []
    if segments:
        recommendations.append("Rotate or rename recurring segment labels in the next newsletter issue.")
    if openings:
        recommendations.append("Rewrite issue introductions so the opening phrase changes across sends.")
    if clusters:
        recommendations.append("Diversify source clusters before reusing the same bundle again.")
    return recommendations


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _first_column(columns: set[str], *names: str) -> str | None:
    return next((name for name in names if name in columns), None)


def _select_expr(table: str, column: str | None, output: str, fallback: str = "NULL") -> str:
    return f"{table}.{column} AS {output}" if column else f"{fallback} AS {output}"


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not _text(value):
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _listish(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [part.strip() for part in re.split(r"[,|]", value) if part.strip()]
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    return [value]


def _normalize_label(value: Any) -> str:
    return re.sub(r"\s+", " ", _text(value)).strip().lower()


def _normalize_phrase(value: Any) -> str:
    phrase = re.sub(r"[^a-z0-9 ]+", "", _normalize_label(value))
    return re.sub(r"\s+", " ", phrase).strip()


def _datetime_text(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else _text(value)


def _parse_datetime(value: Any) -> datetime | None:
    if not _text(value):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *names: str) -> Any:
    return next((row[name] for name in names if name in row and row[name] is not None), None)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()
