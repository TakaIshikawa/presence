"""Report whether selected newsletter subjects are fulfilled by sources and clicks."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_MIN_OVERLAP = 0.25
TOKEN_RE = re.compile(r"[a-z0-9]+")


def build_newsletter_subject_promise_fulfillment_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_overlap: float = DEFAULT_MIN_OVERLAP,
    now: datetime | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if min_overlap < 0 or min_overlap > 1:
        raise ValueError("min_overlap must be between 0 and 1")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "min_overlap": min_overlap, "window_start": cutoff.isoformat(), "window_end": generated_at.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if _required_table_gaps(missing_tables) or _required_column_gaps(missing_columns):
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    content = _load_content(conn, schema)
    clicks = _load_clicks(conn, schema, content)
    rows = _load_subject_rows(conn, schema, cutoff, generated_at)
    items = []
    for row in rows:
        subject = row["subject"] or ""
        source_ids = _parse_ids(row.get("source_content_ids"))
        source_text = " ".join(content.get(content_id, {}).get("text", "") for content_id in source_ids)
        subject_tokens = _tokens(subject)
        source_tokens = _tokens(source_text)
        overlap = len(subject_tokens & source_tokens) / len(subject_tokens) if subject_tokens else 0.0
        clicked_content = clicks.get(int(row["newsletter_send_id"]), set())
        risk = "ok"
        if overlap < min_overlap and not clicked_content:
            risk = "high"
        elif overlap < min_overlap or not clicked_content:
            risk = "medium"
        items.append(
            {
                "newsletter_send_id": int(row["newsletter_send_id"]),
                "issue_id": row.get("issue_id"),
                "sent_at": _iso(row.get("sent_at")),
                "selected_subject": subject,
                "overlap_score": round(overlap, 3),
                "clicked_content_count": len(clicked_content),
                "risk_level": risk,
                "source_content_ids": source_ids,
                "examples": [
                    {
                        "content_id": content_id,
                        "title": content.get(content_id, {}).get("title"),
                        "clicked": content_id in clicked_content,
                    }
                    for content_id in source_ids[:3]
                ],
            }
        )
    items.sort(key=lambda item: ({"high": 0, "medium": 1, "ok": 2}[item["risk_level"]], item["sent_at"] or "", item["newsletter_send_id"]))
    risk_counts = Counter(item["risk_level"] for item in items)
    return {
        "artifact_type": "newsletter_subject_promise_fulfillment",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "issue_count": len(items),
            "high_risk_count": risk_counts.get("high", 0),
            "medium_risk_count": risk_counts.get("medium", 0),
            "ok_count": risk_counts.get("ok", 0),
        },
        "items": items,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_newsletter_subject_promise_fulfillment_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subject_promise_fulfillment_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Subject Promise Fulfillment",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} min_overlap={report['filters']['min_overlap']}",
        f"Totals: issues={report['totals']['issue_count']} high={report['totals']['high_risk_count']} medium={report['totals']['medium_risk_count']} ok={report['totals']['ok_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["items"]:
        lines.append("No selected newsletter subjects matched.")
        return "\n".join(lines)
    lines.append("")
    for item in report["items"]:
        lines.append(
            f"- send={item['newsletter_send_id']} issue={item['issue_id'] or '-'} risk={item['risk_level']} "
            f"overlap={item['overlap_score']} clicked_content={item['clicked_content_count']}: {item['selected_subject']}"
        )
    return "\n".join(lines)


format_newsletter_subject_promise_fulfillment_table = format_newsletter_subject_promise_fulfillment_text


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    required = {
        "newsletter_sends": {"id", "sent_at", "source_content_ids"},
        "newsletter_subject_candidates": {"newsletter_send_id", "subject"},
        "generated_content": {"id"},
    }
    optional = {"newsletter_link_clicks": {"newsletter_send_id"}}
    missing_tables = [table for table in (*required, *optional) if table not in schema]
    missing_columns = {
        table: sorted(columns - schema[table])
        for table, columns in {**required, **optional}.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _required_column_gaps(missing_columns: dict[str, list[str]]) -> bool:
    return any(table != "newsletter_link_clicks" for table in missing_columns)


def _required_table_gaps(missing_tables: list[str]) -> bool:
    return any(table != "newsletter_link_clicks" for table in missing_tables)


def _load_subject_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime, generated_at: datetime) -> list[dict[str, Any]]:
    sc = schema["newsletter_subject_candidates"]
    send_cols = schema["newsletter_sends"]
    selected_filter = "COALESCE(nsc.selected, 0) = 1" if "selected" in sc else "1 = 1"
    issue_expr = "ns.issue_id" if "issue_id" in send_cols else "NULL AS issue_id"
    status_filter = "LOWER(COALESCE(ns.status, 'sent')) = 'sent'" if "status" in send_cols else "1 = 1"
    rows = conn.execute(
        f"""SELECT ns.id AS newsletter_send_id, {issue_expr}, ns.sent_at, ns.source_content_ids, nsc.subject
            FROM newsletter_subject_candidates nsc
            JOIN newsletter_sends ns ON ns.id = nsc.newsletter_send_id
            WHERE {selected_filter}
              AND {status_filter}
              AND datetime(ns.sent_at) >= datetime(?)
              AND datetime(ns.sent_at) <= datetime(?)
            ORDER BY datetime(ns.sent_at) DESC, ns.id ASC""",
        (cutoff.isoformat(), generated_at.isoformat()),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, dict[str, str]]:
    cols = schema["generated_content"]
    title_expr = "title" if "title" in cols else ("content_type" if "content_type" in cols else "NULL")
    text_parts = [col for col in ("title", "content", "body", "summary", "final_text") if col in cols]
    selected = ", ".join(["id", f"{title_expr} AS title", *text_parts])
    rows = conn.execute(f"SELECT {selected} FROM generated_content ORDER BY id ASC").fetchall()
    return {
        int(row["id"]): {
            "title": row["title"] or "",
            "text": " ".join(str(row[col] or "") for col in text_parts),
        }
        for row in rows
    }


def _load_clicks(conn: sqlite3.Connection, schema: dict[str, set[str]], content: dict[int, dict[str, str]]) -> dict[int, set[int]]:
    if "newsletter_link_clicks" not in schema or "newsletter_send_id" not in schema["newsletter_link_clicks"]:
        return {}
    cols = schema["newsletter_link_clicks"]
    if "content_id" in cols:
        rows = conn.execute("SELECT newsletter_send_id, content_id FROM newsletter_link_clicks WHERE content_id IS NOT NULL").fetchall()
        return _click_map((int(row["newsletter_send_id"]), int(row["content_id"])) for row in rows)
    if "link_url" not in cols:
        return {}
    rows = conn.execute("SELECT newsletter_send_id, link_url FROM newsletter_link_clicks WHERE link_url IS NOT NULL").fetchall()
    pairs = []
    for row in rows:
        url = str(row["link_url"])
        for content_id in content:
            if str(content_id) in url:
                pairs.append((int(row["newsletter_send_id"]), content_id))
    return _click_map(pairs)


def _click_map(pairs: Any) -> dict[int, set[int]]:
    result: dict[int, set[int]] = {}
    for send_id, content_id in pairs:
        result.setdefault(send_id, set()).add(content_id)
    return result


def _empty_report(generated_at: datetime, filters: dict[str, Any], missing_tables: list[str], missing_columns: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "artifact_type": "newsletter_subject_promise_fulfillment",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"issue_count": 0, "high_risk_count": 0, "medium_risk_count": 0, "ok_count": 0},
        "items": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _parse_ids(value: Any) -> list[int]:
    if value is None:
        return []
    if isinstance(value, bytes):
        value = value.decode()
    found = re.findall(r"\d+", str(value))
    return [int(item) for item in found]


def _tokens(value: str) -> set[str]:
    stop = {"the", "and", "for", "with", "your", "this", "that", "from"}
    return {token for token in TOKEN_RE.findall(value.lower()) if len(token) > 2 and token not in stop}


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


def _iso(value: Any) -> str | None:
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00"))).isoformat()
    except (TypeError, ValueError):
        return None


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
