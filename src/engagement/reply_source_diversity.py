"""Detect reply drafts repeatedly grounded in the same source or domain."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 14
DEFAULT_CONCENTRATION_THRESHOLD = 0.6


def build_reply_source_diversity_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    concentration_threshold: float = DEFAULT_CONCENTRATION_THRESHOLD,
    now: datetime | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if not 0 < concentration_threshold <= 1:
        raise ValueError("concentration_threshold must be between 0 and 1")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {"days": days, "concentration_threshold": concentration_threshold, "since": cutoff.isoformat()}
    if "reply_draft_sources" not in schema:
        return _report(generated_at, filters, [], 0, missing_tables=["reply_draft_sources"])
    rows = [_normalize(dict(row)) for row in _load_rows(conn, schema, cutoff)]
    reply_ids = sorted({row["reply_id"] for row in rows})
    findings = []
    for dimension in ("source", "domain"):
        counts: Counter[str] = Counter(row[dimension] for row in rows if row[dimension])
        total = sum(counts.values())
        for value, count in sorted(counts.items(), key=lambda pair: (-pair[1], pair[0])):
            share = count / total if total else 0
            if share < concentration_threshold:
                continue
            affected = sorted({row["reply_id"] for row in rows if row[dimension] == value})
            findings.append(
                {
                    "dimension": dimension,
                    "value": value,
                    "count": count,
                    "share": round(share, 4),
                    "affected_reply_ids": affected,
                    "severity": "high" if share >= 0.85 else "medium",
                }
            )
    findings.sort(key=lambda item: (-item["share"], item["dimension"], item["value"]))
    return _report(generated_at, filters, findings, len(reply_ids))


def format_reply_source_diversity_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_source_diversity_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Source Diversity",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days threshold={report['filters']['concentration_threshold']}",
        f"Totals: replies={report['totals']['reply_count']} findings={report['totals']['finding_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No repeated reply source concentration found.")
        return "\n".join(lines)
    for item in report["findings"]:
        lines.append(
            f"  - {item['dimension']}={item['value']} count={item['count']} "
            f"share={item['share']:.2f} severity={item['severity']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime) -> list[sqlite3.Row]:
    cols = schema["reply_draft_sources"]
    drafted = _expr(cols, "drafted_at", fallback="created_at")
    return conn.execute(
        f"""SELECT {_expr(cols, 'reply_id', fallback='draft_id')}, {_expr(cols, 'source_id', fallback='source')},
                  {_expr(cols, 'url')}, {drafted}
            FROM reply_draft_sources
            WHERE {drafted} IS NULL OR {drafted} >= ?""",
        [cutoff.isoformat()],
    ).fetchall()


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    url = _clean(row.get("url"))
    return {
        "reply_id": _clean(row.get("reply_id") or row.get("draft_id")),
        "source": _clean(row.get("source_id") or row.get("source") or url),
        "domain": _domain(url),
    }


def _report(generated_at: datetime, filters: dict[str, Any], findings: list[dict[str, Any]], reply_count: int, *, missing_tables: list[str] | None = None) -> dict[str, Any]:
    return {
        "artifact_type": "reply_source_diversity",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"reply_count": reply_count, "finding_count": len(findings)},
        "findings": findings,
        "missing_tables": missing_tables or [],
    }


def _expr(cols: set[str], name: str, *, fallback: str | None = None) -> str:
    if name in cols:
        return name
    if fallback and fallback in cols:
        return f"{fallback} AS {name}"
    return f"NULL AS {name}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _domain(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    return parsed.netloc.removeprefix("www.").lower()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
