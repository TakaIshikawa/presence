"""Flag newsletter CTA copy whose URL destination does not match intent."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlsplit


ARTIFACT_TYPE = "newsletter_cta_destination_mismatch"
CTA_KINDS = ("subscribe", "reply", "read_more", "sponsor", "feedback")


def build_newsletter_cta_destination_mismatch_report(ctas: list[dict[str, Any]], *, now: datetime | None = None) -> dict[str, Any]:
    generated_at = _utc(now or datetime.now(timezone.utc))
    rows = []
    for cta in ctas:
        expected = _expected_kind(str(cta.get("cta_text") or cta.get("text") or ""))
        observed = _observed_kind(str(cta.get("destination_url") or cta.get("url") or ""), cta)
        if expected == "unknown" or expected == observed:
            continue
        severity = "high" if expected in {"subscribe", "sponsor", "reply"} else "medium"
        rows.append({"issue_id": cta.get("issue_id"), "cta_text": cta.get("cta_text") or cta.get("text"), "destination_url": cta.get("destination_url") or cta.get("url"), "expected_destination_kind": expected, "observed_destination_kind": observed, "severity": severity})
    rows.sort(key=lambda row: (str(row["issue_id"]), row["cta_text"] or ""))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": generated_at.isoformat(), "known_cta_kinds": list(CTA_KINDS), "totals": {"cta_count": len(ctas), "row_count": len(rows)}, "rows": rows}


def build_newsletter_cta_destination_mismatch_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = "newsletter_ctas" if "newsletter_ctas" in schema else "newsletter_links" if "newsletter_links" in schema else None
    if table is None:
        return build_newsletter_cta_destination_mismatch_report([], **kwargs) | {"missing_tables": ["newsletter_ctas"]}
    cols = schema[table]
    rows = [dict(row) for row in conn.execute(f"SELECT {_expr(cols, ('issue_id',), 'NULL')} AS issue_id, {_expr(cols, ('cta_text','text','label'), 'NULL')} AS cta_text, {_expr(cols, ('destination_url','url','href'), 'NULL')} AS destination_url, {_expr(cols, ('sponsor_domain',), 'NULL')} AS sponsor_domain FROM {table} ORDER BY rowid")]
    return build_newsletter_cta_destination_mismatch_report(rows, **kwargs) | {"missing_tables": []}


def format_newsletter_cta_destination_mismatch_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_cta_destination_mismatch_text(report: dict[str, Any]) -> str:
    lines = ["Newsletter CTA Destination Mismatch", f"Generated: {report['generated_at']}", f"Rows: {report['totals']['row_count']}"]
    lines.extend(f"{row['issue_id']} | {row['cta_text']} | {row['expected_destination_kind']} -> {row['observed_destination_kind']} | {row['severity']}" for row in report["rows"])
    return "\n".join(lines)


def _expected_kind(text: str) -> str:
    lowered = text.lower()
    if re.search(r"\bsubscribe|join\b", lowered):
        return "subscribe"
    if re.search(r"\breply|respond\b", lowered):
        return "reply"
    if re.search(r"\bread more|continue|full story\b", lowered):
        return "read_more"
    if re.search(r"\bsponsor|partner\b", lowered):
        return "sponsor"
    if re.search(r"\bfeedback|survey|rate\b", lowered):
        return "feedback"
    return "unknown"


def _observed_kind(url: str, cta: dict[str, Any]) -> str:
    lowered = url.lower()
    host = urlsplit(url).netloc.lower()
    sponsor = str(cta.get("sponsor_domain") or "").lower()
    if sponsor and sponsor in host:
        return "sponsor"
    if "subscribe" in lowered or "signup" in lowered or "join" in lowered:
        return "subscribe"
    if lowered.startswith("mailto:") or "reply" in lowered:
        return "reply"
    if "feedback" in lowered or "survey" in lowered:
        return "feedback"
    if "archive" in lowered or "/p/" in lowered or "/posts/" in lowered or "/blog/" in lowered:
        return "read_more"
    return "home"


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    if isinstance(db_or_conn, sqlite3.Connection):
        db_or_conn.row_factory = sqlite3.Row
        return db_or_conn
    conn = sqlite3.connect(db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _expr(columns: set[str], names: tuple[str, ...], fallback: str) -> str:
    return next((name for name in names if name in columns), fallback)
