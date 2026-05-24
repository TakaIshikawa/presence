"""Small helpers for deterministic SQLite-backed batch reports."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import re
import sqlite3
from typing import Any, Iterable


def connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        row[0]: {col[1] for col in conn.execute(f"PRAGMA table_info({row[0]})").fetchall()}
        for row in rows
    }


def first_table(sch: dict[str, set[str]], names: Iterable[str]) -> str | None:
    return next((name for name in names if name in sch), None)


def pick(cols: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in cols:
            return name
    return default


def coalesce(cols: set[str], *names: str, default: str = "NULL") -> str:
    found = [name for name in names if name in cols]
    if len(found) > 1:
        return f"COALESCE({', '.join(found)})"
    return found[0] if found else default


def parse_time(value: Any) -> datetime | None:
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def utc_now(now: datetime | None = None) -> datetime:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc)


def text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def json_load(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    raw = text(value)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def dump_json(payload: dict[str, Any] | list[dict[str, Any]]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True)


def csv_rows(rows: list[dict[str, Any]], fieldnames: list[str]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _csv_value(row.get(key)) for key in fieldnames})
    return out.getvalue()


def digest(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, default=str) if not isinstance(value, str) else value
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


SECRET_RE = re.compile(r"(?i)(api[_-]?key|token|secret|password|authorization)\s*[:=]\s*['\"]?[^,'\"\s}]+")


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: ("[REDACTED]" if _secret_key(k) else redact(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v) for v in value]
    return SECRET_RE.sub(lambda m: m.group(1) + "=[REDACTED]", str(value)) if isinstance(value, str) else value


def _secret_key(key: str) -> bool:
    lowered = key.lower()
    return any(part in lowered for part in ("secret", "token", "password", "api_key", "apikey", "authorization"))


def _csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return "" if value is None else value
