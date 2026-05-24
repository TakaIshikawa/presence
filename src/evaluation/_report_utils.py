"""Small helpers for deterministic SQLite-backed evaluation reports."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


def connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def expr(columns: set[str], *names: str, default: str = "NULL", alias: str | None = None, out: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    for name in names:
        if name in columns:
            value = f"{prefix}{name}"
            return f"{value} AS {out}" if out else value
    return f"{default} AS {out}" if out else default


def clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def lower(value: Any, default: str = "") -> str:
    return clean(value, default).lower()


def dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def iso(value: Any) -> str | None:
    parsed = dt(value)
    return parsed.isoformat() if parsed else (clean(value) or None)


def now_iso(now: datetime | None = None) -> str:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def loads_obj(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def loads_list(value: Any) -> list[Any]:
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def json_dumps(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True)


def positive(name: str, value: int | float) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def nonnegative(name: str, value: int | float) -> None:
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return round(float(ordered[mid]), 4)
    return round((ordered[mid - 1] + ordered[mid]) / 2, 4)


def domain(value: Any) -> str:
    text = clean(value).lower()
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = (parsed.hostname or text).lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def table_missing_columns(columns: set[str], required: set[str]) -> list[str]:
    return sorted(required - columns)
