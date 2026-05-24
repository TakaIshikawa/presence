"""Shared helpers for batch evaluation reports."""
from __future__ import annotations
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any
from urllib.parse import urlparse
import json, math, sqlite3, re


def connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(r[0]): {str(c[1]) for c in conn.execute(f"PRAGMA table_info({r[0]})")} for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def lower(value: Any, default: str = "") -> str:
    return clean(value, default).lower()


def to_int(value: Any, default: int = 0) -> int:
    try: return int(value)
    except (TypeError, ValueError): return default


def to_float(value: Any, default: float = 0.0) -> float:
    try: return float(value)
    except (TypeError, ValueError): return default


def dt(value: Any) -> datetime | None:
    if value in (None, ""): return None
    text = str(value).strip().replace("Z", "+00:00")
    for cand in (text, text.replace(" ", "T", 1)):
        try: parsed = datetime.fromisoformat(cand)
        except ValueError: continue
        if parsed.tzinfo is None: parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def now_value(now: datetime | None = None) -> datetime:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None: value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def now_iso(now: datetime | None = None) -> str:
    return now_value(now).isoformat()


def json_dumps(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True)


def positive(name: str, value: int | float) -> None:
    if value <= 0: raise ValueError(f"{name} must be positive")


def non_negative(name: str, value: int | float) -> None:
    if value < 0: raise ValueError(f"{name} must be non-negative")


def bounded_share(name: str, value: float) -> None:
    if value < 0 or value > 1: raise ValueError(f"{name} must be between 0 and 1")


def pick(cols: set[str], *names: str, default: str = "NULL", out: str | None = None) -> str:
    for name in names:
        if name in cols:
            return f"{name} AS {out}" if out else name
    return f"{default} AS {out}" if out else default


def load_table(conn: sqlite3.Connection, table: str, cols: set[str], mapping: dict[str, tuple[str, ...]], *, order: str = "rowid") -> list[dict[str, Any]]:
    select = []
    for out, names in mapping.items():
        default = "NULL"
        if out in {"count", "audience_size", "size_bytes", "expected_count", "attempt_count"}: default = "0"
        select.append(pick(cols, *names, default=default, out=out))
    return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY {order}")]


def schema_missing(cols: set[str], required: list[str], optional: list[str] | None = None) -> list[str]:
    return sorted([c for c in required + (optional or []) if c not in cols])


def domain(url_or_email: Any) -> str:
    text = clean(url_or_email).lower()
    if "@" in text and "://" not in text: text = text.rsplit("@",1)[1]
    parsed = urlparse(text if "://" in text else "//" + text)
    host = (parsed.hostname or text).lower().strip(".")
    if host.startswith("www."): host = host[4:]
    return host


def tokens(text: Any) -> set[str]:
    stop = {"the","and","for","with","from","your","you","our","into","that","this","are","was","were","a","an","to","of","in","on","by","is"}
    return {t for t in re.findall(r"[a-z0-9]+", clean(text).lower()) if len(t)>2 and t not in stop}


def jaccard(a: set[str], b: set[str]) -> float:
    return 0.0 if not a or not b else round(len(a & b) / len(a | b), 4)


def empty_state(findings: list[Any], message: str, *, schema_gap: bool = False) -> dict[str, Any]:
    return {"is_empty": not findings, "reason": "missing_schema" if schema_gap else ("no_findings" if not findings else None), "message": message if not findings else None}


def flatten_missing(missing_columns: dict[str, list[str]]) -> str:
    return "; ".join(f"{t}({', '.join(cols)})" for t, cols in sorted(missing_columns.items()) if cols)
