"""Export redacted publication replay bundles for offline failure debugging."""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any


VALID_PLATFORMS = {"all", "x", "bluesky"}
REDACTED = "[REDACTED]"

SECRET_KEY_PARTS = (
    "api_key",
    "apikey",
    "app_password",
    "authorization",
    "auth",
    "bearer",
    "client_secret",
    "cookie",
    "csrf",
    "jwt",
    "oauth",
    "password",
    "refresh_token",
    "secret",
    "session",
    "token",
)

AUTH_VALUE_RE = re.compile(r"\b(Bearer|Basic)\s+([^,\s;]+)", re.IGNORECASE)
COOKIE_VALUE_RE = re.compile(r"\bCookie\s*:\s*[^;\n\r]+(?:;[^\n\r]+)?", re.IGNORECASE)


def build_publication_replay_bundle(
    db_or_conn: Any,
    *,
    content_id: int | None = None,
    platform: str = "all",
    since: str | None = None,
    include_successful: bool = False,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic, JSON-serializable replay bundle."""
    if content_id is not None and content_id <= 0:
        raise ValueError("content_id must be positive")
    if platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")
    if since is not None and _parse_timestamp(since) is None:
        raise ValueError("since must be an ISO timestamp")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    current = _aware(generated_at or datetime.now(timezone.utc)).isoformat()
    attempts = _fetch_attempts(
        conn,
        schema,
        content_id=content_id,
        platform=platform,
        since=since,
        include_successful=include_successful,
    )
    content_ids = sorted({int(attempt["content_id"]) for attempt in attempts})

    return {
        "bundle_version": 1,
        "generated_at": current,
        "filters": {
            "content_id": content_id,
            "platform": platform,
            "since": since,
            "include_successful": include_successful,
        },
        "contents": [
            {
                "content": _fetch_content(conn, schema, row_content_id),
                "media": _fetch_media(conn, schema, row_content_id),
                "platform_states": _fetch_publication_states(
                    conn,
                    schema,
                    row_content_id,
                    platform=platform,
                ),
                "attempts": [
                    _serialize_attempt(attempt)
                    for attempt in attempts
                    if int(attempt["content_id"]) == row_content_id
                ],
                "selected_variants": _fetch_selected_variants(
                    conn,
                    schema,
                    row_content_id,
                    platform=platform,
                ),
            }
            for row_content_id in content_ids
        ],
    }


def publication_replay_bundle_to_json(bundle: dict[str, Any]) -> str:
    """Render a replay bundle as stable JSON."""
    return json.dumps(bundle, indent=2, sort_keys=True, default=str)


def redact_response_metadata(value: Any) -> Any:
    """Recursively redact secret-like keys and auth-bearing string values."""
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            text_key = str(key)
            if _is_secret_key(text_key):
                redacted[text_key] = REDACTED
            else:
                redacted[text_key] = redact_response_metadata(item)
        return redacted
    if isinstance(value, list):
        return [redact_response_metadata(item) for item in value]
    if isinstance(value, tuple):
        return [redact_response_metadata(item) for item in value]
    if isinstance(value, str):
        return _redact_auth_values(value)
    return value


def _fetch_attempts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    content_id: int | None,
    platform: str,
    since: str | None,
    include_successful: bool,
) -> list[dict[str, Any]]:
    columns = schema.get("publication_attempts")
    if not columns:
        return []

    filters: list[str] = []
    params: list[Any] = []
    if content_id is not None and "content_id" in columns:
        filters.append("content_id = ?")
        params.append(content_id)
    if platform != "all" and "platform" in columns:
        filters.append("platform = ?")
        params.append(platform)
    if since is not None and "attempted_at" in columns:
        filters.append("attempted_at >= ?")
        params.append(since)
    if not include_successful and "success" in columns:
        filters.append("success = 0")

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    order_clause = _order_by(columns, ["content_id", "platform", "attempted_at", "id"])
    rows = conn.execute(
        f"SELECT * FROM publication_attempts {where_clause} {order_clause}",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _fetch_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> dict[str, Any] | None:
    columns = schema.get("generated_content")
    if not columns:
        return None
    row = conn.execute(
        "SELECT * FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    if not row:
        return None
    content = dict(row)
    content.pop("content_embedding", None)
    for key in ("source_commits", "source_messages", "source_activity_ids"):
        if key in content:
            content[key] = _parse_json(content[key], fallback=[])
    return _json_ready(content)


def _fetch_media(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> dict[str, Any]:
    columns = schema.get("generated_content", set())
    media_keys = ["image_path", "image_prompt", "image_alt_text"]
    selected = [key for key in media_keys if key in columns]
    if not selected:
        return {}
    select_clause = ", ".join(selected)
    row = conn.execute(
        f"SELECT {select_clause} FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    if not row:
        return {}
    return {key: row[key] for key in selected}


def _fetch_publication_states(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
    *,
    platform: str,
) -> list[dict[str, Any]]:
    columns = schema.get("content_publications")
    if not columns or "content_id" not in columns:
        return []
    filters = ["content_id = ?"]
    params: list[Any] = [content_id]
    if platform != "all" and "platform" in columns:
        filters.append("platform = ?")
        params.append(platform)
    rows = conn.execute(
        f"""SELECT *
            FROM content_publications
            WHERE {' AND '.join(filters)}
            {_order_by(columns, ["platform", "id"])}""",
        params,
    ).fetchall()
    return [_json_ready(dict(row)) for row in rows]


def _fetch_selected_variants(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
    *,
    platform: str,
) -> list[dict[str, Any]]:
    columns = schema.get("content_variants")
    if not columns or not {"content_id", "selected"}.issubset(columns):
        return []
    filters = ["content_id = ?", "selected = 1"]
    params: list[Any] = [content_id]
    if platform != "all" and "platform" in columns:
        filters.append("platform = ?")
        params.append(platform)
    rows = conn.execute(
        f"""SELECT *
            FROM content_variants
            WHERE {' AND '.join(filters)}
            {_order_by(columns, ["platform", "variant_type", "id"])}""",
        params,
    ).fetchall()

    variants = []
    for row in rows:
        variant = dict(row)
        if "metadata" in variant:
            variant["metadata"] = _parse_json(variant["metadata"], fallback={})
        if "selected" in variant:
            variant["selected"] = bool(variant["selected"])
        variants.append(_json_ready(variant))
    return variants


def _serialize_attempt(attempt: dict[str, Any]) -> dict[str, Any]:
    serialized = dict(attempt)
    if "success" in serialized:
        serialized["success"] = bool(serialized["success"])
    if "response_metadata" in serialized:
        metadata = _parse_json(serialized["response_metadata"], fallback=serialized["response_metadata"])
        serialized["response_metadata"] = redact_response_metadata(metadata)
    return _json_ready(serialized)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _order_by(columns: set[str], preferred: list[str]) -> str:
    available = [column for column in preferred if column in columns]
    if not available:
        return ""
    return "ORDER BY " + ", ".join(available)


def _parse_json(value: Any, *, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"
    return value


def _is_secret_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(part in normalized for part in SECRET_KEY_PARTS)


def _redact_auth_values(value: str) -> str:
    value = AUTH_VALUE_RE.sub(lambda match: f"{match.group(1)} {REDACTED}", value)
    return COOKIE_VALUE_RE.sub(f"Cookie: {REDACTED}", value)


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
