"""Report publication attempts with missing or reused idempotency keys."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3
from typing import Any


ARTIFACT_TYPE = "publication_attempt_idempotency_key_reuse"
DEFAULT_LIMIT = 100
DEFAULT_PROVIDER = "all"
DEFAULT_LOOKBACK_DAYS: int | None = None


def build_publication_attempt_idempotency_key_reuse_report(
    attempt_rows: list[dict[str, Any]],
    *,
    provider: str = DEFAULT_PROVIDER,
    lookback_days: int | None = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a duplicate/missing idempotency-key report from attempt rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    if lookback_days is not None and lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    provider_filter = _clean(provider).lower() or DEFAULT_PROVIDER
    cutoff = generated_at - timedelta(days=lookback_days) if lookback_days else None
    filtered: list[dict[str, Any]] = []
    missing_findings: list[dict[str, Any]] = []
    keyed: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)

    for row in attempt_rows:
        row_provider = _clean(row.get("provider")).lower() or _clean(row.get("platform")).lower() or "unknown"
        if provider_filter != DEFAULT_PROVIDER and row_provider != provider_filter:
            continue
        attempted_at = _parse_time(row.get("attempted_at") or row.get("created_at") or row.get("published_at"))
        if cutoff and attempted_at and attempted_at < cutoff:
            continue
        item = _attempt_item(row, row_provider, attempted_at)
        filtered.append(item)
        if not item["idempotency_key"]:
            missing_findings.append({**item, "reason": "missing_idempotency_key"})
        else:
            keyed[(item["provider"], item["platform"], item["idempotency_key"])].append(item)

    reused_findings: list[dict[str, Any]] = []
    for (row_provider, platform, key), items in keyed.items():
        if len(items) < 2:
            continue
        content_ids = sorted({_clean(item["content_id"]) for item in items})
        fingerprints = sorted({item["request_payload_fingerprint"] for item in items})
        if len(content_ids) > 1 or len(fingerprints) > 1:
            reasons = []
            if len(content_ids) > 1:
                reasons.append("different_content_id")
            if len(fingerprints) > 1:
                reasons.append("different_request_payload")
            reused_findings.append(
                {
                    "reason": "reused_idempotency_key",
                    "provider": row_provider,
                    "platform": platform,
                    "idempotency_key": key,
                    "attempt_count": len(items),
                    "content_ids": content_ids,
                    "request_payload_fingerprints": fingerprints,
                    "reuse_reasons": reasons,
                    "attempt_ids": [item["attempt_id"] for item in sorted(items, key=_attempt_sort_key)],
                    "first_attempted_at": min(_clean(item["attempted_at"]) for item in items if _clean(item["attempted_at"])) or None,
                    "last_attempted_at": max(_clean(item["attempted_at"]) for item in items if _clean(item["attempted_at"])) or None,
                }
            )

    missing_findings.sort(key=lambda item: (item["provider"], item["platform"], _clean(item["attempted_at"]), _sortable(item["attempt_id"])))
    reused_findings.sort(key=lambda item: (-item["attempt_count"], item["provider"], item["platform"], item["idempotency_key"]))
    findings = [*reused_findings, *missing_findings]
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"provider": provider_filter, "lookback_days": lookback_days, "limit": limit},
        "totals": {
            "attempts": len(filtered),
            "missing_keys": len(missing_findings),
            "reused_keys": len(reused_findings),
            "shown_findings": len(shown),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not findings,
            "message": "No publication attempt idempotency key reuse found." if not findings else None,
        },
    }


def build_publication_attempt_idempotency_key_reuse_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("publication_attempts")
    if columns is None:
        return build_publication_attempt_idempotency_key_reuse_report([], missing_tables=["publication_attempts"], **kwargs)
    required_missing = sorted({"id"} - columns)
    optional_missing = sorted({"idempotency_key", "request_payload"} - columns)
    if required_missing:
        return build_publication_attempt_idempotency_key_reuse_report(
            [], missing_columns={"publication_attempts": required_missing + optional_missing}, **kwargs
        )
    return build_publication_attempt_idempotency_key_reuse_report(
        _load_attempts(conn, columns),
        missing_columns={"publication_attempts": optional_missing} if optional_missing else None,
        **kwargs,
    )


def format_publication_attempt_idempotency_key_reuse_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_idempotency_key_reuse_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Publication Attempt Idempotency Key Reuse",
        f"Generated: {report['generated_at']}",
        f"Provider: {report['filters']['provider']}",
        f"Lookback days: {report['filters']['lookback_days'] or 'all'}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: attempts={totals['attempts']} missing_keys={totals['missing_keys']} reused_keys={totals['reused_keys']} shown={totals['shown_findings']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "reason | provider | platform | key | details"])
    for item in report["findings"]:
        details = (
            f"attempts={item['attempt_count']} content_ids={','.join(item['content_ids'])}"
            if item["reason"] == "reused_idempotency_key"
            else f"attempt_id={item['attempt_id']} content_id={item['content_id']}"
        )
        lines.append(f"{item['reason']} | {item['provider']} | {item['platform']} | {item.get('idempotency_key') or '-'} | {details}")
    return "\n".join(lines)


def _load_attempts(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    expr = {
        "attempt_id": "id",
        "content_id": _column_expr(columns, "content_id", fallback="NULL"),
        "provider": _column_expr(columns, "provider", fallback="NULL"),
        "platform": _column_expr(columns, "platform", fallback="NULL"),
        "idempotency_key": _column_expr(columns, "idempotency_key", fallback="NULL"),
        "request_payload": _column_expr(columns, "request_payload", "request_body", "body", fallback="NULL"),
        "attempted_at": _column_expr(columns, "attempted_at", "created_at", "published_at", fallback="NULL"),
    }
    rows = conn.execute(
        f"""SELECT {expr['attempt_id']} AS attempt_id,
                  {expr['content_id']} AS content_id,
                  {expr['provider']} AS provider,
                  {expr['platform']} AS platform,
                  {expr['idempotency_key']} AS idempotency_key,
                  {expr['request_payload']} AS request_payload,
                  {expr['attempted_at']} AS attempted_at
           FROM publication_attempts
           ORDER BY datetime({expr['attempted_at']}) ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _attempt_item(row: dict[str, Any], provider: str, attempted_at: datetime | None) -> dict[str, Any]:
    platform = _clean(row.get("platform")).lower() or provider or "unknown"
    payload = row.get("request_payload")
    return {
        "attempt_id": row.get("attempt_id", row.get("id")),
        "content_id": row.get("content_id"),
        "provider": provider,
        "platform": platform,
        "idempotency_key": _clean(row.get("idempotency_key")),
        "request_payload_fingerprint": _fingerprint(payload),
        "attempted_at": attempted_at.isoformat() if attempted_at else _clean(row.get("attempted_at")) or None,
    }


def _fingerprint(value: Any) -> str:
    if value is None:
        text = ""
    else:
        try:
            parsed = json.loads(value) if isinstance(value, str) else value
            text = json.dumps(parsed, sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError):
            text = str(value)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _parse_time(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')")]
    return {table: {str(col[1]) for col in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _column_expr(columns: set[str], *candidates: str, fallback: str) -> str:
    return next((column for column in candidates if column in columns), fallback)


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _attempt_sort_key(item: dict[str, Any]) -> tuple[str, tuple[int, Any]]:
    return (_clean(item.get("attempted_at")), _sortable(item.get("attempt_id")))


def _sortable(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, _clean(value))


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
