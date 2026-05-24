"""Export replay seeds for failed publication attempts."""

from __future__ import annotations

from datetime import datetime, timedelta
import json
from typing import Any

from evaluation._batch_report_utils import connection, digest, dump_json, first_table, json_load, pick, redact, schema, text, utc_now

DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 100


def build_publication_failure_replay_seed_export_from_db(db_or_conn: Any, *, platform: str | None = None, retryable_only: bool = False, lookback_days: int = DEFAULT_LOOKBACK_DAYS, limit: int = DEFAULT_LIMIT, now: datetime | None = None) -> dict[str, Any]:
    if lookback_days <= 0 or limit <= 0:
        raise ValueError("lookback_days and limit must be positive")
    conn = connection(db_or_conn)
    generated_at = utc_now(now)
    sch = schema(conn)
    table = first_table(sch, ("publication_attempts", "content_publications"))
    missing_tables = [] if table else ["publication_attempts|content_publications"]
    rows = []
    if table:
        rows = _load(conn, table, sch[table], generated_at - timedelta(days=lookback_days))
    seeds = []
    for row in rows:
        if platform and text(row.get("platform")).lower() != platform.lower():
            continue
        status = text(row.get("status")).lower()
        if status not in {"failed", "failure", "error", "errored"}:
            continue
        reason = _reason(row)
        retryable = not any(term in reason for term in ("auth", "permission", "forbidden", "invalid"))
        if retryable_only and not retryable:
            continue
        payload = redact(json_load(row.get("payload")) or row.get("payload") or row.get("error"))
        seeds.append({"replay_seed_id": digest([row.get("attempt_id"), row.get("content_id"), reason]), "attempt_id": row.get("attempt_id"), "content_id": row.get("content_id"), "platform": text(row.get("platform")) or "unknown", "failure_reason": reason, "retryable": retryable, "payload_digest": digest(payload)})
    seeds.sort(key=lambda r: (r["platform"], str(r["attempt_id"])))
    return {"artifact_type": "publication_failure_replay_seed_export", "filters": {"platform": platform, "retryable_only": retryable_only, "lookback_days": lookback_days, "limit": limit}, "rows": seeds[:limit], "missing_tables": missing_tables, "empty_state": {"is_empty": not seeds, "message": "No failed publication replay seeds found." if not seeds and not missing_tables else None}}


def build_publication_failure_replay_seed_export(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return build_publication_failure_replay_seed_export_from_db(*args, **kwargs)


def format_publication_failure_replay_seed_export_json(export: dict[str, Any]) -> str:
    return dump_json(export)


def format_publication_failure_replay_seed_export_jsonl(export: dict[str, Any]) -> str:
    return "\n".join(json.dumps(row, sort_keys=True) for row in export["rows"]) + ("\n" if export["rows"] else "")


def _load(conn, table: str, cols: set[str], cutoff: datetime) -> list[dict[str, Any]]:
    ts = pick(cols, "created_at", "updated_at", "attempted_at", default="'1970-01-01T00:00:00+00:00'")
    select = [f"{pick(cols, 'id', 'attempt_id', default='rowid')} AS attempt_id", f"{pick(cols, 'content_id', 'generated_content_id', default='NULL')} AS content_id", f"{pick(cols, 'platform', 'channel', default='NULL')} AS platform", f"{pick(cols, 'status', default='NULL')} AS status", f"{pick(cols, 'error', 'error_message', 'failure_reason', default='NULL')} AS error", f"{pick(cols, 'payload', 'request_payload', 'metadata', default='NULL')} AS payload", f"{ts} AS created_at"]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} WHERE datetime({ts}) >= datetime(?) ORDER BY created_at ASC, attempt_id ASC", (cutoff.isoformat(),))]


def _reason(row: dict[str, Any]) -> str:
    raw = text(row.get("error")).lower()
    return "unknown_failure" if not raw else "_".join(raw.split())[:80]
