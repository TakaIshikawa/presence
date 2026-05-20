"""Report prompt versions used without recent evaluation coverage."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 100
ISSUE_TYPES = ("missing_recent_evaluation", "unknown_prompt_hash")


def build_prompt_version_evaluation_coverage_report(
    prompt_rows: list[dict[str, Any]],
    eval_result_rows: list[dict[str, Any]],
    usage_rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    prompts_by_id = {_clean(row.get("prompt_version_id")): row for row in prompt_rows if _clean(row.get("prompt_version_id"))}
    prompts_by_hash = {_clean(row.get("prompt_hash")): row for row in prompt_rows if _clean(row.get("prompt_hash"))}
    covered_ids: set[str] = set()
    covered_hashes: set[str] = set()
    for row in eval_result_rows:
        created_at = _parse_time(row.get("created_at"))
        if created_at is not None and created_at < cutoff:
            continue
        prompt_id = _clean(row.get("prompt_version_id"))
        prompt_hash = _clean(row.get("prompt_hash"))
        if prompt_id:
            covered_ids.add(prompt_id)
        if prompt_hash:
            covered_hashes.add(prompt_hash)

    activity: dict[str, dict[str, Any]] = {}
    unknown_hash_counts: Counter[str] = Counter()
    for row in usage_rows:
        created_at = _parse_time(row.get("created_at"))
        if created_at is not None and created_at < cutoff:
            continue
        prompt_id = _clean(row.get("prompt_version_id"))
        prompt_hash = _clean(row.get("prompt_hash"))
        key = prompt_id or (f"hash:{prompt_hash}" if prompt_hash else "")
        if key:
            bucket = activity.setdefault(key, {"usage_count": 0, "latest_usage_at": None, "prompt_version_id": prompt_id or None, "prompt_hash": prompt_hash or None})
            bucket["usage_count"] += _to_int(row.get("usage_count"), default=1)
            bucket["latest_usage_at"] = _max_iso(bucket["latest_usage_at"], created_at)
        if prompt_hash and prompt_hash not in prompts_by_hash:
            unknown_hash_counts[prompt_hash] += 1

    issues: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for key, bucket in activity.items():
        prompt_id = bucket["prompt_version_id"]
        prompt_hash = bucket["prompt_hash"]
        prompt = prompts_by_id.get(prompt_id or "") or prompts_by_hash.get(prompt_hash or "") or {}
        resolved_id = _clean(prompt.get("prompt_version_id") or prompt_id)
        resolved_hash = _clean(prompt.get("prompt_hash") or prompt_hash)
        if resolved_id and (resolved_id in covered_ids or resolved_hash in covered_hashes):
            continue
        if not resolved_id and resolved_hash not in prompts_by_hash:
            continue
        issues.append(
            {
                "issue_type": "missing_recent_evaluation",
                "prompt_version_id": resolved_id or None,
                "prompt_hash": resolved_hash or None,
                "prompt_type": _clean(prompt.get("prompt_type")) or None,
                "prompt_version": _clean(prompt.get("prompt_version")) or None,
                "usage_count": bucket["usage_count"],
                "latest_usage_at": bucket["latest_usage_at"],
                "lookback_start": cutoff.isoformat(),
            }
        )
        counts["missing_recent_evaluation"] += 1

    for prompt_hash, seen_count in sorted(unknown_hash_counts.items()):
        issues.append(
            {
                "issue_type": "unknown_prompt_hash",
                "prompt_version_id": None,
                "prompt_hash": prompt_hash,
                "usage_count": seen_count,
                "latest_usage_at": None,
                "lookback_start": cutoff.isoformat(),
            }
        )
        counts["unknown_prompt_hash"] += 1

    issues.sort(key=lambda item: (ISSUE_TYPES.index(item["issue_type"]), -(item.get("usage_count") or 0), item.get("prompt_hash") or "", item.get("prompt_version_id") or ""))
    shown = issues[:limit]
    return {
        "artifact_type": "prompt_version_evaluation_coverage",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"lookback_days": lookback_days, "limit": limit},
        "summary": {
            "prompt_version_count": len(prompt_rows),
            "usage_reference_count": len(usage_rows),
            "eval_result_count": len(eval_result_rows),
            "issue_count": len(issues),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
        },
        "missing_tables": sorted(missing_tables or []),
        "issue_items": shown,
        "empty_state": {"is_empty": not issues, "message": "No prompt version evaluation coverage issues found." if not issues else None},
    }


def build_prompt_version_evaluation_coverage_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing = sorted(table for table in ("prompt_versions", "eval_batches", "eval_results", "model_usage", "engagement_predictions") if table not in schema)
    return build_prompt_version_evaluation_coverage_report(
        _load_prompts(conn, schema) if "prompt_versions" in schema else [],
        _load_eval_results(conn, schema) if "eval_results" in schema else [],
        _load_usage(conn, schema),
        missing_tables=missing,
        **kwargs,
    )


def format_prompt_version_evaluation_coverage_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_prompt_version_evaluation_coverage_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Prompt Version Evaluation Coverage",
        f"Generated: {report['generated_at']}",
        f"Thresholds: lookback_days={report['thresholds']['lookback_days']} limit={report['thresholds']['limit']}",
        f"Totals: prompts={summary['prompt_version_count']} usage_refs={summary['usage_reference_count']} eval_results={summary['eval_result_count']} issues={summary['issue_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["issue_items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Issues:")
    for item in report["issue_items"]:
        lines.append(f"  - {item['issue_type']} prompt_id={item.get('prompt_version_id') or '-'} hash={item.get('prompt_hash') or '-'} usage={item.get('usage_count') or 0}")
    return "\n".join(lines)


def _load_prompts(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["prompt_versions"]
    select = [
        _expr(cols, "id", "prompt_version_id", fallback="NULL") + " AS prompt_version_id",
        _expr(cols, "prompt_hash", "hash", fallback="NULL") + " AS prompt_hash",
        _expr(cols, "prompt_type", "type", fallback="NULL") + " AS prompt_type",
        _expr(cols, "prompt_version", "version", fallback="NULL") + " AS prompt_version",
    ]
    order = _expr(cols, "id", "prompt_version_id", fallback="rowid")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM prompt_versions ORDER BY {order} ASC").fetchall()]


def _load_eval_results(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["eval_results"]
    select = [
        _expr(cols, "prompt_version_id", fallback="NULL") + " AS prompt_version_id",
        _expr(cols, "prompt_hash", fallback="NULL") + " AS prompt_hash",
        _expr(cols, "created_at", "evaluated_at", fallback="NULL") + " AS created_at",
    ]
    if "eval_batches" in schema and "batch_id" in cols and "id" in schema["eval_batches"]:
        batch_cols = schema["eval_batches"]
        select[2] = "COALESCE(" + _expr(cols, "created_at", "evaluated_at", fallback="NULL", alias="er") + ", " + _expr(batch_cols, "created_at", "completed_at", fallback="NULL", alias="eb") + ") AS created_at"
        sql = f"SELECT {', '.join(select)} FROM eval_results er LEFT JOIN eval_batches eb ON eb.id = er.batch_id"
    else:
        sql = f"SELECT {', '.join(select)} FROM eval_results"
    return [dict(row) for row in conn.execute(sql).fetchall()]


def _load_usage(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if "model_usage" in schema:
        cols = schema["model_usage"]
        rows.extend(dict(row) for row in conn.execute(f"SELECT {_expr(cols, 'prompt_version_id', fallback='NULL')} AS prompt_version_id, {_expr(cols, 'prompt_hash', fallback='NULL')} AS prompt_hash, {_expr(cols, 'usage_count', fallback='1')} AS usage_count, {_expr(cols, 'created_at', fallback='NULL')} AS created_at FROM model_usage").fetchall())
    if "engagement_predictions" in schema:
        cols = schema["engagement_predictions"]
        rows.extend(dict(row) for row in conn.execute(f"SELECT {_expr(cols, 'prompt_version_id', fallback='NULL')} AS prompt_version_id, {_expr(cols, 'prompt_hash', fallback='NULL')} AS prompt_hash, 1 AS usage_count, {_expr(cols, 'created_at', fallback='NULL')} AS created_at FROM engagement_predictions").fetchall())
    return rows


def _expr(columns: set[str], *names: str, fallback: str, alias: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    found = [f"{prefix}{name}" for name in names if name in columns]
    if not found:
        return fallback
    return found[0] if len(found) == 1 else "COALESCE(" + ", ".join(found) + ")"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _max_iso(current: str | None, candidate: datetime | None) -> str | None:
    if candidate is None:
        return current
    candidate_iso = candidate.isoformat()
    return candidate_iso if current is None or candidate_iso > current else current


def _to_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
