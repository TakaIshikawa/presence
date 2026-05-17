"""Report generated content rows missing source evidence."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
EVIDENCE_KINDS = ("source_commit", "source_content_ids", "source_activity_ids", "source_urls")


def build_generated_content_source_gap_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    content_type: str | None = None,
    status: str | None = None,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    findings = []
    scanned = 0
    for row in rows:
        created_at = _parse_dt(row.get("created_at"))
        if created_at and created_at < cutoff:
            continue
        ctype = _text(row.get("content_type"))
        row_status = _text(row.get("status") or row.get("publication_status"))
        if content_type and ctype != content_type:
            continue
        if status and row_status != status:
            continue
        scanned += 1
        missing = [kind for kind in EVIDENCE_KINDS if not _has_evidence(row, kind)]
        if len(missing) == len(EVIDENCE_KINDS):
            findings.append(
                {
                    "content_id": _text(row.get("id")),
                    "content_type": ctype,
                    "status": row_status,
                    "created_at": created_at.isoformat() if created_at else None,
                    "missing_evidence_kinds": missing,
                    "excerpt": _text(row.get("content") or row.get("body") or row.get("title"))[:160],
                }
            )
    findings.sort(key=lambda item: (item["created_at"] or "", item["content_id"]), reverse=True)
    by_type = Counter(item["content_type"] or "unknown" for item in findings)
    by_kind = Counter(kind for item in findings for kind in item["missing_evidence_kinds"])
    return {
        "artifact_type": "generated_content_source_gap",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "content_type": content_type, "status": status, "lookback_start": cutoff.isoformat()},
        "summary": {
            "scanned_count": scanned,
            "gap_count": len(findings),
            "gap_rate": round(len(findings) / scanned, 4) if scanned else 0.0,
            "counts_by_content_type": dict(sorted(by_type.items())),
            "counts_by_missing_evidence_kind": dict(sorted(by_kind.items())),
        },
        "findings": findings[:limit],
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_generated_content_source_gap_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_rows(conn, schema) if not gaps["missing_tables"] else []
    return build_generated_content_source_gap_report(rows, schema_gaps=gaps, **kwargs)


def format_generated_content_source_gap_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_generated_content_source_gap_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Generated Content Source Gap",
        f"Generated: {report['generated_at']}",
        f"Totals: scanned={summary['scanned_count']} gaps={summary['gap_count']} rate={summary['gap_rate']}",
    ]
    if not report["findings"]:
        lines.extend(["", "No generated content source gaps found."])
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for item in report["findings"]:
        lines.append(
            f"  - content={item['content_id']} type={item['content_type'] or '-'} status={item['status'] or '-'} "
            f"missing={', '.join(item['missing_evidence_kinds'])}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("generated_content", set())
    select = [
        _select(columns, ("id",), "id"),
        _select(columns, ("content_type", "type"), "content_type"),
        _select(columns, ("status", "publication_status"), "status"),
        _select(columns, ("created_at", "updated_at"), "created_at"),
        _select(columns, ("content", "body", "title"), "content"),
        _select(columns, ("source_commit", "source_commits", "source_sha"), "source_commit"),
        _select(columns, ("source_content_ids",), "source_content_ids"),
        _select(columns, ("source_activity_ids", "source_activity_id"), "source_activity_ids"),
        _select(columns, ("source_urls", "source_url"), "source_urls"),
        _select(columns, ("metadata",), "metadata"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content").fetchall()]


def _has_evidence(row: dict[str, Any], kind: str) -> bool:
    metadata = _json_obj(row.get("metadata"))
    direct = row.get(kind)
    aliases = {
        "source_commit": ("source_commit", "source_commits", "commit", "commit_sha"),
        "source_content_ids": ("source_content_ids", "content_ids"),
        "source_activity_ids": ("source_activity_ids", "activity_ids"),
        "source_urls": ("source_urls", "source_url", "urls"),
    }[kind]
    return _filled(direct) or any(_filled(metadata.get(alias)) for alias in aliases)


def _filled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    text = str(value).strip()
    return bool(text and text not in {"[]", "{}", "null", "None"})


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "generated_content" not in schema:
        return {"missing_tables": ["generated_content"], "missing_columns": {}}
    missing = [column for column in ("id",) if column not in schema["generated_content"]]
    return {"missing_tables": [], "missing_columns": {"generated_content": missing} if missing else {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
