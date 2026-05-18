"""Identify reply drafts with stale or narrow supporting evidence age mixes."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from statistics import median
from typing import Any


DEFAULT_STALE_DAYS = 180
DEFAULT_SINGLE_BAND_MIN_EVIDENCE = 2
DEFAULT_LIMIT = 100


def build_reply_draft_evidence_age_mix_report(
    draft_rows: list[dict[str, Any]],
    evidence_rows: list[dict[str, Any]] | None = None,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    single_band_min_evidence: int = DEFAULT_SINGLE_BAND_MIN_EVIDENCE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if single_band_min_evidence <= 0:
        raise ValueError("single_band_min_evidence must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    evidence_by_draft = _evidence_by_draft(evidence_rows or [])
    risks = []
    reason_counts: Counter[str] = Counter()

    for raw in draft_rows:
        draft = _normalize_draft(raw)
        evidence = evidence_by_draft.get(draft["draft_id"], [])
        dated = [_parse_ts(_first(item, "evidence_at", "source_published_at", "published_at", "created_at", "updated_at", "fetched_at")) for item in evidence]
        missing_count = sum(1 for item in dated if item is None)
        dates = [item for item in dated if item]
        ages = sorted((_age_days(generated_at, item) or 0) for item in dates)
        reasons: list[str] = []
        if not evidence:
            reasons.append("no_evidence")
            freshness_mix = "none"
        elif missing_count:
            reasons.append("missing_evidence_dates")
            freshness_mix = "missing_dates"
        else:
            bands = {_age_band(age) for age in ages}
            freshness_mix = "single_age_band" if len(bands) == 1 else "mixed_age_bands"
            if len(bands) == 1 and len(ages) >= single_band_min_evidence:
                reasons.append("single_age_band")
        if ages and max(ages) > stale_days:
            reasons.append("stale_evidence")

        if not reasons:
            continue
        reason_counts.update(reasons)
        risks.append(
            {
                "draft_id": draft["draft_id"],
                "target_author": draft["target_author"],
                "created_at": _iso(draft["created_at"]),
                "evidence_count": len(evidence),
                "oldest_evidence_age_days": max(ages) if ages else None,
                "median_evidence_age_days": int(median(ages)) if ages else None,
                "freshness_mix": freshness_mix,
                "reasons": reasons,
            }
        )

    risks.sort(
        key=lambda item: (
            "no_evidence" not in item["reasons"],
            -(item["oldest_evidence_age_days"] or 0),
            item["draft_id"],
        )
    )
    shown = risks[:limit]
    return {
        "artifact_type": "reply_draft_evidence_age_mix",
        "generated_at": generated_at.isoformat(),
        "filters": {"stale_days": stale_days, "single_band_min_evidence": single_band_min_evidence, "limit": limit},
        "totals": {
            "draft_count": len(draft_rows),
            "risk_count": len(risks),
            "shown_count": len(shown),
            "reason_counts": dict(sorted(reason_counts.items())),
        },
        "draft_risks": shown,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
        "empty_state": {
            "is_empty": not risks,
            "message": "No reply draft evidence age mix risks found." if not risks else None,
        },
    }


def build_reply_draft_evidence_age_mix_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    drafts = _load_drafts(conn, schema) if not gaps["missing_tables"] else []
    evidence = _load_evidence(conn, schema)
    return build_reply_draft_evidence_age_mix_report(drafts, evidence, schema_gaps=gaps, **kwargs)


def format_reply_draft_evidence_age_mix_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_draft_evidence_age_mix_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Draft Evidence Age Mix",
        f"Generated: {report['generated_at']}",
        f"Totals: drafts={report['totals']['draft_count']} risks={report['totals']['risk_count']}",
    ]
    if not report["draft_risks"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "draft_id | author | evidence | oldest_days | median_days | mix | reasons"])
    for row in report["draft_risks"]:
        lines.append(
            f"{row['draft_id']} | {row['target_author'] or '-'} | {row['evidence_count']} | "
            f"{_dash(row['oldest_evidence_age_days'])} | {_dash(row['median_evidence_age_days'])} | "
            f"{row['freshness_mix']} | {','.join(row['reasons'])}"
        )
    return "\n".join(lines)


format_reply_draft_evidence_age_mix_table = format_reply_draft_evidence_age_mix_text


def _normalize_draft(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "draft_id": _text(_first(row, "draft_id", "reply_draft_id", "id", "reply_queue_id")) or "unknown",
        "target_author": _text(_first(row, "target_author", "inbound_author", "author", "screen_name")),
        "created_at": _parse_ts(_first(row, "created_at", "drafted_at", "detected_at", "updated_at")),
    }


def _evidence_by_draft(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        draft_id = _text(_first(row, "draft_id", "reply_draft_id", "reply_queue_id", "target_id"))
        if draft_id:
            grouped[draft_id].append(row)
    return grouped


def _age_band(age_days: int) -> str:
    if age_days <= 30:
        return "fresh"
    if age_days <= 90:
        return "recent"
    if age_days <= 180:
        return "aging"
    return "stale"


def _load_drafts(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "reply_drafts" if "reply_drafts" in schema else "reply_queue"
    cols = schema[table]
    select = [
        _select(cols, ("id", "draft_id", "reply_queue_id"), "id"),
        _select(cols, ("target_author", "inbound_author", "author", "screen_name"), "target_author"),
        _select(cols, ("created_at", "drafted_at", "detected_at", "updated_at"), "created_at"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _load_evidence(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = next((name for name in ("reply_draft_evidence", "reply_sources", "reply_knowledge_links") if name in schema), "")
    if not table:
        return []
    cols = schema[table]
    if table == "reply_knowledge_links" and "knowledge" in schema:
        link_cols = cols
        knowledge_cols = schema["knowledge"]
        select = [
            _select(link_cols, ("reply_queue_id", "draft_id", "reply_draft_id"), "draft_id"),
            _select(knowledge_cols, ("published_at", "ingested_at", "created_at", "updated_at"), "evidence_at", table_alias="k"),
        ]
        return [
            dict(row)
            for row in conn.execute(
                f"SELECT {', '.join(select)} FROM reply_knowledge_links l LEFT JOIN knowledge k ON k.id = l.knowledge_id"
            ).fetchall()
        ]
    select = [
        _select(cols, ("draft_id", "reply_draft_id", "reply_queue_id", "target_id"), "draft_id"),
        _select(cols, ("evidence_at", "source_published_at", "published_at", "created_at", "updated_at", "fetched_at"), "evidence_at"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "reply_drafts" in schema or "reply_queue" in schema:
        return {"missing_tables": [], "missing_columns": {}}
    return {"missing_tables": ["reply_drafts_or_reply_queue"], "missing_columns": {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str, table_alias: str | None = None) -> str:
    prefix = f"{table_alias}." if table_alias else ""
    for candidate in candidates:
        if candidate in columns:
            return f"{prefix}{candidate} AS {alias}" if candidate != alias or table_alias else candidate
    return f"NULL AS {alias}"


def _first(row: dict[str, Any], *keys: str) -> Any:
    return next((row[key] for key in keys if key in row and row[key] not in (None, "")), None)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _utc(value)
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _age_days(now: datetime, value: datetime | None) -> int | None:
    if not value:
        return None
    return max((now - value).days, 0)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _dash(value: Any) -> Any:
    return "-" if value is None else value
