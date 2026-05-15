"""Detect source-backed claims reused across too many content artifacts."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_WINDOW_DAYS = 30
DEFAULT_MEDIUM_THRESHOLD = 3
DEFAULT_HIGH_THRESHOLD = 5
DEFAULT_LIMIT = 50

_WORD_RE = re.compile(r"[a-z0-9]+")


def build_source_claim_reuse_risk_report(
    claim_rows: list[dict[str, Any]],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    medium_threshold: int = DEFAULT_MEDIUM_THRESHOLD,
    high_threshold: int = DEFAULT_HIGH_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return reuse-risk records from in-memory source-backed claim rows."""
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if not (0 < medium_threshold <= high_threshold):
        raise ValueError("thresholds must satisfy 0 < medium_threshold <= high_threshold")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=window_days)
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for row in claim_rows:
        used_at = _parse_dt(row.get("used_at") or row.get("created_at") or row.get("published_at"))
        if used_at and used_at < cutoff:
            continue
        source_identifier = _source_identifier(row)
        normalized_claim = normalize_claim_text(row.get("claim") or row.get("claim_text") or row.get("text"))
        if not source_identifier or not normalized_claim:
            continue
        group = groups.setdefault(
            (source_identifier, normalized_claim),
            {
                "source_identifier": source_identifier,
                "normalized_claim": normalized_claim,
                "reuse_count": 0,
                "content_ids": set(),
                "content_types": set(),
                "first_used_at": None,
                "last_used_at": None,
                "examples": [],
            },
        )
        group["reuse_count"] += 1
        content_id = _text(row.get("content_id") or row.get("post_id") or row.get("id"))
        content_type = _text(row.get("content_type") or row.get("artifact_type") or "unknown")
        if content_id:
            group["content_ids"].add(content_id)
        if content_type:
            group["content_types"].add(content_type)
        if used_at:
            group["first_used_at"] = min(_coalesce_dt(group["first_used_at"], used_at), used_at)
            group["last_used_at"] = max(_coalesce_dt(group["last_used_at"], used_at), used_at)
        if len(group["examples"]) < 3:
            group["examples"].append(_text(row.get("claim") or row.get("claim_text") or row.get("text")))

    records = [_finalize_group(group, medium_threshold, high_threshold) for group in groups.values()]
    records.sort(key=_sort_key)
    ranked = records[:limit]
    totals = {
        "claim_group_count": len(records),
        "ranked_count": len(ranked),
        "low": sum(1 for item in records if item["risk_level"] == "low"),
        "medium": sum(1 for item in records if item["risk_level"] == "medium"),
        "high": sum(1 for item in records if item["risk_level"] == "high"),
    }
    return {
        "artifact_type": "source_claim_reuse_risk",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "window_days": window_days,
            "medium_threshold": medium_threshold,
            "high_threshold": high_threshold,
            "limit": limit,
            "window_start": cutoff.isoformat(),
        },
        "totals": totals,
        "risks": ranked,
        "empty_state": {
            "is_empty": not records,
            "message": "No source-backed claims found in the selected window." if not records else None,
        },
    }


def build_source_claim_reuse_risk_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_source_claim_reuse_risk_report(_load_claims(conn, schema), **kwargs)


def normalize_claim_text(value: Any) -> str:
    words = _WORD_RE.findall(_text(value).lower())
    stop = {"a", "an", "the", "and", "or", "of", "to", "in", "for", "with", "that"}
    kept = [word for word in words if word not in stop]
    return " ".join(kept)


def format_source_claim_reuse_risk_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_source_claim_reuse_risk_text(report: dict[str, Any]) -> str:
    lines = [
        "Source Claim Reuse Risk",
        f"Generated: {report['generated_at']}",
        (
            f"Window: {report['filters']['window_days']} days "
            f"medium>={report['filters']['medium_threshold']} high>={report['filters']['high_threshold']}"
        ),
        (
            "Totals: "
            f"groups={report['totals']['claim_group_count']} high={report['totals']['high']} "
            f"medium={report['totals']['medium']}"
        ),
    ]
    if not report["risks"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Risks:", "risk    count types                 source  claim"])
    for item in report["risks"]:
        lines.append(
            f"{item['risk_level']:<7} {item['reuse_count']:<5} "
            f"{','.join(item['content_types'])[:20]:<20} "
            f"{item['source_identifier'][:7]:<7} {item['normalized_claim'][:70]}"
        )
    return "\n".join(lines)


def _load_claims(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("source_claims", "content_claims"):
        if table in schema:
            columns = schema[table]
            selected = [
                "id",
                "content_id" if "content_id" in columns else "NULL AS content_id",
                "content_type" if "content_type" in columns else "NULL AS content_type",
                "source_id" if "source_id" in columns else "NULL AS source_id",
                "source_url" if "source_url" in columns else "NULL AS source_url",
                "claim_text" if "claim_text" in columns else "claim" if "claim" in columns else "NULL AS claim_text",
                "created_at" if "created_at" in columns else "NULL AS created_at",
                "published_at" if "published_at" in columns else "NULL AS published_at",
            ]
            rows = conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()
            return [dict(row) for row in rows]
    return []


def _finalize_group(group: dict[str, Any], medium_threshold: int, high_threshold: int) -> dict[str, Any]:
    count = group["reuse_count"]
    if count >= high_threshold:
        risk = "high"
    elif count >= medium_threshold:
        risk = "medium"
    else:
        risk = "low"
    return {
        "source_identifier": group["source_identifier"],
        "normalized_claim": group["normalized_claim"],
        "reuse_count": count,
        "risk_level": risk,
        "content_ids": sorted(group["content_ids"]),
        "content_types": sorted(group["content_types"]),
        "first_used_at": _iso(group["first_used_at"]),
        "last_used_at": _iso(group["last_used_at"]),
        "example_claims": group["examples"],
        "recommended_action": _recommendation(risk),
    }


def _recommendation(risk: str) -> str:
    return {
        "low": "track reuse",
        "medium": "vary framing or add fresh corroborating evidence",
        "high": "retire repeated claim until refreshed or replace with new sourcing",
    }[risk]


def _source_identifier(row: dict[str, Any]) -> str:
    return _text(row.get("source_url") or row.get("source_id") or row.get("url")).strip().lower()


def _sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
    rank = {"high": 2, "medium": 1, "low": 0}
    return (-rank[item["risk_level"]], -item["reuse_count"], item["source_identifier"])


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return _utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _coalesce_dt(value: datetime | None, fallback: datetime) -> datetime:
    return value or fallback


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}
