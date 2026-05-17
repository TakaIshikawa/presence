"""Summarize freshness mix of knowledge sources used for generation context."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_FRESH_DAYS = 30
DEFAULT_AGING_DAYS = 90
DEFAULT_STALE_DOMINANCE_THRESHOLD = 0.5


def build_knowledge_source_freshness_mix_report(
    rows: list[dict[str, Any]],
    *,
    fresh_days: int = DEFAULT_FRESH_DAYS,
    aging_days: int = DEFAULT_AGING_DAYS,
    stale_dominance_threshold: float = DEFAULT_STALE_DOMINANCE_THRESHOLD,
    now: datetime | None = None,
) -> dict[str, Any]:
    if fresh_days < 0 or aging_days < 0:
        raise ValueError("fresh_days and aging_days must be non-negative")
    if fresh_days > aging_days:
        raise ValueError("fresh_days must be less than or equal to aging_days")
    if stale_dominance_threshold < 0 or stale_dominance_threshold > 1:
        raise ValueError("stale_dominance_threshold must be between 0 and 1")
    generated_at = _utc(now or datetime.now(timezone.utc))
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        run_id = _text(_first(row, "run_id", "generation_run_id", "content_id", "id")) or "unknown"
        source_ts = _parse_ts(_first(row, "source_published_at", "published_at", "source_created_at", "created_at"))
        age_days = round((generated_at - source_ts).total_seconds() / 86400, 2) if source_ts else None
        bucket = _bucket(age_days, fresh_days, aging_days)
        buckets[run_id].append(
            {
                "source_id": _text(_first(row, "source_id", "knowledge_id", "id")) or "unknown",
                "source_type": _text(_first(row, "source_type")) or None,
                "source_published_at": source_ts.isoformat() if source_ts else None,
                "age_days": age_days,
                "bucket": bucket,
            }
        )

    runs = []
    total_refs = 0
    flagged_count = 0
    for run_id, sources in buckets.items():
        counts = Counter(source["bucket"] for source in sources)
        total = len(sources)
        total_refs += total
        percentages = {name: round(counts.get(name, 0) / total, 4) for name in ("fresh", "aging", "stale", "unknown")}
        stale_dominated = percentages["stale"] >= stale_dominance_threshold
        flagged_count += int(stale_dominated)
        runs.append(
            {
                "run_id": run_id,
                "source_count": total,
                "bucket_counts": {name: counts.get(name, 0) for name in ("fresh", "aging", "stale", "unknown")},
                "bucket_percentages": percentages,
                "stale_dominance_flag": stale_dominated,
                "sources": sorted(sources, key=lambda item: (item["bucket"], item["source_id"])),
            }
        )
    runs.sort(key=lambda run: (-int(run["stale_dominance_flag"]), -run["bucket_percentages"]["stale"], run["run_id"]))
    return {
        "artifact_type": "knowledge_source_freshness_mix",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "fresh_days": fresh_days,
            "aging_days": aging_days,
            "stale_dominance_threshold": stale_dominance_threshold,
        },
        "totals": {"run_count": len(runs), "source_reference_count": total_refs, "stale_dominated_run_count": flagged_count},
        "runs": runs,
        "empty_state": {"is_empty": not runs, "message": "No generation context source references found." if not runs else None},
    }


def build_knowledge_source_freshness_mix_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    return build_knowledge_source_freshness_mix_report(_load_rows(conn, _schema(conn)), **kwargs)


def format_knowledge_source_freshness_mix_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_source_freshness_mix_table(report: dict[str, Any]) -> str:
    lines = [
        "Knowledge Source Freshness Mix",
        f"Generated: {report['generated_at']}",
        (
            f"Buckets: fresh<={report['filters']['fresh_days']}d "
            f"aging<={report['filters']['aging_days']}d "
            f"stale_dominance>={report['filters']['stale_dominance_threshold']}"
        ),
        f"Totals: runs={report['totals']['run_count']} references={report['totals']['source_reference_count']} flagged={report['totals']['stale_dominated_run_count']}",
    ]
    if not report["runs"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "run_id | sources | fresh% | aging% | stale% | unknown% | flagged"])
    for run in report["runs"]:
        pct = run["bucket_percentages"]
        lines.append(f"{run['run_id']} | {run['source_count']} | {pct['fresh']:.2f} | {pct['aging']:.2f} | {pct['stale']:.2f} | {pct['unknown']:.2f} | {run['stale_dominance_flag']}")
    return "\n".join(lines)


format_knowledge_source_freshness_mix_text = format_knowledge_source_freshness_mix_table


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("generation_context_sources", "generated_context_sources", "run_knowledge_sources"):
        if table in schema:
            cols = schema[table]
            selected = [
                _col(cols, "run_id", "generation_run_id", "content_id", default="NULL") + " AS run_id",
                _col(cols, "source_id", "knowledge_id", default="NULL") + " AS source_id",
                _col(cols, "source_type", default="NULL") + " AS source_type",
                _col(cols, "source_published_at", "published_at", "source_created_at", "created_at", default="NULL") + " AS source_published_at",
            ]
            return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    return []


def _bucket(age_days: float | None, fresh_days: int, aging_days: int) -> str:
    if age_days is None:
        return "unknown"
    if age_days <= fresh_days:
        return "fresh"
    if age_days <= aging_days:
        return "aging"
    return "stale"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
