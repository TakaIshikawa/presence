"""Bucket knowledge citation outcomes by evidence age."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_STALE_USAGE = 3
FRESH_DAYS = 30
AGING_DAYS = 90


def build_knowledge_citation_freshness_decay_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    stale_usage_threshold: int = DEFAULT_STALE_USAGE,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a citation freshness report from row dictionaries."""
    if days <= 0:
        raise ValueError("days must be positive")
    if stale_usage_threshold <= 0:
        raise ValueError("stale_usage_threshold must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    citations = [_citation(row, generated_at) for row in rows]
    citations = [row for row in citations if row["used_at"] is None or row["used_at_dt"] >= cutoff]

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in citations:
        grouped[row["age_bucket"]].append(row)

    bucket_order = {"fresh": 0, "aging": 1, "stale": 2, "unknown": 3}
    buckets = []
    for bucket, bucket_rows in sorted(grouped.items(), key=lambda item: bucket_order.get(item[0], 99)):
        bucket_passes = sum(1 for row in bucket_rows if row["gate_passed"] is True)
        bucket_rejections = sum(1 for row in bucket_rows if row["rejected"] is True)
        bucket_published = sum(1 for row in bucket_rows if row["published"] is True)
        engagement_values = [row["engagement_value"] for row in bucket_rows if row["engagement_value"] is not None]
        successes = sum(1 for row in bucket_rows if row["outcome_success"] is True)
        known_outcomes = sum(1 for row in bucket_rows if row["outcome_success"] is not None)
        buckets.append(
            {
                "age_bucket": bucket,
                "citation_count": len(bucket_rows),
                "unique_knowledge_count": len({row["knowledge_id"] for row in bucket_rows if row["knowledge_id"]}),
                "gate_pass_rate": _rate(bucket_passes, sum(1 for row in bucket_rows if row["gate_passed"] is not None)),
                "rejection_rate": _rate(bucket_rejections, sum(1 for row in bucket_rows if row["rejected"] is not None)),
                "publication_rate": _rate(bucket_published, sum(1 for row in bucket_rows if row["published"] is not None)),
                "outcome_rate": _rate(successes, known_outcomes),
                "average_engagement": _avg(engagement_values),
            }
        )

    stale_rows = grouped.get("stale", [])
    stale_summary = next((row for row in buckets if row["age_bucket"] == "stale"), None)
    fresh_summary = next((row for row in buckets if row["age_bucket"] == "fresh"), None)
    patterns = []
    if len(stale_rows) >= stale_usage_threshold:
        patterns.append(
            {
                "pattern": "stale_high_usage",
                "citation_count": len(stale_rows),
                "threshold": stale_usage_threshold,
                "knowledge_ids": sorted({row["knowledge_id"] for row in stale_rows if row["knowledge_id"]}),
            }
        )
    if stale_summary and stale_summary["outcome_rate"] is not None:
        fresh_rate = fresh_summary["outcome_rate"] if fresh_summary else None
        if stale_summary["outcome_rate"] < 0.5 or (fresh_rate is not None and stale_summary["outcome_rate"] < fresh_rate):
            patterns.append(
                {
                    "pattern": "stale_low_outcome",
                    "stale_outcome_rate": stale_summary["outcome_rate"],
                    "fresh_outcome_rate": fresh_rate,
                }
            )

    return {
        "artifact_type": "knowledge_citation_freshness_decay",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "fresh_days": FRESH_DAYS,
            "aging_days": AGING_DAYS,
            "stale_usage_threshold": stale_usage_threshold,
        },
        "totals": {
            "citation_count": len(citations),
            "bucket_count": len(buckets),
            "stale_citation_count": len(stale_rows),
            "pattern_count": len(patterns),
        },
        "age_buckets": buckets,
        "patterns": patterns,
        "recommendations": _recommendations(patterns),
    }


def build_knowledge_citation_freshness_decay_report_from_db(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    stale_usage_threshold: int = DEFAULT_STALE_USAGE,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load knowledge citations and available outcomes from SQLite."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_db_rows(conn, schema)
    report = build_knowledge_citation_freshness_decay_report(
        rows,
        days=days,
        stale_usage_threshold=stale_usage_threshold,
        now=now,
    )
    report["missing_tables"] = [table for table in ("knowledge", "generated_content") if table not in schema]
    report["engagement_available"] = any(table in schema for table in ("post_engagement", "bluesky_engagement", "linkedin_engagement"))
    return report


def format_knowledge_citation_freshness_decay_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_citation_freshness_decay_text(report: dict[str, Any]) -> str:
    lines = [
        "Knowledge Citation Freshness Decay",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']}d",
        f"Citations: {report['totals']['citation_count']} stale={report['totals']['stale_citation_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["age_buckets"]:
        lines.append("Buckets:")
        for row in report["age_buckets"]:
            lines.append(
                f"- {row['age_bucket']}: citations={row['citation_count']} "
                f"outcome_rate={_format_rate(row['outcome_rate'])} "
                f"published={_format_rate(row['publication_rate'])} "
                f"engagement={_format_number(row['average_engagement'])}"
            )
    if report["patterns"]:
        lines.append("Patterns:")
        for row in report["patterns"]:
            lines.append(f"- {row['pattern']}")
    if report["recommendations"]:
        lines.append("Recommendations:")
        lines.extend(f"- {item}" for item in report["recommendations"])
    if not report["patterns"]:
        lines.append("No citation freshness decay patterns found.")
    return "\n".join(lines)


def _load_db_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "knowledge" not in schema:
        return []
    knowledge_cols = schema["knowledge"]
    generated_cols = schema.get("generated_content", set())
    rows: list[dict[str, Any]] = []
    content_source_col = _first_column(generated_cols, "source_activity_ids", "source_content_ids", "source_commits", "source_messages")
    content_created = _first_column(generated_cols, "created_at", "published_at")
    if "generated_content" in schema and content_source_col:
        selected = [
            "knowledge.id AS knowledge_id",
            "knowledge.published_at AS evidence_at",
            "knowledge.ingested_at AS ingested_at",
            "generated_content.id AS content_id",
            _select_expr("generated_content", content_created, "used_at"),
            _select_expr("generated_content", _first_column(generated_cols, "published"), "published"),
            _select_expr("generated_content", _first_column(generated_cols, "eval_score"), "eval_score"),
            _select_expr("generated_content", _first_column(generated_cols, "eval_feedback"), "eval_feedback"),
        ]
        rows.extend(
            dict(row)
            for row in conn.execute(
                f"""SELECT {', '.join(selected)}
                    FROM knowledge
                    JOIN generated_content
                      ON COALESCE(generated_content.{content_source_col}, '') LIKE '%' || knowledge.id || '%'"""
            )
        )
    if not rows:
        selected = [
            "knowledge.id AS knowledge_id",
            "knowledge.published_at AS evidence_at",
            "knowledge.ingested_at AS ingested_at",
            "knowledge.ingested_at AS used_at",
        ]
        rows.extend(dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM knowledge"))

    engagement = _load_engagement(conn, schema)
    for row in rows:
        content_id = _text(row.get("content_id"))
        if content_id and content_id in engagement:
            row["engagement"] = engagement[content_id]
    return rows


def _load_engagement(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[str, float]:
    values: dict[str, float] = {}
    for table in ("post_engagement", "bluesky_engagement", "linkedin_engagement", "newsletter_engagement"):
        if table not in schema or "content_id" not in schema[table]:
            continue
        score_col = _first_column(schema[table], "engagement_score", "clicks", "opens", "like_count")
        if score_col is None:
            continue
        for row in conn.execute(f"SELECT content_id, MAX({score_col}) AS engagement FROM {table} GROUP BY content_id"):
            if row["engagement"] is not None:
                values[_text(row["content_id"])] = float(row["engagement"])
    return values


def _citation(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    evidence_at = _parse_datetime(_first(row, "evidence_at", "source_published_at", "published_at", "ingested_at", "created_at"))
    used_at = _parse_datetime(_first(row, "used_at", "generated_at", "created_at", "published_at")) or now
    age_days = (used_at - evidence_at).days if evidence_at else None
    gate_status = _text(_first(row, "gate_status", "status", "claim_status")).lower()
    rejection = _first(row, "rejected", "rejection_reason", "eval_feedback")
    published = _boolish(_first(row, "published", "is_published"))
    engagement = _float(_first(row, "engagement", "engagement_score", "clicks", "opens", "outcome_score"))
    gate_passed = None
    if gate_status:
        gate_passed = gate_status in {"pass", "passed", "approved", "ok"}
    eval_score = _float(_first(row, "eval_score", "score", "final_score"))
    if gate_passed is None and eval_score is not None:
        gate_passed = eval_score >= 7
    rejected = _boolish(rejection)
    outcome_success = _outcome_success(gate_passed, published, engagement, rejected)
    return {
        "knowledge_id": _text(_first(row, "knowledge_id", "citation_id", "source_id", "id")),
        "used_at": used_at.isoformat() if used_at else None,
        "used_at_dt": used_at,
        "evidence_at": evidence_at.isoformat() if evidence_at else None,
        "age_days": age_days,
        "age_bucket": _bucket(age_days),
        "gate_passed": gate_passed,
        "rejected": rejected,
        "published": published,
        "engagement_value": engagement,
        "outcome_success": outcome_success,
    }


def _outcome_success(gate_passed: bool | None, published: bool | None, engagement: float | None, rejected: bool | None) -> bool | None:
    if engagement is not None:
        return engagement > 0
    if published is not None:
        return published
    if gate_passed is not None:
        return gate_passed
    if rejected is not None:
        return not rejected
    return None


def _bucket(age_days: int | None) -> str:
    if age_days is None or age_days < 0:
        return "unknown"
    if age_days <= FRESH_DAYS:
        return "fresh"
    if age_days <= AGING_DAYS:
        return "aging"
    return "stale"


def _recommendations(patterns: list[dict[str, Any]]) -> list[str]:
    names = {row["pattern"] for row in patterns}
    recommendations = []
    if "stale_high_usage" in names:
        recommendations.append("Refresh heavily reused stale knowledge before citing it again.")
    if "stale_low_outcome" in names:
        recommendations.append("Down-rank stale citations when fresher evidence has better outcomes.")
    return recommendations


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _first_column(columns: set[str], *names: str) -> str | None:
    return next((name for name in names if name in columns), None)


def _select_expr(table: str, column: str | None, output: str, fallback: str = "NULL") -> str:
    return f"{table}.{column} AS {output}" if column else f"{fallback} AS {output}"


def _parse_datetime(value: Any) -> datetime | None:
    if not _text(value):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *names: str) -> Any:
    return next((row[name] for name in names if name in row and row[name] is not None), None)


def _boolish(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = _text(value).lower()
    if text in {"1", "true", "yes", "y", "published", "pass", "passed", "approved"}:
        return True
    if text in {"0", "false", "no", "n", "none", "", "unpublished"}:
        return False
    return True


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rate(count: int, total: int) -> float | None:
    return round(count / total, 3) if total else None


def _avg(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 3) if values else None


def _format_rate(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f}"


def _format_number(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()
