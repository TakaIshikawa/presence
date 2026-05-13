"""Report queued reply drafts whose supporting context is stale."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25
DEFAULT_CONTEXT_MAX_AGE = 14


@dataclass(frozen=True)
class ReplyDraftContextStalenessFinding:
    draft_id: int
    mention_id: str | None
    author_handle: str | None
    status: str
    severity: str
    stale_fields: tuple[str, ...]
    age_days: int | None
    recommended_action: str
    context_updated_at: str | None = None
    knowledge_updated_at: str | None = None
    mention_fetched_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["stale_fields"] = list(self.stale_fields)
        return payload


@dataclass(frozen=True)
class ReplyDraftContextStalenessReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    findings: tuple[ReplyDraftContextStalenessFinding, ...]
    schema_warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_draft_context_staleness",
            "filters": dict(self.filters),
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "schema_warnings": list(self.schema_warnings),
            "totals": dict(sorted(self.totals.items())),
        }


def build_reply_draft_context_staleness_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    context_max_age: int = DEFAULT_CONTEXT_MAX_AGE,
    now: datetime | None = None,
) -> ReplyDraftContextStalenessReport:
    """Build a deterministic report for stale reply draft context."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if context_max_age <= 0:
        raise ValueError("context_max_age must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "context_max_age": context_max_age,
        "cutoff": cutoff.isoformat(),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    warnings = _schema_warnings(schema)
    if "reply_queue" not in schema or {"id", "status"} - schema.get("reply_queue", set()):
        return _report(generated_at, filters, (), warnings, draft_count=0)

    rows = _load_reply_rows(conn, schema, cutoff)
    findings = [_finding(row, generated_at, context_max_age) for row in rows]
    findings = [finding for finding in findings if finding.stale_fields]
    findings.sort(key=_sort_key)
    return _report(generated_at, filters, tuple(findings[:limit]), warnings, draft_count=len(rows))


def format_reply_draft_context_staleness_json(report: ReplyDraftContextStalenessReport) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_draft_context_staleness_text(report: ReplyDraftContextStalenessReport) -> str:
    """Render a stable text report."""
    lines = [
        "Reply Draft Context Staleness",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        f"Context max age: {report.filters['context_max_age']} days",
        (
            "Totals: "
            f"drafts={report.totals['draft_count']} "
            f"flagged={report.totals['finding_count']}"
        ),
    ]
    if report.schema_warnings:
        lines.append("Schema warnings: " + "; ".join(report.schema_warnings))
    if not report.findings:
        lines.append("No reply draft context staleness issues found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Findings:")
    for finding in report.findings:
        fields = ",".join(finding.stale_fields)
        age = "-" if finding.age_days is None else str(finding.age_days)
        identity = finding.mention_id or finding.author_handle or "-"
        lines.append(
            f"- draft={finding.draft_id} mention={identity} severity={finding.severity} "
            f"fields={fields} age_days={age} action={finding.recommended_action}"
        )
    return "\n".join(lines)


def _load_reply_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime) -> list[dict[str, Any]]:
    rq = schema["reply_queue"]
    detected_at = _column_expr(rq, "detected_at", "NULL", "rq")
    platform_metadata = _column_expr(rq, "platform_metadata", "NULL", "rq")
    relationship_context = _column_expr(rq, "relationship_context", "NULL", "rq")
    inbound_id = _column_expr(rq, "inbound_tweet_id", "NULL", "rq")
    author = _column_expr(rq, "inbound_author_handle", "NULL", "rq")
    joins = ""
    select_knowledge = "NULL AS knowledge_updated_at, 0 AS cited_knowledge_count"
    if {"reply_knowledge_links", "knowledge"}.issubset(schema):
        rkl = schema["reply_knowledge_links"]
        k = schema["knowledge"]
        if {"reply_queue_id", "knowledge_id"}.issubset(rkl) and "id" in k:
            knowledge_time = _coalesce_expr(k, ("ingested_at", "published_at", "created_at"), "NULL", "k")
            joins = (
                " LEFT JOIN reply_knowledge_links rkl ON rkl.reply_queue_id = rq.id"
                " LEFT JOIN knowledge k ON k.id = rkl.knowledge_id"
            )
            select_knowledge = f"MAX({knowledge_time}) AS knowledge_updated_at, COUNT(k.id) AS cited_knowledge_count"
    rows = conn.execute(
        f"""SELECT rq.id,
                  rq.status,
                  {detected_at} AS detected_at,
                  {inbound_id} AS mention_id,
                  {author} AS author_handle,
                  {relationship_context} AS relationship_context,
                  {platform_metadata} AS platform_metadata,
                  {select_knowledge}
           FROM reply_queue rq
           {joins}
           WHERE rq.status IN ('pending', 'queued')
             AND ({detected_at} IS NULL OR datetime({detected_at}) >= datetime(?))
           GROUP BY rq.id
           ORDER BY {detected_at} DESC, rq.id DESC""",
        (cutoff.isoformat(),),
    ).fetchall()
    return [dict(row) for row in rows]


def _finding(row: dict[str, Any], now: datetime, max_age: int) -> ReplyDraftContextStalenessFinding:
    context_ts = _timestamp_from_json(row.get("relationship_context"), ("updated_at", "refreshed_at", "fetched_at", "generated_at"))
    mention_ts = _timestamp_from_json(row.get("platform_metadata"), ("mention_fetched_at", "fetched_at", "snapshot_at", "retrieved_at"))
    knowledge_ts = row.get("knowledge_updated_at")
    values = {
        "relationship_context": context_ts,
        "cited_knowledge": knowledge_ts,
        "mention_snapshot": mention_ts,
    }
    stale_fields: list[str] = []
    ages = []
    for field, value in values.items():
        parsed = _parse_datetime(value)
        if parsed is None:
            stale_fields.append(f"missing_{field}")
            continue
        age = max(0, (now - parsed).days)
        ages.append(age)
        if age > max_age:
            stale_fields.append(field)
    age_days = max(ages) if ages else None
    severity = "high" if any(field.startswith("missing_") for field in stale_fields) else "medium"
    return ReplyDraftContextStalenessFinding(
        draft_id=int(row["id"]),
        mention_id=row.get("mention_id"),
        author_handle=row.get("author_handle"),
        status=str(row.get("status") or ""),
        severity=severity,
        stale_fields=tuple(stale_fields),
        age_days=age_days,
        recommended_action=_action(stale_fields),
        context_updated_at=context_ts,
        knowledge_updated_at=knowledge_ts,
        mention_fetched_at=mention_ts,
    )


def _action(fields: list[str]) -> str:
    if not fields:
        return "ready_for_review"
    if any("mention_snapshot" in field for field in fields):
        return "refresh inbound mention snapshot"
    if any("cited_knowledge" in field for field in fields):
        return "refresh cited knowledge before review"
    return "refresh relationship context before review"


def _report(
    generated_at: datetime,
    filters: dict[str, Any],
    findings: tuple[ReplyDraftContextStalenessFinding, ...],
    warnings: tuple[str, ...],
    *,
    draft_count: int,
) -> ReplyDraftContextStalenessReport:
    return ReplyDraftContextStalenessReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={"draft_count": draft_count, "finding_count": len(findings)},
        findings=findings,
        schema_warnings=warnings,
    )


def _schema_warnings(schema: dict[str, set[str]]) -> tuple[str, ...]:
    warnings: list[str] = []
    if "reply_queue" not in schema:
        warnings.append("missing table: reply_queue")
    elif {"id", "status"} - schema["reply_queue"]:
        warnings.append("missing columns: reply_queue(id, status)")
    for table in ("reply_knowledge_links", "knowledge"):
        if table not in schema:
            warnings.append(f"missing optional table: {table}")
    return tuple(warnings)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _column_expr(columns: set[str], column: str, fallback: str, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _coalesce_expr(columns: set[str], names: tuple[str, ...], fallback: str, alias: str) -> str:
    present = [f"{alias}.{name}" for name in names if name in columns]
    return f"COALESCE({', '.join(present)})" if present else fallback


def _timestamp_from_json(raw: Any, keys: tuple[str, ...]) -> str | None:
    data = _json_object(raw)
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sort_key(finding: ReplyDraftContextStalenessFinding) -> tuple[int, int, int]:
    severity_rank = {"high": 0, "medium": 1, "low": 2}
    return (severity_rank.get(finding.severity, 9), -(finding.age_days or 0), finding.draft_id)
