"""Score open content ideas for evidence readiness requirements."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping


DEFAULT_FRESHNESS_DAYS = 45


@dataclass(frozen=True)
class ContentIdeaEvidenceRequirementRow:
    """Evidence readiness requirements for one open idea."""

    idea_id: int
    topic: str | None
    priority: str
    category: str
    evidence_counts: dict[str, int]
    missing_requirements: tuple[str, ...]
    recommended_next_action: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evidence_counts"] = dict(sorted(self.evidence_counts.items()))
        payload["missing_requirements"] = list(self.missing_requirements)
        return payload


@dataclass(frozen=True)
class ContentIdeaEvidenceRequirementsReport:
    """Read-only evidence requirements report for open content ideas."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[ContentIdeaEvidenceRequirementRow, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_content_idea_evidence_requirements_report(
    db_or_conn: Any,
    *,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    now: datetime | None = None,
) -> ContentIdeaEvidenceRequirementsReport:
    """Categorize open ideas by evidence readiness, not generated-content readiness."""
    if freshness_days <= 0:
        raise ValueError("freshness_days must be positive")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    rows = _load_rows(conn) if not missing_tables and not missing_columns else []
    findings = tuple(
        sorted(
            (_classify_row(row, freshness_days=freshness_days, now=generated_at) for row in rows),
            key=lambda row: (_priority_rank(row.priority), row.category, row.idea_id),
        )
    )
    by_category = Counter(row.category for row in findings)
    return ContentIdeaEvidenceRequirementsReport(
        artifact_type="content_idea_evidence_requirements",
        generated_at=generated_at.isoformat(),
        filters={"freshness_days": freshness_days, "status": "open"},
        totals={
            "idea_count": len(findings),
            "by_category": dict(sorted(by_category.items())),
            "ready_count": by_category.get("ready", 0),
        },
        rows=findings,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_content_idea_evidence_requirements_json(
    report: ContentIdeaEvidenceRequirementsReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_idea_evidence_requirements_text(
    report: ContentIdeaEvidenceRequirementsReport,
) -> str:
    """Render a concise text report."""
    lines = [
        "Content Idea Evidence Requirements",
        f"Generated: {report.generated_at}",
        f"Open ideas: {report.totals['idea_count']} ready={report.totals['ready_count']}",
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + "; ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in sorted(report.missing_columns.items())
            )
        )
    if not report.rows:
        lines.append("No open content ideas found.")
        return "\n".join(lines)
    for row in report.rows:
        lines.append(
            f"- idea={row.idea_id} category={row.category} priority={row.priority} "
            f"topic={row.topic or '-'} missing={','.join(row.missing_requirements) or '-'} "
            f"next={row.recommended_next_action}"
        )
    return "\n".join(lines)


def _classify_row(
    row: Mapping[str, Any],
    *,
    freshness_days: int,
    now: datetime,
) -> ContentIdeaEvidenceRequirementRow:
    metadata = _json_dict(row.get("source_metadata"))
    counts = _evidence_counts(row, metadata)
    latest = _latest_evidence_timestamp(row, metadata)
    is_recent = latest is not None and now - latest <= timedelta(days=freshness_days)
    missing: list[str] = []
    if not any(counts.values()):
        missing.append("source_evidence")
    if counts["source_metadata"] == 0:
        missing.append("explicit_source_metadata")
    if not is_recent:
        missing.append("recent_activity")
    if _needs_claim_evidence(row, metadata) and counts["knowledge_sources"] == 0:
        missing.append("specific_claim_evidence")

    if "source_evidence" in missing or "explicit_source_metadata" in missing:
        category = "needs_source"
        action = "Add linked commits, sessions, knowledge sources, or source metadata."
    elif "recent_activity" in missing:
        category = "needs_recent_activity"
        action = "Link fresh commits, sessions, or source updates before drafting."
    elif "specific_claim_evidence" in missing:
        category = "needs_specific_claim_evidence"
        action = "Attach knowledge sources for the specific claim in the idea."
    else:
        category = "ready"
        action = "Evidence requirements are satisfied for idea development."

    return ContentIdeaEvidenceRequirementRow(
        idea_id=int(row["id"]),
        topic=row.get("topic"),
        priority=str(row.get("priority") or "normal"),
        category=category,
        evidence_counts=counts,
        missing_requirements=tuple(sorted(missing)),
        recommended_next_action=action,
    )


def _evidence_counts(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> dict[str, int]:
    return {
        "commits": len(_list(metadata.get("commits") or metadata.get("source_commits"))),
        "knowledge_sources": len(
            _list(metadata.get("knowledge_sources") or metadata.get("sources"))
        ),
        "sessions": len(_list(metadata.get("sessions") or metadata.get("source_messages"))),
        "source_metadata": 1 if metadata or row.get("source") else 0,
    }


def _latest_evidence_timestamp(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> datetime | None:
    candidates = [
        row.get("updated_at"),
        row.get("created_at"),
        metadata.get("updated_at"),
        metadata.get("evidence_updated_at"),
        metadata.get("last_activity_at"),
    ]
    parsed = [_timestamp(value) for value in candidates]
    parsed = [value for value in parsed if value is not None]
    return max(parsed) if parsed else None


def _needs_claim_evidence(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> bool:
    text = " ".join(str(value or "") for value in (row.get("note"), row.get("topic")))
    return bool(metadata.get("claims") or any(word in text.lower() for word in ("claim", "data", "metric", "study", "%")))


def _load_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """SELECT id, note, topic, priority, source, source_metadata, created_at, updated_at
               FROM content_ideas
               WHERE COALESCE(status, 'open') = 'open'
               ORDER BY priority ASC, created_at ASC, id ASC"""
        ).fetchall()
    ]


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "content_ideas": {
            "created_at",
            "id",
            "note",
            "priority",
            "source",
            "source_metadata",
            "status",
            "topic",
            "updated_at",
        }
    }
    missing_tables = tuple(sorted(table for table in required if table not in schema))
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _priority_rank(priority: str) -> int:
    return {"high": 0, "normal": 1, "low": 2}.get(priority, 3)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
