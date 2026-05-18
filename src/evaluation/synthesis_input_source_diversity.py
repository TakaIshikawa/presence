"""Report synthesis outputs dominated by one input source type."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DOMINANCE_THRESHOLD = 0.75
DEFAULT_LIMIT = 100


@dataclass(frozen=True)
class SynthesisInputDominanceFinding:
    """Dominant source-type mix for one synthesis output or run."""

    content_id: str
    run_id: str
    total_source_count: int
    source_type_counts: dict[str, int]
    dominant_source_type: str
    dominant_share: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SynthesisInputSourceDiversityReport:
    """Synthesis input source diversity report."""

    generated_at: str
    dominance_threshold: float
    limit: int
    total_groups: int
    total_source_count: int
    findings: tuple[SynthesisInputDominanceFinding, ...]
    schema_gaps: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "synthesis_input_source_diversity",
            "dominance_threshold": self.dominance_threshold,
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "limit": self.limit,
            "schema_gaps": self.schema_gaps or {"missing_tables": [], "missing_columns": {}},
            "totals": {
                "flagged_count": len(self.findings),
                "group_count": self.total_groups,
                "source_count": self.total_source_count,
                "shown_count": len(self.findings),
            },
            "empty_state": {
                "is_empty": not self.findings,
                "message": "No synthesis input source dominance found." if not self.findings else None,
            },
        }


def build_synthesis_input_source_diversity_report(
    rows: Sequence[Mapping[str, Any]],
    *,
    dominance_threshold: float = DEFAULT_DOMINANCE_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> SynthesisInputSourceDiversityReport:
    """Analyze source artifact rows without querying storage."""
    threshold = _threshold(dominance_threshold)
    row_limit = _positive_int(limit, "limit")
    generated_at = _utc(now or datetime.now(timezone.utc)).isoformat()

    groups: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for row in rows:
        content_id = _text(_first(row, "content_id", "generated_content_id", "id"))
        run_id = _text(_first(row, "run_id", "generation_run_id", "batch_id"))
        group_key = (content_id, run_id)
        if not content_id and not run_id:
            group_key = (_text(row.get("group_id")) or "unknown", "")
        source_type = normalize_source_type(_first(row, "source_type", "artifact_type", "type"))
        groups[group_key][source_type] += 1

    findings: list[SynthesisInputDominanceFinding] = []
    for (content_id, run_id), counts in groups.items():
        total = sum(counts.values())
        if total <= 0:
            continue
        dominant_source_type, dominant_count = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0]
        raw_dominant_share = dominant_count / total
        dominant_share = round(raw_dominant_share, 4)
        if raw_dominant_share <= threshold:
            continue
        reason = (
            f"{dominant_source_type} supplies {dominant_count}/{total} "
            f"sources ({dominant_share:.0%}), above threshold {threshold:.0%}"
        )
        findings.append(
            SynthesisInputDominanceFinding(
                content_id=content_id,
                run_id=run_id,
                total_source_count=total,
                source_type_counts=dict(sorted(counts.items())),
                dominant_source_type=dominant_source_type,
                dominant_share=dominant_share,
                reason=reason,
            )
        )

    findings.sort(key=lambda item: (-item.dominant_share, -item.total_source_count, item.content_id, item.run_id))
    shown = tuple(findings[:row_limit])
    return SynthesisInputSourceDiversityReport(
        generated_at=generated_at,
        dominance_threshold=threshold,
        limit=row_limit,
        total_groups=len(groups),
        total_source_count=sum(sum(counts.values()) for counts in groups.values()),
        findings=shown,
        schema_gaps=schema_gaps or {"missing_tables": [], "missing_columns": {}},
    )


def build_synthesis_input_source_diversity_report_from_db(db_or_conn: Any, **kwargs: Any) -> SynthesisInputSourceDiversityReport:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_source_rows(conn, schema) if not gaps["missing_tables"] else []
    return build_synthesis_input_source_diversity_report(rows, schema_gaps=gaps, **kwargs)


def format_synthesis_input_source_diversity_json(report: SynthesisInputSourceDiversityReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_synthesis_input_source_diversity_text(report: SynthesisInputSourceDiversityReport) -> str:
    totals = report.to_dict()["totals"]
    lines = [
        "Synthesis Input Source Diversity",
        f"Generated: {report.generated_at}",
        f"Filters: dominance_threshold={report.dominance_threshold:g} limit={report.limit}",
        f"Totals: groups={totals['group_count']} sources={totals['source_count']} flagged={totals['flagged_count']}",
    ]
    if not report.findings:
        lines.append("No synthesis input source dominance found.")
        return "\n".join(lines)
    lines.extend(["", "content_id | run_id | sources | dominant | share | counts | reason"])
    for item in report.findings:
        counts = ",".join(f"{key}:{value}" for key, value in item.source_type_counts.items())
        lines.append(
            f"{item.content_id or '-'} | {item.run_id or '-'} | {item.total_source_count} | "
            f"{item.dominant_source_type} | {item.dominant_share:.2f} | {counts} | {item.reason}"
        )
    return "\n".join(lines)


format_synthesis_input_source_diversity_table = format_synthesis_input_source_diversity_text


def normalize_source_type(value: Any) -> str:
    text = _text(value).lower().replace("-", "_").replace(" ", "_")
    if not text:
        return "unknown"
    if text in {"source_commits", "commit", "commits", "github_commit", "github_commits"}:
        return "commit"
    if text in {"source_messages", "message", "messages", "claude_message", "claude_messages", "claude_session"}:
        return "claude_session"
    if text in {"source_activity_ids", "github_activity", "github_issue", "github_pr", "pull_request"}:
        return "github_activity"
    if text.startswith("curated_") or text in {"knowledge", "content_knowledge", "knowledge_link"}:
        return "curated_knowledge"
    if text in {"reply", "reply_context", "reply_queue", "reply_knowledge"}:
        return "reply_context"
    return text


def _load_source_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    columns = schema.get("generated_content", set())
    if columns:
        selected = [
            "id AS content_id",
            _select(columns, ("generation_run_id", "run_id", "batch_id"), "run_id"),
            _select(columns, ("source_commits",), "source_commits"),
            _select(columns, ("source_messages",), "source_messages"),
            _select(columns, ("source_activity_ids",), "source_activity_ids"),
        ]
        for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content").fetchall():
            base = {"content_id": row["content_id"], "run_id": row["run_id"]}
            rows.extend(_json_source_rows(base, "commit", row["source_commits"]))
            rows.extend(_json_source_rows(base, "claude_session", row["source_messages"]))
            rows.extend(_json_source_rows(base, "github_activity", row["source_activity_ids"]))

    if _knowledge_links_available(schema):
        for row in conn.execute(
            """SELECT ckl.content_id,
                      k.source_type,
                      COALESCE(k.source_id, k.source_url, CAST(k.id AS TEXT)) AS source_id
               FROM content_knowledge_links ckl
               INNER JOIN knowledge k ON k.id = ckl.knowledge_id
               ORDER BY ckl.content_id ASC, k.id ASC"""
        ).fetchall():
            rows.append(
                {
                    "content_id": row["content_id"],
                    "run_id": "",
                    "source_type": row["source_type"],
                    "source_id": row["source_id"],
                }
            )
    return rows


def _json_source_rows(base: dict[str, Any], source_type: str, value: Any) -> list[dict[str, Any]]:
    return [
        {**base, "source_type": source_type, "source_id": _text(item)}
        for item in _json_list(value)
        if _text(item)
    ]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "generated_content" not in schema:
        return {"missing_tables": ["generated_content"], "missing_columns": {}}
    missing = [column for column in ("id",) if column not in schema["generated_content"]]
    return {"missing_tables": [], "missing_columns": {"generated_content": missing} if missing else {}}


def _knowledge_links_available(schema: dict[str, set[str]]) -> bool:
    return {"content_id", "knowledge_id"}.issubset(schema.get("content_knowledge_links", set())) and {
        "id",
        "source_type",
    }.issubset(schema.get("knowledge", set()))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _json_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _threshold(value: float) -> float:
    parsed = float(value)
    if not 0 <= parsed <= 1:
        raise ValueError("dominance_threshold must be between 0 and 1")
    return parsed


def _positive_int(value: int, name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()
