"""Report likely context switches inside Claude Code sessions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Iterable, Mapping

from ingestion.claude_session_topic_drift import (
    DEFAULT_DAYS,
    DEFAULT_EXCERPT_CHARS,
    DEFAULT_LIMIT,
    DEFAULT_THRESHOLD,
    _connection,
    _ensure_utc,
    _filter_rows,
    _load_rows,
    _looks_like_rows,
    _missing_columns,
    _optional_text,
    _parse_datetime,
    _row_sort_key,
    _schema,
    jaccard_distance,
    tokenize_prompt_keywords,
)


@dataclass(frozen=True)
class ClaudeSessionContextSwitchRow:
    """One adjacent-message context switch inside a Claude session."""

    session_id: str
    from_message_uuid: str | None
    to_message_uuid: str | None
    from_timestamp: str | None
    to_timestamp: str | None
    from_project_path: str | None
    to_project_path: str | None
    switch_type: str
    switch_score: float
    from_excerpt: str
    to_excerpt: str
    from_keywords: tuple[str, ...]
    to_keywords: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["from_keywords"] = list(self.from_keywords)
        payload["to_keywords"] = list(self.to_keywords)
        return payload


@dataclass(frozen=True)
class ClaudeSessionContextSwitchReport:
    """Claude session context switch report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionContextSwitchRow, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_context_switches",
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


def build_claude_session_context_switches_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    project_path: str | None = None,
    threshold: float = DEFAULT_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionContextSwitchReport:
    """Build a deterministic report of likely session context switches."""
    if days <= 0:
        raise ValueError("days must be positive")
    if threshold <= 0 or threshold > 1:
        raise ValueError("threshold must be greater than 0 and at most 1")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    project_path = _optional_text(project_path)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "project_path": project_path,
        "project_path_filter_applied": False,
        "threshold": threshold,
    }

    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}
    if _looks_like_rows(db_or_rows):
        rows = [dict(row) for row in db_or_rows]
        rows = _filter_rows(rows, cutoff=cutoff, project_path=project_path)
        filters["project_path_filter_applied"] = bool(project_path)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        if "claude_messages" not in schema:
            missing_tables = ("claude_messages",)
            rows = []
        else:
            columns = schema["claude_messages"]
            missing_columns = _missing_columns(columns)
            rows = _load_rows(conn, columns, cutoff=cutoff, project_path=project_path)
            filters["project_path_filter_applied"] = bool(
                project_path and "project_path" in columns
            )

    scanned_session_count = len({str(row.get("session_id") or "unknown-session") for row in rows})
    switches = detect_context_switches(rows, threshold=threshold)
    switches.sort(key=_switch_sort_key)
    reported = tuple(switches[:limit])
    return ClaudeSessionContextSwitchReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "messages_scanned": len(rows),
            "sessions_scanned": scanned_session_count,
            "switch_count": len(switches),
            "switch_sessions": len({row.session_id for row in switches}),
        },
        rows=reported,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def detect_context_switches(
    rows: Iterable[Mapping[str, Any]],
    *,
    threshold: float = DEFAULT_THRESHOLD,
) -> list[ClaudeSessionContextSwitchRow]:
    """Detect adjacent prompt transitions that look like context switches."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        prompt = _optional_text(row.get("prompt_text"))
        if not prompt:
            continue
        session_id = str(row.get("session_id") or "unknown-session")
        grouped.setdefault(session_id, []).append({**dict(row), "prompt_text": prompt})

    switches: list[ClaudeSessionContextSwitchRow] = []
    for session_id, session_rows in sorted(grouped.items()):
        ordered = sorted(session_rows, key=_row_sort_key)
        for previous, current in zip(ordered, ordered[1:]):
            previous_project = _optional_text(previous.get("project_path"))
            current_project = _optional_text(current.get("project_path"))
            previous_keywords = tokenize_prompt_keywords(previous["prompt_text"])
            current_keywords = tokenize_prompt_keywords(current["prompt_text"])
            drift = jaccard_distance(previous_keywords, current_keywords)
            project_changed = bool(
                previous_project and current_project and previous_project != current_project
            )
            if not project_changed and drift < threshold:
                continue
            switches.append(
                ClaudeSessionContextSwitchRow(
                    session_id=session_id,
                    from_message_uuid=_optional_text(previous.get("message_uuid")),
                    to_message_uuid=_optional_text(current.get("message_uuid")),
                    from_timestamp=_optional_text(previous.get("timestamp")),
                    to_timestamp=_optional_text(current.get("timestamp")),
                    from_project_path=previous_project,
                    to_project_path=current_project,
                    switch_type="project_path_changed" if project_changed else "topic_shift",
                    switch_score=round(1.0 if project_changed else drift, 6),
                    from_excerpt=_excerpt(previous["prompt_text"]),
                    to_excerpt=_excerpt(current["prompt_text"]),
                    from_keywords=tuple(sorted(previous_keywords)),
                    to_keywords=tuple(sorted(current_keywords)),
                )
            )
    return switches


def format_claude_session_context_switches_json(
    report: ClaudeSessionContextSwitchReport,
) -> str:
    """Serialize a context-switch report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_context_switches_text(
    report: ClaudeSessionContextSwitchReport,
) -> str:
    """Render a concise context-switch report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Session Context Switches",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"threshold={filters['threshold']:.2f} "
            f"project_path={filters['project_path'] or '-'}"
        ),
        (
            "Totals: "
            f"sessions={totals['sessions_scanned']} "
            f"messages={totals['messages_scanned']} "
            f"switches={totals['switch_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.rows:
        lines.extend(["", "No context switches detected."])
        return "\n".join(lines)
    lines.extend(["", "Switches:"])
    for row in report.rows:
        lines.append(
            f"- session={row.session_id} type={row.switch_type} "
            f"score={row.switch_score:.3f} "
            f"{row.from_timestamp or '-'} -> {row.to_timestamp or '-'}"
        )
        lines.append(f"  from: {row.from_excerpt}")
        lines.append(f"  to: {row.to_excerpt}")
    return "\n".join(lines)


def _switch_sort_key(row: ClaudeSessionContextSwitchRow) -> tuple[float, str, str]:
    timestamp = _parse_datetime(row.to_timestamp)
    return (
        -row.switch_score,
        timestamp.isoformat() if timestamp else str(row.to_timestamp or ""),
        row.session_id,
    )


def _excerpt(text: str, max_chars: int = DEFAULT_EXCERPT_CHARS) -> str:
    compact = " ".join(text.split())
    if len(compact) <= max_chars:
        return compact
    return compact[: max(0, max_chars - 3)].rstrip() + "..."
