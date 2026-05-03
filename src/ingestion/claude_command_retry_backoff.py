"""Report backoff between repeated failed Claude Code commands."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Iterable, Mapping

from ingestion.claude_command_retry_effectiveness import normalize_command_signature
from ingestion.claude_command_retry_recovery import (
    ClaudeCommandEvent,
    DEFAULT_DAYS,
    SOURCE_TABLE_CANDIDATES,
    _connection,
    _ensure_utc,
    _is_row_iterable,
    _parse_datetime,
    _schema,
    load_claude_command_event_rows,
    load_claude_command_events,
)


DEFAULT_LIMIT = 50
DEFAULT_MIN_BACKOFF_SECONDS = 60


@dataclass(frozen=True)
class ClaudeCommandRetryBackoffRow:
    """One repeated failed command and the elapsed backoff before retry."""

    session_id: str
    project_path: str | None
    command_signature: str
    command_prefix: str
    previous_failed_at: str | None
    retry_failed_at: str | None
    elapsed_seconds: int | None
    is_too_fast: bool
    previous_command: str
    retry_command: str
    source_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source_tables"] = list(self.source_tables)
        return payload


@dataclass(frozen=True)
class ClaudeCommandRetryBackoffReport:
    """Claude command retry backoff report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeCommandRetryBackoffRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_command_retry_backoff",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_command_retry_backoff_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_backoff_seconds: int = DEFAULT_MIN_BACKOFF_SECONDS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeCommandRetryBackoffReport:
    """Build a deterministic report of repeated failed command retry backoff."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_backoff_seconds <= 0:
        raise ValueError("min_backoff_seconds must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    source_tables: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()

    if _is_row_iterable(db_or_rows):
        raw_rows = [dict(row) for row in db_or_rows]
        raw_rows = _filter_rows(raw_rows, cutoff=cutoff)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        source_tables = tuple(table for table in SOURCE_TABLE_CANDIDATES if table in schema)
        missing_tables = () if source_tables else SOURCE_TABLE_CANDIDATES
        raw_rows = [
            row
            for table in source_tables
            for row in load_claude_command_event_rows(conn, table, schema[table], cutoff=cutoff)
        ]

    events, malformed_metadata_count = load_claude_command_events(raw_rows)
    rows = detect_command_retry_backoffs(
        events,
        min_backoff_seconds=min_backoff_seconds,
    )
    reported = tuple(rows[:limit])

    return ClaudeCommandRetryBackoffReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "limit": limit,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
            "min_backoff_seconds": min_backoff_seconds,
        },
        totals={
            "command_group_count": len(_group_failed_events(events)),
            "malformed_metadata_count": malformed_metadata_count,
            "retry_pair_count": len(rows),
            "rows_scanned": len(raw_rows),
            "too_fast_count": sum(1 for row in rows if row.is_too_fast),
        },
        rows=reported,
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def detect_command_retry_backoffs(
    events: Iterable[ClaudeCommandEvent],
    *,
    min_backoff_seconds: int = DEFAULT_MIN_BACKOFF_SECONDS,
) -> list[ClaudeCommandRetryBackoffRow]:
    """Detect adjacent failed retries for the same command signature."""
    rows: list[ClaudeCommandRetryBackoffRow] = []
    for (_session_id, _signature), group_events in _group_failed_events(events).items():
        ordered = tuple(sorted(group_events, key=_event_sort_key))
        for previous, retry in zip(ordered, ordered[1:]):
            rows.append(
                _backoff_row(
                    previous,
                    retry,
                    min_backoff_seconds=min_backoff_seconds,
                )
            )
    return sorted(rows, key=_row_sort_key)


def format_claude_command_retry_backoff_json(
    report: ClaudeCommandRetryBackoffReport,
) -> str:
    """Serialize a command retry backoff report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_command_retry_backoff_text(
    report: ClaudeCommandRetryBackoffReport,
) -> str:
    """Render a concise command retry backoff report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Command Retry Backoff",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"min_backoff_seconds={filters['min_backoff_seconds']}"
        ),
        (
            "Totals: "
            f"rows={totals['rows_scanned']} groups={totals['command_group_count']} "
            f"retry_pairs={totals['retry_pair_count']} too_fast={totals['too_fast_count']} "
            f"malformed_metadata={totals['malformed_metadata_count']}"
        ),
    ]
    if report.source_tables:
        lines.append("Source tables: " + ", ".join(report.source_tables))
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.rows:
        lines.extend(["", "No repeated failed command retries found."])
        return "\n".join(lines)

    lines.extend(["", "Retry Backoffs:"])
    for row in report.rows:
        elapsed = "-" if row.elapsed_seconds is None else f"{row.elapsed_seconds}s"
        fast = " too_fast" if row.is_too_fast else ""
        lines.append(
            f"- session={row.session_id} elapsed={elapsed}{fast} "
            f"previous_failed_at={row.previous_failed_at or '-'} "
            f"retry_failed_at={row.retry_failed_at or '-'}"
        )
        lines.append(f"  command={row.command_signature}")
    return "\n".join(lines)


def _group_failed_events(
    events: Iterable[ClaudeCommandEvent],
) -> dict[tuple[str, str], tuple[ClaudeCommandEvent, ...]]:
    grouped: dict[tuple[str, str], list[ClaudeCommandEvent]] = {}
    for event in events:
        if event.status != "failed":
            continue
        key = (event.session_id, normalize_command_signature(event.command))
        grouped.setdefault(key, []).append(event)
    return {
        key: tuple(sorted(group_events, key=_event_sort_key))
        for key, group_events in sorted(grouped.items())
        if len(group_events) > 1
    }


def _backoff_row(
    previous: ClaudeCommandEvent,
    retry: ClaudeCommandEvent,
    *,
    min_backoff_seconds: int,
) -> ClaudeCommandRetryBackoffRow:
    elapsed_seconds = _elapsed_seconds(previous.timestamp, retry.timestamp)
    signature = normalize_command_signature(previous.command)
    return ClaudeCommandRetryBackoffRow(
        session_id=previous.session_id,
        project_path=previous.project_path or retry.project_path,
        command_signature=signature,
        command_prefix=previous.command_prefix,
        previous_failed_at=previous.timestamp,
        retry_failed_at=retry.timestamp,
        elapsed_seconds=elapsed_seconds,
        is_too_fast=elapsed_seconds is not None and elapsed_seconds < min_backoff_seconds,
        previous_command=previous.command,
        retry_command=retry.command,
        source_tables=tuple(sorted({previous.source_table, retry.source_table})),
    )


def _elapsed_seconds(start: str | None, end: str | None) -> int | None:
    start_at = _parse_datetime(start)
    end_at = _parse_datetime(end)
    if not start_at or not end_at:
        return None
    elapsed = int((end_at - start_at).total_seconds())
    return elapsed if elapsed >= 0 else None


def _filter_rows(rows: list[dict[str, Any]], *, cutoff: datetime) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        timestamp = row.get("timestamp") or row.get("created_at") or row.get("event_time") or row.get("event_at")
        parsed = _parse_datetime(timestamp)
        if parsed is None or parsed >= cutoff:
            filtered.append(row)
    return filtered


def _event_sort_key(event: ClaudeCommandEvent) -> tuple[str, str, str]:
    parsed = _parse_datetime(event.timestamp)
    return (
        parsed.isoformat() if parsed else str(event.timestamp or ""),
        event.session_id,
        event.command,
    )


def _row_sort_key(row: ClaudeCommandRetryBackoffRow) -> tuple[str, str, str]:
    parsed = _parse_datetime(row.retry_failed_at)
    return (
        parsed.isoformat() if parsed else str(row.retry_failed_at or ""),
        row.session_id,
        row.command_signature,
    )
