"""Report retry effectiveness for repeated Claude Code commands."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import shlex
import sqlite3
from typing import Any, Iterable, Mapping

from ingestion.claude_command_retry_recovery import (
    ClaudeCommandEvent,
    SOURCE_TABLE_CANDIDATES,
    load_claude_command_event_rows,
    load_claude_command_events,
)


DEFAULT_DAYS = 14


@dataclass(frozen=True)
class ClaudeCommandRetryEffectivenessRow:
    """One command retry effectiveness group."""

    session_id: str
    project_path: str | None
    command_signature: str
    command_prefix: str
    retry_outcome: str
    attempt_count: int
    failure_count: int
    success_count: int
    recovered_failure_count: int
    unresolved_failure_count: int
    first_seen_at: str | None
    last_seen_at: str | None
    first_failure_at: str | None
    last_failure_at: str | None
    first_success_at: str | None
    last_success_at: str | None
    representative_command: str
    source_tables: tuple[str, ...]
    signature_id: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source_tables"] = list(self.source_tables)
        return payload


@dataclass(frozen=True)
class ClaudeCommandRetryEffectivenessReport:
    """Claude command retry effectiveness report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeCommandRetryEffectivenessRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_command_retry_effectiveness",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_command_retry_effectiveness_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
) -> ClaudeCommandRetryEffectivenessReport:
    """Build a deterministic report of repeated command retry outcomes."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    source_tables: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()

    if _is_row_iterable(db_or_rows):
        raw_rows = [dict(row) for row in db_or_rows]
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
    grouped = group_command_events_by_session_and_signature(events)
    rows, single_success_count = classify_command_retry_groups(grouped)

    return ClaudeCommandRetryEffectivenessReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
        },
        totals={
            "attempt_event_count": len(events),
            "command_group_count": len(grouped),
            "flaky_group_count": sum(1 for row in rows if row.retry_outcome == "flaky"),
            "malformed_metadata_count": malformed_metadata_count,
            "recovered_group_count": sum(1 for row in rows if row.retry_outcome == "recovered"),
            "reported_group_count": len(rows),
            "rows_scanned": len(raw_rows),
            "single_success_group_count": single_success_count,
            "unresolved_group_count": sum(1 for row in rows if row.retry_outcome == "unresolved"),
        },
        rows=tuple(rows),
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def normalize_command_signature(command: str) -> str:
    """Normalize a command into a stable full-command matching signature."""
    text = _clean_command(command).lower()
    try:
        tokens = shlex.split(text)
    except ValueError:
        tokens = text.split()
    tokens = [token for token in tokens if token]
    while tokens and ("=" in tokens[0] and not tokens[0].startswith("-")):
        tokens.pop(0)
    if tokens[:2] == ["env", "-i"]:
        tokens = tokens[2:]
    if tokens and tokens[0] in {"sudo", "command", "exec"}:
        tokens = tokens[1:]
    return " ".join(tokens) or "unknown-command"


def group_command_events_by_session_and_signature(
    events: Iterable[ClaudeCommandEvent],
) -> dict[tuple[str, str], tuple[ClaudeCommandEvent, ...]]:
    """Group command events by Claude session and normalized command signature."""
    grouped: dict[tuple[str, str], list[ClaudeCommandEvent]] = {}
    for event in events:
        key = (event.session_id, normalize_command_signature(event.command))
        grouped.setdefault(key, []).append(event)
    return {
        key: tuple(sorted(group_events, key=_event_sort_key))
        for key, group_events in sorted(grouped.items())
    }


def classify_command_retry_groups(
    groups: Mapping[tuple[str, str], Iterable[ClaudeCommandEvent]],
) -> tuple[list[ClaudeCommandRetryEffectivenessRow], int]:
    """Classify grouped command histories as recovered, unresolved, or flaky."""
    rows: list[ClaudeCommandRetryEffectivenessRow] = []
    single_success_count = 0
    for (session_id, command_signature), group_events in sorted(groups.items()):
        ordered = tuple(sorted(group_events, key=_event_sort_key))
        failures = tuple(event for event in ordered if event.status == "failed")
        successes = tuple(event for event in ordered if event.status == "succeeded")
        if not failures:
            if len(ordered) == 1 and successes:
                single_success_count += 1
            continue
        rows.append(_effectiveness_row(session_id, command_signature, ordered))
    return sorted(rows, key=_row_sort_key), single_success_count


def format_claude_command_retry_effectiveness_json(
    report: ClaudeCommandRetryEffectivenessReport,
) -> str:
    """Serialize a Claude command retry effectiveness report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _effectiveness_row(
    session_id: str,
    command_signature: str,
    events: tuple[ClaudeCommandEvent, ...],
) -> ClaudeCommandRetryEffectivenessRow:
    failures = tuple(event for event in events if event.status == "failed")
    successes = tuple(event for event in events if event.status == "succeeded")
    recovered_failure_count = sum(1 for failure in failures if _has_later_success(failure, successes))
    unresolved_failure_count = len(failures) - recovered_failure_count
    command_prefix = events[0].command_prefix
    signature_hash = hashlib.sha256(f"{session_id}:{command_signature}".encode("utf-8")).hexdigest()[
        :12
    ]
    return ClaudeCommandRetryEffectivenessRow(
        session_id=session_id,
        project_path=events[0].project_path,
        command_signature=command_signature,
        command_prefix=command_prefix,
        retry_outcome=_retry_outcome(events, recovered_failure_count),
        attempt_count=len(events),
        failure_count=len(failures),
        success_count=len(successes),
        recovered_failure_count=recovered_failure_count,
        unresolved_failure_count=unresolved_failure_count,
        first_seen_at=events[0].timestamp,
        last_seen_at=events[-1].timestamp,
        first_failure_at=failures[0].timestamp if failures else None,
        last_failure_at=failures[-1].timestamp if failures else None,
        first_success_at=successes[0].timestamp if successes else None,
        last_success_at=successes[-1].timestamp if successes else None,
        representative_command=events[0].command,
        source_tables=tuple(sorted({event.source_table for event in events})),
        signature_id=f"claude_command_retry_effectiveness_{signature_hash}",
    )


def _retry_outcome(
    events: tuple[ClaudeCommandEvent, ...],
    recovered_failure_count: int,
) -> str:
    statuses = tuple(event.status for event in events)
    if "succeeded" in statuses and _has_failure_after_success(events):
        return "flaky"
    if recovered_failure_count:
        return "recovered"
    return "unresolved"


def _has_later_success(
    failure: ClaudeCommandEvent,
    successes: Iterable[ClaudeCommandEvent],
) -> bool:
    failure_at = _parse_datetime(failure.timestamp)
    if not failure_at:
        return False
    return any(
        success_at is not None and success_at >= failure_at
        for success_at in (_parse_datetime(success.timestamp) for success in successes)
    )


def _has_failure_after_success(events: tuple[ClaudeCommandEvent, ...]) -> bool:
    saw_success = False
    for event in events:
        if event.status == "succeeded":
            saw_success = True
        elif event.status == "failed" and saw_success:
            return True
    return False


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in rows}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {
            str(row["name"])
            for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
    except sqlite3.Error:
        return set()


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _event_sort_key(event: ClaudeCommandEvent) -> tuple[str, str, str, str]:
    return (
        _timestamp_sort(event.timestamp),
        event.session_id,
        event.status,
        event.command,
    )


def _row_sort_key(row: ClaudeCommandRetryEffectivenessRow) -> tuple[str, str, str]:
    return (
        _timestamp_sort(row.first_failure_at),
        row.session_id,
        row.command_signature,
    )


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean_command(command: str) -> str:
    text = " ".join(str(command).strip().strip("`'\"").split())
    text = re.sub(r"^\$\s*", "", text)
    return text.strip(" .")


def _is_row_iterable(value: Any) -> bool:
    return not hasattr(value, "execute") and not hasattr(value, "conn") and not isinstance(
        value,
        (str, bytes),
    )
