"""Export interruption markers from parsed Claude Code session artifacts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_SINCE_DAYS = 14
RECENT_UNFINISHED_PLAN_DAYS = 3
EXCERPT_CHARS = 320
SOURCE_NAME = "claude_session_interruption_export"


@dataclass(frozen=True)
class ClaudeSessionInterruption:
    """One interruption or unfinished-work marker from a Claude Code session."""

    interruption_id: str
    session_id: str
    timestamp: str | None
    project_path: str | None
    excerpt: str
    marker_type: str
    priority: int
    suggested_follow_up_priority: int
    reason: str
    message_id: int | None
    message_uuid: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _Marker:
    marker_type: str
    excerpt: str
    identity_text: str
    priority: int
    reason: str


def export_claude_session_interruptions(
    db_or_rows: Any,
    *,
    since: str | datetime | None = None,
    now: datetime | None = None,
) -> list[ClaudeSessionInterruption]:
    """Return interruption records from Claude session rows or a database handle."""

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = _parse_since(since, now=generated_at)
    rows = (
        list(db_or_rows)
        if _looks_like_rows(db_or_rows)
        else _load_claude_message_rows(db_or_rows, since=cutoff)
    )
    if cutoff is not None:
        filtered_rows = []
        for row in rows:
            mapped = _mapping(row)
            timestamp = _row_timestamp(mapped)
            if timestamp is None or timestamp >= cutoff:
                filtered_rows.append(mapped)
        rows = filtered_rows

    candidates: list[ClaudeSessionInterruption] = []
    for raw_row in rows:
        row = _mapping(raw_row)
        text = _row_text(row)
        if not text:
            continue
        metadata = _row_metadata(row)
        timestamp = _optional_text(metadata.get("timestamp"))
        row_time = _parse_datetime(timestamp)
        for marker in _extract_markers(text, timestamp=row_time, now=generated_at):
            candidates.append(_record_from_marker(marker, metadata))
    return _dedupe_records(candidates)


def format_claude_session_interruptions_json(
    records: list[ClaudeSessionInterruption],
) -> str:
    """Render interruption records as deterministic JSON."""

    return json.dumps([record.to_dict() for record in records], indent=2, sort_keys=True)


def format_claude_session_interruptions_markdown(
    records: list[ClaudeSessionInterruption],
) -> str:
    """Render interruption records as compact Markdown."""

    lines = ["# Claude Session Interruptions", "", f"interruptions: {len(records)}"]
    if not records:
        lines.extend(["", "No Claude session interruptions found."])
        return "\n".join(lines)

    for record in records:
        lines.extend(
            [
                "",
                f"## {record.marker_type} - priority {record.priority}",
                f"- session: {record.session_id}",
                f"- timestamp: {record.timestamp or '-'}",
                f"- project: {record.project_path or '-'}",
                f"- reason: {record.reason}",
                f"- excerpt: {record.excerpt}",
            ]
        )
    return "\n".join(lines)


def _extract_markers(
    text: str,
    *,
    timestamp: datetime | None,
    now: datetime,
) -> list[_Marker]:
    lines = [
        _clean_text(line.strip(" \t-*•>"))
        for line in text.replace("\r\n", "\n").splitlines()
    ]
    lines = [line for line in lines if line]
    markers: list[_Marker] = []
    for index, line in enumerate(lines):
        lowered = line.lower()
        marker_type = ""
        priority = 0
        reasons: list[str] = []

        if _is_aborted_tool_call(lowered):
            marker_type = "aborted_tool_call"
            priority = 3
            reasons.append("aborted tool-call marker")
        elif _is_user_cancellation(lowered):
            marker_type = "user_cancellation"
            priority = 3
            reasons.append("user cancellation marker")
        elif _is_todo_handoff(lowered):
            marker_type = "todo_handoff"
            priority = 4
            reasons.append("explicit TODO or handoff")
        elif _is_unfinished_plan(lowered):
            marker_type = "unfinished_plan"
            priority = 2
            reasons.append("unfinished plan marker")
        else:
            continue

        if marker_type == "todo_handoff" and _has_implementation_signal(lowered):
            priority += 1
            reasons.append("implementation follow-up")
        if marker_type == "unfinished_plan" and _is_recent(timestamp, now):
            priority += 1
            reasons.append("recent unfinished plan")
        if marker_type in {"aborted_tool_call", "user_cancellation"} and _has_implementation_signal(lowered):
            priority += 1
            reasons.append("work context")

        markers.append(
            _Marker(
                marker_type=marker_type,
                excerpt=_shorten(_snippet(lines, index)),
                identity_text=line,
                priority=max(1, min(priority, 5)),
                reason=", ".join(reasons),
            )
        )
    return markers


def _record_from_marker(
    marker: _Marker,
    metadata: dict[str, Any],
) -> ClaudeSessionInterruption:
    session_id = str(metadata.get("session_id") or metadata.get("sessionId") or "plain-transcript")
    timestamp = _optional_text(metadata.get("timestamp"))
    project_path = _optional_text(metadata.get("project_path") or metadata.get("cwd") or metadata.get("project"))
    message_id = _int_or_none(metadata.get("id") or metadata.get("message_id"))
    message_uuid = _optional_text(metadata.get("message_uuid") or metadata.get("uuid"))
    interruption_id = _interruption_id(session_id, marker.marker_type, marker.identity_text)
    return ClaudeSessionInterruption(
        interruption_id=interruption_id,
        session_id=session_id,
        timestamp=timestamp,
        project_path=project_path,
        excerpt=marker.excerpt,
        marker_type=marker.marker_type,
        priority=marker.priority,
        suggested_follow_up_priority=marker.priority,
        reason=marker.reason,
        message_id=message_id,
        message_uuid=message_uuid,
    )


def _load_claude_message_rows(
    db_or_conn: Any,
    *,
    since: datetime | None,
) -> list[dict[str, Any]]:
    if hasattr(db_or_conn, "conn"):
        conn = db_or_conn.conn
    elif isinstance(db_or_conn, sqlite3.Connection):
        conn = db_or_conn
    else:
        getter = getattr(db_or_conn, "get_messages_in_range", None)
        if callable(getter):
            start = since or datetime.now(timezone.utc) - timedelta(days=DEFAULT_SINCE_DAYS)
            end = datetime.now(timezone.utc) + timedelta(seconds=1)
            return [dict(row) for row in getter(start, end)]
        return []

    if not _has_table(conn, "claude_messages"):
        return []
    conn.row_factory = sqlite3.Row
    if since is None:
        rows = conn.execute(
            """SELECT * FROM claude_messages
               ORDER BY timestamp ASC, id ASC"""
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT * FROM claude_messages
               WHERE timestamp >= ?
               ORDER BY timestamp ASC, id ASC""",
            (since.isoformat(),),
        ).fetchall()
    return [dict(row) for row in rows]


def _parse_since(value: str | datetime | None, *, now: datetime) -> datetime | None:
    if value is None:
        return now - timedelta(days=DEFAULT_SINCE_DAYS)
    if isinstance(value, datetime):
        return _ensure_utc(value)
    cleaned = str(value).strip()
    if not cleaned:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", cleaned):
        return datetime.fromisoformat(cleaned).replace(tzinfo=timezone.utc)
    return _ensure_utc(datetime.fromisoformat(cleaned.replace("Z", "+00:00")))


def _row_text(row: dict[str, Any]) -> str:
    for key in ("transcript", "prompt_text", "content", "text", "message", "body"):
        value = row.get(key)
        text = _content_text(value)
        if text:
            return text
    return ""


def _content_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return _content_text(value.get("content"))
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(_content_text(item.get("text") or item.get("content")))
            else:
                parts.append(_content_text(item))
        return "\n".join(part for part in parts if part)
    return ""


def _row_metadata(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in (
            "id",
            "message_id",
            "message_uuid",
            "uuid",
            "session_id",
            "sessionId",
            "project_path",
            "project",
            "cwd",
            "timestamp",
        )
        if key in row
    }


def _dedupe_records(
    records: Iterable[ClaudeSessionInterruption],
) -> list[ClaudeSessionInterruption]:
    best: dict[str, ClaudeSessionInterruption] = {}
    for record in records:
        existing = best.get(record.interruption_id)
        if existing is None or _record_rank(record) > _record_rank(existing):
            best[record.interruption_id] = record
    return sorted(
        best.values(),
        key=lambda record: (
            -record.priority,
            str(record.timestamp or ""),
            record.session_id,
            record.marker_type,
            record.interruption_id,
        ),
    )


def _record_rank(record: ClaudeSessionInterruption) -> tuple[int, str, str]:
    return (record.priority, str(record.timestamp or ""), str(record.message_uuid or ""))


def _is_aborted_tool_call(lowered: str) -> bool:
    return bool(
        re.search(
            r"\b(tool call|tool use|tool_use|bash|edit|read|write|mcp|command)\b"
            r".*\b(aborted|interrupted|terminated)\b|"
            r"\b(aborted|interrupted|terminated)\b.*\b(tool call|tool use|tool_use|bash|command)\b",
            lowered,
        )
    )


def _is_user_cancellation(lowered: str) -> bool:
    return bool(
        re.search(
            r"\b(cancelled|canceled|interrupted|stopped)\s+by\s+user\b|"
            r"\buser\s+(cancelled|canceled|interrupted|stopped)\b|"
            r"\b(ctrl-c|keyboardinterrupt|escape pressed|esc pressed)\b",
            lowered,
        )
    )


def _is_todo_handoff(lowered: str) -> bool:
    return bool(
        re.search(
            r"^(todo|to do|follow[- ]?up|handoff|action item|next step|next time)\s*[:\-]|"
            r"\b(todo|follow[- ]?up|handoff for next session|left for next session)\b",
            lowered,
        )
    )


def _is_unfinished_plan(lowered: str) -> bool:
    return bool(
        re.search(
            r"^(unfinished plan|remaining work|next steps?|still to do)\s*[:\-]|"
            r"\b(still need to|need to finish|wasn't able to finish|not yet completed|before stopping)\b",
            lowered,
        )
    )


def _has_implementation_signal(lowered: str) -> bool:
    return bool(
        re.search(
            r"\b(add|build|create|implement|update|fix|refactor|test|verify|run|document|export|wire|"
            r"persist|commit|tool|cli|schema|database)\b",
            lowered,
        )
    )


def _is_recent(timestamp: datetime | None, now: datetime) -> bool:
    if timestamp is None:
        return False
    return now - timedelta(days=RECENT_UNFINISHED_PLAN_DAYS) <= timestamp <= now + timedelta(seconds=1)


def _snippet(lines: list[str], index: int) -> str:
    start = max(0, index - 1)
    end = min(len(lines), index + 2)
    return _clean_text(" ".join(lines[start:end]))


def _interruption_id(session_id: str, marker_type: str, excerpt: str) -> str:
    identity = f"{session_id}|{marker_type}|{_normalize_excerpt(excerpt)}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"claude_interrupt_{digest}"


def _normalize_excerpt(excerpt: str) -> str:
    value = _clean_text(excerpt).lower()
    value = re.sub(r"`{1,3}", "", value)
    value = re.sub(r"https?://\S+", "<url>", value)
    value = re.sub(r"(/[^\s:]+)+", "<path>", value)
    value = re.sub(r"\b[a-f0-9]{7,40}\b", "<hash>", value)
    value = re.sub(r"\b\d+\b", "<num>", value)
    value = re.sub(r"[^a-z0-9<> ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _looks_like_rows(value: Any) -> bool:
    if isinstance(value, (str, bytes, Mapping, sqlite3.Connection)):
        return False
    if hasattr(value, "conn") or callable(getattr(value, "get_messages_in_range", None)):
        return False
    return isinstance(value, Iterable)


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row)


def _row_timestamp(row: dict[str, Any]) -> datetime | None:
    return _parse_datetime(row.get("timestamp"))


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if not value:
        return None
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _shorten(text: str | None, width: int = EXCERPT_CHARS) -> str:
    value = _clean_text(text or "")
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
