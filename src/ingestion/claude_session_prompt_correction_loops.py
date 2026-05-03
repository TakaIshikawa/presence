"""Detect repeated Claude session prompt correction loops."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping

from ingestion.claude_command_retry_recovery import (
    _event_status,
    _error_text,
)
from ingestion.claude_session_approval_decision_audit import (
    PROJECT_COLUMNS,
    SESSION_COLUMNS,
    STATUS_COLUMNS,
    TEXT_COLUMNS,
    TIMESTAMP_COLUMNS,
    _ensure_utc,
    _first_text,
    _metadata,
    _nested_text,
    _parse_datetime,
    _tool_name,
)


DEFAULT_LIMIT = 50
DEFAULT_SNIPPET_CHARS = 160

ROLE_COLUMNS = ("role", "speaker", "message_role", "author")
MESSAGE_COLUMNS = TEXT_COLUMNS + ("prompt_text", "error_message", "stderr")

CORRECTION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("retry_request", re.compile(r"\b(try again|retry|rerun|run it again)\b", re.IGNORECASE)),
    ("wrong_target", re.compile(r"\b(wrong|incorrect|not that|instead|actually)\b", re.IGNORECASE)),
    ("clarification", re.compile(r"\b(i meant|what i meant|to clarify|correction)\b", re.IGNORECASE)),
    ("missing_context", re.compile(r"\b(use the (?:existing|current)|you missed|missing context)\b", re.IGNORECASE)),
)
PARSE_ERROR_RE = re.compile(
    r"\b(parse error|jsondecodeerror|invalid json|syntaxerror|yaml error)\b",
    re.IGNORECASE,
)
MISSING_CONTEXT_RE = re.compile(
    r"\b(missing context|no such file|file not found|unknown reference|cannot find)\b",
    re.IGNORECASE,
)
TOOL_ERROR_RE = re.compile(
    r"\b(error|failed|failure|exit code [1-9]\d*|non[- ]?zero|traceback|exception)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ClaudePromptCorrectionLoopRow:
    """One grouped correction loop inside a Claude session."""

    session_id: str
    project_path: str | None
    trigger_type: str
    correction_kind: str
    correction_count: int
    first_seen_at: str | None
    last_seen_at: str | None
    trigger_snippets: tuple[str, ...]
    correction_snippets: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["trigger_snippets"] = list(self.trigger_snippets)
        payload["correction_snippets"] = list(self.correction_snippets)
        return payload


@dataclass(frozen=True)
class ClaudePromptCorrectionLoopReport:
    """Claude prompt correction loop report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudePromptCorrectionLoopRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_prompt_correction_loops",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _ClaudePromptEvent:
    session_id: str
    project_path: str | None
    timestamp: str | None
    role: str
    tool_name: str
    status: str | None
    text: str
    error_text: str | None
    metadata: Mapping[str, Any]
    ordinal: int


@dataclass(frozen=True)
class _CorrectionEvent:
    event: _ClaudePromptEvent
    trigger: _ClaudePromptEvent | None
    trigger_type: str
    correction_kind: str


def build_claude_session_prompt_correction_loops_report(
    rows: Iterable[Mapping[str, Any]],
    *,
    max_snippet_chars: int = DEFAULT_SNIPPET_CHARS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudePromptCorrectionLoopReport:
    """Build a deterministic report of repeated correction loops."""
    if max_snippet_chars <= 0:
        raise ValueError("max_snippet_chars must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    raw_rows = [_mapping(row) for row in rows]
    events, malformed_metadata_count = load_prompt_correction_events(raw_rows)
    loop_rows = detect_prompt_correction_loops(
        events,
        max_snippet_chars=max_snippet_chars,
    )
    reported = tuple(loop_rows[:limit])

    return ClaudePromptCorrectionLoopReport(
        generated_at=generated_at.isoformat(),
        filters={"limit": limit, "max_snippet_chars": max_snippet_chars},
        totals={
            "correction_loop_count": len(loop_rows),
            "malformed_metadata_count": malformed_metadata_count,
            "reported_count": len(reported),
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
        },
        rows=reported,
    )


def load_prompt_correction_events(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[_ClaudePromptEvent], int]:
    """Normalize parsed Claude session rows into prompt/tool events."""
    events: list[_ClaudePromptEvent] = []
    malformed_metadata_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        text = _event_text(row, metadata)
        error_text = _error_text(row, metadata)
        status = _first_text(row, STATUS_COLUMNS) or _first_text(metadata, STATUS_COLUMNS)
        events.append(
            _ClaudePromptEvent(
                session_id=(
                    _first_text(row, SESSION_COLUMNS)
                    or _first_text(metadata, SESSION_COLUMNS)
                    or "unknown-session"
                ),
                project_path=_first_text(row, PROJECT_COLUMNS) or _first_text(metadata, PROJECT_COLUMNS),
                timestamp=_first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS),
                role=_normalize_role(_first_text(row, ROLE_COLUMNS) or _first_text(metadata, ROLE_COLUMNS)),
                tool_name=_tool_name(row, metadata),
                status=status,
                text=text,
                error_text=error_text,
                metadata=metadata,
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def detect_prompt_correction_loops(
    events: Iterable[_ClaudePromptEvent],
    *,
    max_snippet_chars: int = DEFAULT_SNIPPET_CHARS,
) -> list[ClaudePromptCorrectionLoopRow]:
    """Detect repeated correction phrases after session error triggers."""
    sessions: dict[str, list[_ClaudePromptEvent]] = {}
    for event in events:
        sessions.setdefault(event.session_id, []).append(event)

    corrections: list[_CorrectionEvent] = []
    for _session_id, session_events in sorted(sessions.items()):
        latest_trigger: _ClaudePromptEvent | None = None
        latest_trigger_type: str | None = None
        for event in sorted(session_events, key=_event_sort_key):
            correction_kind = classify_correction(event.text)
            if correction_kind and (latest_trigger_type or event.role in {"user", "assistant"}):
                if latest_trigger_type:
                    trigger = latest_trigger
                    correction_trigger_type = latest_trigger_type
                else:
                    trigger = None
                    correction_trigger_type = "user_clarification"
                corrections.append(
                    _CorrectionEvent(
                        event=event,
                        trigger=trigger,
                        trigger_type=correction_trigger_type,
                        correction_kind=correction_kind,
                    )
                )
                continue
            trigger_type = classify_trigger(event)
            if trigger_type:
                latest_trigger = event
                latest_trigger_type = trigger_type
                continue

    grouped: dict[tuple[str, str], list[_CorrectionEvent]] = {}
    for correction in corrections:
        key = (
            correction.event.session_id,
            correction.trigger_type,
        )
        grouped.setdefault(key, []).append(correction)

    rows = [
        _loop_row(group, max_snippet_chars=max_snippet_chars)
        for group in grouped.values()
        if len(group) >= 2
    ]
    return sorted(rows, key=_row_sort_key)


def classify_trigger(event: _ClaudePromptEvent) -> str | None:
    """Classify a failed tool output or error-like event."""
    text = " ".join(part for part in (event.status, event.text, event.error_text) if part)
    if PARSE_ERROR_RE.search(text):
        return "parse_error"
    if MISSING_CONTEXT_RE.search(text):
        return "missing_context"
    if _status_bucket(event) == "failed" or TOOL_ERROR_RE.search(text):
        return "tool_error"
    return None


def classify_correction(text: str) -> str | None:
    """Classify a correction phrase in user or assistant text."""
    for correction_kind, pattern in CORRECTION_PATTERNS:
        if pattern.search(text):
            return correction_kind
    return None


def read_claude_session_rows(path: str | Path) -> list[dict[str, Any]]:
    """Read parsed Claude session rows from a JSON or JSONL file."""
    raw = Path(path).read_text(encoding="utf-8")
    if not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        rows: list[dict[str, Any]] = []
        for line_number, line in enumerate(raw.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at line {line_number}: {exc}") from exc
            if not isinstance(item, Mapping):
                raise ValueError(f"expected object at JSONL line {line_number}")
            rows.append(dict(item))
        return rows
    if isinstance(parsed, Mapping):
        if isinstance(parsed.get("rows"), list):
            return [dict(item) for item in parsed["rows"] if isinstance(item, Mapping)]
        return [dict(parsed)]
    if isinstance(parsed, list):
        return [dict(item) for item in parsed if isinstance(item, Mapping)]
    raise ValueError("expected a JSON object, JSON array, or JSONL objects")


def format_claude_session_prompt_correction_loops_json(
    report: ClaudePromptCorrectionLoopReport,
) -> str:
    """Serialize a prompt correction loop report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _loop_row(
    corrections: list[_CorrectionEvent],
    *,
    max_snippet_chars: int,
) -> ClaudePromptCorrectionLoopRow:
    ordered = sorted(corrections, key=lambda correction: _event_sort_key(correction.event))
    first = ordered[0]
    trigger_snippets = tuple(
        dict.fromkeys(
            _snippet(correction.trigger.text or correction.trigger.error_text, max_snippet_chars)
            for correction in ordered
            if correction.trigger
        )
    )
    correction_snippets = tuple(
        dict.fromkeys(
            _snippet(correction.event.text, max_snippet_chars)
            for correction in ordered
            if correction.event.text
        )
    )
    return ClaudePromptCorrectionLoopRow(
        session_id=first.event.session_id,
        project_path=first.event.project_path,
        trigger_type=first.trigger_type,
        correction_kind=_correction_kind(ordered),
        correction_count=len(ordered),
        first_seen_at=ordered[0].event.timestamp,
        last_seen_at=ordered[-1].event.timestamp,
        trigger_snippets=trigger_snippets[:3],
        correction_snippets=correction_snippets[:3],
    )


def _correction_kind(corrections: list[_CorrectionEvent]) -> str:
    kinds = tuple(dict.fromkeys(correction.correction_kind for correction in corrections))
    return kinds[0] if len(kinds) == 1 else "mixed"


def _event_text(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for source in (row, metadata):
        for column in MESSAGE_COLUMNS:
            value = source.get(column)
            if isinstance(value, (str, int, float)) and str(value).strip():
                parts.append(str(value).strip())
            elif isinstance(value, Mapping):
                nested = _first_text(value, MESSAGE_COLUMNS)
                if nested:
                    parts.append(nested)
    for path in (
        ("message", "content"),
        ("message", "text"),
        ("tool_result", "content"),
        ("tool_result", "error"),
        ("result", "error"),
    ):
        nested = _nested_text(metadata, path)
        if nested:
            parts.append(nested)
    return "\n".join(dict.fromkeys(parts))


def _status_bucket(event: _ClaudePromptEvent) -> str | None:
    return _event_status(
        {
            "status": event.status,
            "content": event.text,
            "error_message": event.error_text,
        },
        dict(event.metadata),
    )


def _normalize_role(value: str | None) -> str:
    text = re.sub(r"[^a-z0-9_.-]+", "_", str(value or "").lower()).strip("_")
    return text or "unknown"


def _snippet(value: Any, max_chars: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)].rstrip() + "..."


def _event_sort_key(event: _ClaudePromptEvent) -> tuple[str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.ordinal)


def _row_sort_key(row: ClaudePromptCorrectionLoopRow) -> tuple[int, str, str, str]:
    return (-row.correction_count, row.session_id, row.trigger_type, row.correction_kind)


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)
