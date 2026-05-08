"""Export Claude Code action items as future content idea seeds."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable


SOURCE_NAME = "claude_action_item_export"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 25
DEFAULT_MIN_CONFIDENCE = 0.68
EXCERPT_CHARS = 300


@dataclass(frozen=True)
class ClaudeActionItemExport:
    action_item_id: str
    action_item: str
    excerpt: str
    confidence: float
    suggested_content_angle: str
    session_id: str
    session_path: str | None
    project_path: str | None
    message_id: int | None
    message_uuid: str | None
    timestamp: str | None
    signal_type: str
    reason: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _Signal:
    signal_type: str
    action_item: str
    excerpt: str
    confidence: float
    reason: str


def extract_claude_action_items_from_text(
    text: str,
    *,
    session_metadata: dict[str, Any] | None = None,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> list[ClaudeActionItemExport]:
    """Extract committed action-item and decision follow-up signals from text."""
    _validate_confidence(min_confidence)
    metadata = dict(session_metadata or {})
    candidates = [
        _candidate_from_signal(signal, metadata)
        for signal in _extract_signals(text)
        if signal.confidence >= min_confidence
    ]
    return _dedupe_candidates(candidates)


def extract_claude_action_items_from_rows(
    rows: Iterable[dict[str, Any]],
    *,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> list[ClaudeActionItemExport]:
    """Extract action-item exports from stored Claude message/session rows."""
    _validate_confidence(min_confidence)
    candidates: list[ClaudeActionItemExport] = []
    for row in rows:
        text = _row_text(row)
        if not text:
            continue
        candidates.extend(
            extract_claude_action_items_from_text(
                text,
                session_metadata=_row_metadata(row),
                min_confidence=min_confidence,
            )
        )
    return _dedupe_candidates(candidates)


def build_claude_action_item_exports(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    now: datetime | None = None,
) -> list[ClaudeActionItemExport]:
    """Return recent Claude action-item exports from rows or a database handle."""
    if days <= 0 or (limit is not None and limit <= 0):
        return []
    _validate_confidence(min_confidence)
    now = now or datetime.now(timezone.utc)
    rows = (
        list(db_or_rows)
        if isinstance(db_or_rows, (list, tuple))
        else _recent_claude_message_rows(db_or_rows, days=days, now=now)
    )
    candidates = extract_claude_action_items_from_rows(
        rows,
        min_confidence=min_confidence,
    )
    return candidates[:limit] if limit is not None else candidates


def format_claude_action_item_exports_json(
    exports: list[ClaudeActionItemExport],
) -> str:
    return json.dumps([export.to_dict() for export in exports], indent=2, sort_keys=True)


def export_claude_action_items_json(
    text: str,
    *,
    session_metadata: dict[str, Any] | None = None,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    status: str = "open",
    created_at: str | None = None,
    resolved_at: str | None = None,
) -> str:
    """Extract action items from text and export a schema-validated JSON payload."""
    exports = extract_claude_action_items_from_text(
        text,
        session_metadata=session_metadata,
        min_confidence=min_confidence,
    )
    payload = [
        _action_item_json_row(
            export,
            status=status,
            created_at=created_at,
            resolved_at=resolved_at,
        )
        for export in exports
    ]
    _validate_action_item_json_payload(payload)
    return json.dumps(payload, indent=2, sort_keys=True)


def format_claude_action_item_exports_text(
    exports: list[ClaudeActionItemExport],
) -> str:
    lines = [f"action_items={len(exports)}"]
    lines.append(f"{'Conf':>4s}  {'Type':13s}  {'Session':16s}  Action / angle")
    lines.append(f"{'-' * 4:>4s}  {'-' * 13:13s}  {'-' * 16:16s}  {'-' * 60}")
    if not exports:
        lines.append("   -  none           -                 no Claude action items")
        return "\n".join(lines)
    for export in exports:
        lines.append(
            f"{export.confidence:4.2f}  {export.signal_type[:13]:13s}  "
            f"{_shorten(export.session_id, 16):16s}  "
            f"{_shorten(export.action_item, 70)} -> "
            f"{_shorten(export.suggested_content_angle, 80)}"
        )
    return "\n".join(lines)


def _extract_signals(text: str) -> list[_Signal]:
    if not text or not text.strip():
        return []
    raw_lines = [_clean_text(line.strip(" \t-•*[]")) for line in text.splitlines()]
    lines = [line for line in raw_lines if line]
    signals: list[_Signal] = []
    for index, line in enumerate(lines):
        if _is_question_only(line) or _looks_completed(line):
            continue
        signal = _signal_from_line(line, lines, index)
        if signal is not None:
            signals.append(signal)
    return signals


def _signal_from_line(line: str, lines: list[str], index: int) -> _Signal | None:
    lowered = line.lower()
    signal_type = ""
    confidence = 0.0
    reasons: list[str] = []

    if re.search(r"^(todo|to do|follow[- ]?up|action item|next step|next|later)\s*[:\-]", lowered):
        signal_type = "action_item"
        confidence = 0.8
        reasons.append("explicit action marker")
    elif re.search(r"\b(need to|needs to|we should|should add|should update|should keep|should make|should run|should verify|should document|must|let's)\b", lowered):
        signal_type = "implementation_follow_up"
        confidence = 0.72
        reasons.append("committed follow-up language")
    elif re.search(r"^(decision|decided)\s*[:\-]", lowered) or re.search(r"\b(we decided to|decision is to)\b", lowered):
        signal_type = "decision"
        confidence = 0.76
        reasons.append("decision marker")
    else:
        return None

    if re.search(r"\b(add|build|create|implement|update|fix|refactor|test|verify|run|document|export|wire|persist|collapse|dedupe|ship|commit)\b", lowered):
        confidence += 0.1
        reasons.append("implementation verb")
    if re.search(r"\b(cli|exporter|test|tests|api|schema|database|session|content|idea|post|thread|newsletter|release|migration)\b", lowered):
        confidence += 0.07
        reasons.append("content or implementation context")
    if re.search(r"\b(done|finished|completed|already|shipped|merged|fixed)\b", lowered):
        confidence -= 0.25
        reasons.append("completed language")
    if re.search(r"\b(maybe|possibly|could|whether|not sure|unclear|open question|figure out|investigate whether)\b", lowered):
        confidence -= 0.22
        reasons.append("uncertainty language")
    if line.endswith("?"):
        confidence -= 0.35
        reasons.append("question form")

    confidence = round(max(0.0, min(confidence, 0.98)), 2)
    if confidence <= 0:
        return None
    action_item = _clean_action_item(line)
    if not action_item or _is_question_only(action_item):
        return None
    return _Signal(
        signal_type=signal_type,
        action_item=action_item,
        excerpt=_shorten(_snippet(lines, index)),
        confidence=confidence,
        reason=", ".join(reasons),
    )


def _candidate_from_signal(
    signal: _Signal,
    metadata: dict[str, Any],
) -> ClaudeActionItemExport:
    session_id = str(metadata.get("session_id") or metadata.get("sessionId") or "plain-transcript")
    action_item = _clean_text(signal.action_item)
    action_item_id = _action_item_id(session_id, action_item)
    message_id = _int_or_none(metadata.get("id") or metadata.get("message_id"))
    message_uuid = _optional_text(metadata.get("message_uuid") or metadata.get("uuid"))
    project_path = _optional_text(metadata.get("project_path") or metadata.get("cwd") or metadata.get("project"))
    session_path = _optional_text(
        metadata.get("session_path") or metadata.get("path") or metadata.get("artifact_path")
    )
    timestamp = _optional_text(metadata.get("timestamp"))
    suggested_content_angle = _suggest_angle(signal.signal_type, action_item)
    source_metadata = {
        "source": SOURCE_NAME,
        "action_item_id": action_item_id,
        "action_item": action_item,
        "excerpt": signal.excerpt,
        "confidence": signal.confidence,
        "suggested_content_angle": suggested_content_angle,
        "signal_type": signal.signal_type,
        "reason": signal.reason,
        "session_id": session_id,
        "session_path": session_path,
        "project_path": project_path,
        "message_id": message_id,
        "message_uuid": message_uuid,
        "timestamp": timestamp,
    }
    return ClaudeActionItemExport(
        action_item_id=action_item_id,
        action_item=action_item,
        excerpt=signal.excerpt,
        confidence=signal.confidence,
        suggested_content_angle=suggested_content_angle,
        session_id=session_id,
        session_path=session_path,
        project_path=project_path,
        message_id=message_id,
        message_uuid=message_uuid,
        timestamp=timestamp,
        signal_type=signal.signal_type,
        reason=signal.reason,
        source_metadata={key: value for key, value in source_metadata.items() if value is not None},
    )


def _recent_claude_message_rows(db: Any, *, days: int, now: datetime) -> list[dict[str, Any]]:
    cutoff = (now - timedelta(days=days)).isoformat()
    if hasattr(db, "conn"):
        rows = db.conn.execute(
            """SELECT * FROM claude_messages
               WHERE timestamp >= ?
               ORDER BY timestamp ASC, id ASC""",
            (cutoff,),
        ).fetchall()
        return [dict(row) for row in rows]
    getter = getattr(db, "get_messages_in_range", None)
    if callable(getter):
        return [dict(row) for row in getter(now - timedelta(days=days), now + timedelta(seconds=1))]
    return []


def _row_text(row: dict[str, Any]) -> str:
    for key in ("transcript", "prompt_text", "content", "text", "message", "body"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value
        if isinstance(value, dict):
            content = value.get("content")
            if isinstance(content, str) and content.strip():
                return content
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
            "session_path",
            "path",
            "artifact_path",
            "project_path",
            "project",
            "cwd",
            "timestamp",
        )
        if key in row
    }


def _dedupe_candidates(
    candidates: Iterable[ClaudeActionItemExport],
) -> list[ClaudeActionItemExport]:
    best: dict[str, ClaudeActionItemExport] = {}
    for candidate in candidates:
        existing = best.get(candidate.action_item_id)
        if existing is None or candidate.confidence > existing.confidence:
            best[candidate.action_item_id] = candidate
    return sorted(
        best.values(),
        key=lambda candidate: (
            -candidate.confidence,
            str(candidate.timestamp or ""),
            candidate.action_item_id,
        ),
    )


def _suggest_angle(signal_type: str, action_item: str) -> str:
    compact = _shorten(action_item, 120).rstrip(".")
    if signal_type == "decision":
        return f"Explain the decision behind {compact} and the tradeoff it resolved."
    if re.search(r"\b(test|verify|run)\b", action_item.lower()):
        return f"Turn {compact} into a practical quality-gate or verification lesson."
    if re.search(r"\b(export|content|post|thread|newsletter|idea)\b", action_item.lower()):
        return f"Use {compact} as a behind-the-scenes content workflow lesson."
    return f"Show the implementation follow-up behind {compact} and why it mattered."


def _action_item_json_row(
    export: ClaudeActionItemExport,
    *,
    status: str,
    created_at: str | None,
    resolved_at: str | None,
) -> dict[str, Any]:
    created = created_at or export.timestamp
    resolved = resolved_at if status == "resolved" else None
    return {
        "action_id": export.action_item_id,
        "prompt_text": export.action_item,
        "confidence_score": export.confidence,
        "status": status,
        "created_at": created,
        "resolved_at": resolved,
    }


def _validate_action_item_json_payload(payload: list[dict[str, Any]]) -> None:
    required = {"action_id", "prompt_text", "confidence_score", "status", "created_at", "resolved_at"}
    valid_statuses = {"open", "resolved", "dismissed"}
    for item in payload:
        missing = required - set(item)
        if missing:
            raise ValueError(f"action item JSON missing required fields: {sorted(missing)}")
        if not isinstance(item["action_id"], str) or not item["action_id"]:
            raise ValueError("action_id must be a non-empty string")
        if not isinstance(item["prompt_text"], str) or not item["prompt_text"]:
            raise ValueError("prompt_text must be a non-empty string")
        if not isinstance(item["confidence_score"], (int, float)) or isinstance(item["confidence_score"], bool):
            raise ValueError("confidence_score must be numeric")
        if not 0 <= float(item["confidence_score"]) <= 1:
            raise ValueError("confidence_score must be between 0 and 1")
        if item["status"] not in valid_statuses:
            raise ValueError("status must be one of open, resolved, dismissed")
        if item["created_at"] is not None and not isinstance(item["created_at"], str):
            raise ValueError("created_at must be a string or null")
        if item["resolved_at"] is not None and not isinstance(item["resolved_at"], str):
            raise ValueError("resolved_at must be a string or null")


def _clean_action_item(line: str) -> str:
    value = _clean_text(line)
    value = re.sub(
        r"^(todo|to do|follow[- ]?up|action item|next step|next|later|decision|decided)\s*[:\-]\s*",
        "",
        value,
        flags=re.I,
    )
    value = re.sub(r"^(we decided to|decision is to|let's)\s+", "", value, flags=re.I)
    return _clean_text(value)


def _looks_completed(line: str) -> bool:
    return bool(
        re.search(
            r"^(done|completed|finished|fixed|shipped|merged|resolved)\b|"
            r"\b(no action needed|nothing left|already done)\b",
            line.lower(),
        )
    )


def _is_question_only(line: str) -> bool:
    compact = _clean_text(line)
    if not compact.endswith("?"):
        return False
    return not re.search(
        r"\b(todo|follow[- ]?up|action item|next step|need to|we should|must|decision)\b",
        compact.lower(),
    )


def _snippet(lines: list[str], index: int) -> str:
    start = max(0, index - 1)
    end = min(len(lines), index + 2)
    return _clean_text(" ".join(line for line in lines[start:end] if line))


def _shorten(text: str | None, width: int = EXCERPT_CHARS) -> str:
    value = _clean_text(text or "")
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _normalize_action_item(action_item: str) -> str:
    value = _clean_action_item(action_item).lower()
    value = re.sub(r"`{1,3}", "", value)
    value = re.sub(r"https?://\S+", "<url>", value)
    value = re.sub(r"(/[^\s:]+)+", "<path>", value)
    value = re.sub(r"\b[a-f0-9]{7,40}\b", "<hash>", value)
    value = re.sub(r"\b\d+\b", "<num>", value)
    value = re.sub(r"[^a-z0-9<> ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _action_item_id(session_id: str, action_item: str) -> str:
    identity = f"{session_id}|{_normalize_action_item(action_item)}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"claude_action_{digest}"


def _validate_confidence(value: float) -> None:
    if not 0 <= value <= 1:
        raise ValueError("min_confidence must be between 0 and 1")


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
