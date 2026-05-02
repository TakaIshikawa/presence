"""Seed content ideas from unresolved Claude Code session blockers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Iterable


SOURCE_NAME = "claude_blocker_idea_seed"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 10
EXCERPT_CHARS = 280


@dataclass(frozen=True)
class ClaudeBlockerIdeaCandidate:
    source_key: str
    session_id: str
    blocker_excerpt: str
    confidence: float
    suggested_angle: str
    signal_type: str
    message_id: int | None
    message_uuid: str | None
    project_path: str | None
    timestamp: str | None
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeBlockerSeedResult:
    status: str
    source_key: str
    session_id: str
    blocker_excerpt: str
    confidence: float
    suggested_angle: str
    idea_id: int | None
    reason: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _Signal:
    signal_type: str
    excerpt: str
    confidence: float
    reasons: tuple[str, ...]


def extract_claude_blocker_candidates_from_rows(
    rows: Iterable[dict[str, Any]],
    *,
    limit: int | None = DEFAULT_LIMIT,
) -> list[ClaudeBlockerIdeaCandidate]:
    """Extract deterministic blocker candidates from Claude message-like rows."""
    if limit is not None and limit <= 0:
        return []
    candidates: list[ClaudeBlockerIdeaCandidate] = []
    for row in rows:
        text = _row_text(row)
        if not text:
            continue
        metadata = _row_metadata(row)
        for signal in _extract_blocker_signals(text):
            candidates.append(_candidate_from_signal(signal, metadata))

    deduped = _dedupe_candidates(candidates)
    return deduped[:limit] if limit is not None else deduped


def build_claude_blocker_idea_candidates(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> list[ClaudeBlockerIdeaCandidate]:
    """Return recent unresolved Claude blocker candidates from rows or a database."""
    if days <= 0 or (limit is not None and limit <= 0):
        return []
    now = now or datetime.now(timezone.utc)
    rows = (
        list(db_or_rows)
        if isinstance(db_or_rows, (list, tuple))
        else _recent_claude_message_rows(db_or_rows, days=days, now=now)
    )
    return extract_claude_blocker_candidates_from_rows(rows, limit=limit)


def seed_claude_blocker_ideas(
    db,
    *,
    days: int = DEFAULT_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[ClaudeBlockerSeedResult]:
    """Create or preview content ideas from unresolved Claude blockers."""
    candidates = build_claude_blocker_idea_candidates(db, days=days, limit=limit, now=now)
    if not dry_run and not _has_content_idea_table(db):
        return [_result(candidate, "skipped", None, "missing content_ideas table") for candidate in candidates]

    results: list[ClaudeBlockerSeedResult] = []
    for candidate in candidates:
        existing = _find_existing_blocker_idea(db, candidate.source_key)
        if existing:
            results.append(_result(candidate, "skipped", int(existing["id"]), f"{existing['status']} duplicate"))
            continue
        if dry_run:
            results.append(_result(candidate, "proposed", None, "dry run"))
            continue
        idea_id = _insert_content_idea(db, candidate)
        results.append(_result(candidate, "created", int(idea_id), "created"))
    return results


def format_claude_blocker_ideas_json(results: list[ClaudeBlockerSeedResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def format_claude_blocker_ideas_text(results: list[ClaudeBlockerSeedResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    proposed = sum(1 for result in results if result.status == "proposed")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} proposed={proposed} skipped={skipped}"]
    lines.append(f"{'Status':9s}  {'ID':>4s}  {'Conf':>4s}  {'Session':16s}  Blocker / angle")
    lines.append(f"{'-' * 9:9s}  {'-' * 4:>4s}  {'-' * 4:>4s}  {'-' * 16:16s}  {'-' * 60}")
    if not results:
        lines.append("none       -     -     -                 no unresolved blocker candidates")
        return "\n".join(lines)
    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        lines.append(
            f"{result.status:9s}  {idea_id:>4s}  {result.confidence:4.2f}  "
            f"{_shorten(result.session_id, 16):16s}  "
            f"{_shorten(result.blocker_excerpt, 72)} | {_shorten(result.suggested_angle, 72)}"
        )
    return "\n".join(lines)


def _extract_blocker_signals(text: str) -> list[_Signal]:
    lines = [line.strip(" \t-*•") for line in text.splitlines()]
    signals: list[_Signal] = []
    for index, line in enumerate(lines):
        if not line:
            continue
        signal = _line_signal(line, lines, index)
        if signal is not None:
            signals.append(signal)
    return signals


def _line_signal(line: str, lines: list[str], index: int) -> _Signal | None:
    lowered = line.lower()
    if _looks_resolved(line):
        return None

    patterns: list[tuple[str, str, float]] = [
        ("blocked", r"\b(blocked|blocker|blocking|stuck)\b", 0.78),
        ("failing", r"\b(failing|still fails|keeps failing|test(?:s)? fail|build fails|failed)\b", 0.7),
        ("cannot_reproduce", r"\b(cannot reproduce|can't reproduce|could not reproduce|unable to reproduce)\b", 0.84),
        ("waiting_on", r"\b(waiting on|waiting for|blocked on)\b", 0.76),
        ("needs_decision", r"\b(needs? decision|need to decide|pending decision|awaiting decision)\b", 0.82),
        ("todo_unresolved", r"\b(TODO|FIXME|follow[- ]?up|unresolved|open item|needs follow[- ]?up)\b", 0.72),
    ]
    for signal_type, pattern, base_confidence in patterns:
        if not re.search(pattern, line, flags=re.IGNORECASE):
            continue
        snippet = _snippet(lines, index)
        confidence = base_confidence
        reasons = [signal_type.replace("_", " ")]
        if re.search(r"\b(unresolved|still|cannot|can't|blocked|waiting|decision|TODO|FIXME)\b", line, re.IGNORECASE):
            confidence += 0.08
            reasons.append("explicit unresolved marker")
        if re.search(r"\b(test|build|deploy|schema|api|migration|release|repro|review|product|design)\b", line, re.IGNORECASE):
            confidence += 0.06
            reasons.append("engineering context")
        if _looks_resolved(snippet):
            confidence -= 0.3
            reasons.append("nearby resolved language")
        confidence = round(max(0.0, min(confidence, 0.98)), 2)
        if confidence < 0.6:
            return None
        return _Signal(signal_type, _shorten(snippet), confidence, tuple(reasons))
    return None


def _candidate_from_signal(signal: _Signal, metadata: dict[str, Any]) -> ClaudeBlockerIdeaCandidate:
    session_id = str(metadata.get("session_id") or metadata.get("sessionId") or "plain-transcript")
    message_id = _int_or_none(metadata.get("id") or metadata.get("message_id"))
    message_uuid = _optional_text(metadata.get("message_uuid") or metadata.get("uuid"))
    project_path = _optional_text(metadata.get("project_path") or metadata.get("cwd") or metadata.get("project"))
    timestamp = _optional_text(metadata.get("timestamp"))
    source_key = _source_key(session_id, signal.excerpt)
    suggested_angle = _suggested_angle(signal.signal_type, signal.excerpt)
    topic = f"Claude Code blocker follow-up: {signal.signal_type.replace('_', ' ')}"
    note = (
        f"Unresolved Claude Code blocker from session {session_id}: {signal.excerpt} "
        f"Confidence: {signal.confidence:.2f}. Suggested angle: {suggested_angle} "
        "Use the blocker as source material for a concrete follow-up post, thread, or newsletter section."
    )
    priority = "high" if signal.confidence >= 0.86 else "normal"
    source_metadata = {
        "source": SOURCE_NAME,
        "source_key": source_key,
        "session_id": session_id,
        "blocker_excerpt": signal.excerpt,
        "confidence": signal.confidence,
        "suggested_angle": suggested_angle,
        "signal_type": signal.signal_type,
        "message_id": message_id,
        "message_uuid": message_uuid,
        "project_path": project_path,
        "timestamp": timestamp,
        "reasons": list(signal.reasons),
    }
    compact_metadata = {key: value for key, value in source_metadata.items() if value is not None}
    return ClaudeBlockerIdeaCandidate(
        source_key=source_key,
        session_id=session_id,
        blocker_excerpt=signal.excerpt,
        confidence=signal.confidence,
        suggested_angle=suggested_angle,
        signal_type=signal.signal_type,
        message_id=message_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=timestamp,
        topic=topic,
        note=note,
        priority=priority,
        source_metadata=compact_metadata,
    )


def _suggested_angle(signal_type: str, excerpt: str) -> str:
    angles = {
        "blocked": "Explain the dependency or decision that made the work stall and the smallest next diagnostic step.",
        "failing": "Turn the failing loop into a debugging checklist with the signal that finally mattered.",
        "cannot_reproduce": "Show how to capture reproduction context before changing code.",
        "waiting_on": "Write about managing dependency waits without losing engineering context.",
        "needs_decision": "Frame the decision, tradeoffs, and what evidence would make the call easier.",
        "todo_unresolved": "Convert the TODO into a concrete follow-up plan and why it was deferred.",
    }
    return angles.get(signal_type, f"Use this unresolved blocker as a practical engineering follow-up: {_shorten(excerpt, 120)}")


def _recent_claude_message_rows(db: Any, *, days: int, now: datetime) -> list[dict[str, Any]]:
    cutoff = (now - timedelta(days=days)).isoformat()
    conn = _conn(db)
    if conn is not None and _has_table(conn, "claude_messages"):
        rows = conn.execute(
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


def _find_existing_blocker_idea(db: Any, source_key: str) -> dict[str, Any] | None:
    finder = getattr(db, "find_active_content_idea_for_source_metadata", None)
    if callable(finder):
        existing = finder(source=SOURCE_NAME, source_metadata={"source_key": source_key})
        if existing:
            return dict(existing)
    conn = _conn(db)
    if conn is None or not _has_table(conn, "content_ideas"):
        return None
    rows = conn.execute(
        """SELECT * FROM content_ideas
           WHERE status IN ('open', 'promoted')
             AND source = ?
             AND source_metadata IS NOT NULL
           ORDER BY created_at ASC, id ASC""",
        (SOURCE_NAME,),
    ).fetchall()
    for row in rows:
        item = dict(row)
        if _decode_metadata(item.get("source_metadata")).get("source_key") == source_key:
            return item
    return None


def _insert_content_idea(db: Any, candidate: ClaudeBlockerIdeaCandidate) -> int:
    add_idea = getattr(db, "add_content_idea", None) or getattr(db, "insert_content_idea", None)
    if callable(add_idea):
        return int(
            add_idea(
                note=candidate.note,
                topic=candidate.topic,
                priority=candidate.priority,
                source=SOURCE_NAME,
                source_metadata=candidate.source_metadata,
            )
        )
    conn = _conn(db)
    if conn is None or not _has_table(conn, "content_ideas"):
        raise ValueError("content_ideas table is required for insert mode")
    cursor = conn.execute(
        """INSERT INTO content_ideas
           (note, topic, priority, status, source, source_metadata)
           VALUES (?, ?, ?, 'open', ?, ?)""",
        (
            candidate.note,
            candidate.topic,
            candidate.priority,
            SOURCE_NAME,
            json.dumps(candidate.source_metadata, sort_keys=True),
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _has_content_idea_table(db: Any) -> bool:
    if callable(getattr(db, "add_content_idea", None)) or callable(getattr(db, "insert_content_idea", None)):
        return True
    conn = _conn(db)
    return bool(conn is not None and _has_table(conn, "content_ideas"))


def _has_table(db_or_conn: Any, table: str) -> bool:
    conn = _conn(db_or_conn) or db_or_conn
    try:
        row = conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone()
    except Exception:
        return False
    return row is not None


def _conn(db: Any) -> Any:
    return getattr(db, "conn", None) or (db if hasattr(db, "execute") else None)


def _row_text(row: dict[str, Any]) -> str:
    parts = []
    for key in ("transcript", "prompt_text", "response_text", "content", "text", "message", "body"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value)
        elif isinstance(value, dict):
            content = value.get("content")
            if isinstance(content, str) and content.strip():
                parts.append(content)
    return "\n".join(parts)


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


def _dedupe_candidates(candidates: Iterable[ClaudeBlockerIdeaCandidate]) -> list[ClaudeBlockerIdeaCandidate]:
    best: dict[str, ClaudeBlockerIdeaCandidate] = {}
    for candidate in candidates:
        existing = best.get(candidate.source_key)
        if existing is None or candidate.confidence > existing.confidence:
            best[candidate.source_key] = candidate
    return sorted(
        best.values(),
        key=lambda candidate: (-candidate.confidence, str(candidate.timestamp or ""), candidate.source_key),
    )


def _source_key(session_id: str, excerpt: str) -> str:
    normalized = _normalize_text(excerpt)
    digest = hashlib.sha256(f"{session_id}\n{normalized}".encode("utf-8")).hexdigest()[:16]
    return f"claude_blocker_{digest}"


def _normalize_text(text: str) -> str:
    value = text.lower()
    value = re.sub(r"https?://\S+", "<url>", value)
    value = re.sub(r"(/[^\s:]+)+", "<path>", value)
    value = re.sub(r"\b[a-f0-9]{7,40}\b", "<hash>", value)
    value = re.sub(r"\b\d+\b", "<num>", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" -:,.")


def _snippet(lines: list[str], index: int, radius: int = 1) -> str:
    start = max(0, index - radius)
    end = min(len(lines), index + radius + 1)
    return " ".join(line for line in lines[start:end] if line)


def _looks_resolved(text: str) -> bool:
    return bool(
        re.search(
            r"\b(resolved|fixed|done|completed|unblocked|no longer blocked|decision made|not a blocker)\b",
            text,
            flags=re.IGNORECASE,
        )
    )


def _shorten(text: str | None, width: int = EXCERPT_CHARS) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _decode_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _result(
    candidate: ClaudeBlockerIdeaCandidate,
    status: str,
    idea_id: int | None,
    reason: str,
) -> ClaudeBlockerSeedResult:
    return ClaudeBlockerSeedResult(
        status=status,
        source_key=candidate.source_key,
        session_id=candidate.session_id,
        blocker_excerpt=candidate.blocker_excerpt,
        confidence=candidate.confidence,
        suggested_angle=candidate.suggested_angle,
        idea_id=idea_id,
        reason=reason,
        topic=candidate.topic,
        note=candidate.note,
        priority=candidate.priority,
        source_metadata=candidate.source_metadata,
    )
