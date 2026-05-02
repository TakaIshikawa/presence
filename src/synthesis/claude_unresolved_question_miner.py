"""Mine unresolved Claude Code transcript questions into content ideas."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable


SOURCE_NAME = "claude_unresolved_question_miner"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 10
DEFAULT_MIN_CONFIDENCE = 0.62
EXCERPT_CHARS = 320


@dataclass(frozen=True)
class ClaudeUnresolvedQuestionCandidate:
    question_fingerprint: str
    question: str
    snippet: str
    reason: str
    confidence: float
    session_id: str
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
class ClaudeUnresolvedQuestionSeedResult:
    status: str
    question_fingerprint: str
    question: str
    confidence: float
    reason: str
    idea_id: int | None
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def extract_unresolved_questions_from_text(
    text: str,
    *,
    session_metadata: dict[str, Any] | None = None,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> list[ClaudeUnresolvedQuestionCandidate]:
    """Extract unresolved question candidates from one transcript-like text blob."""
    metadata = dict(session_metadata or {})
    candidates = [
        _candidate_from_signal(signal, metadata)
        for signal in _extract_signals(text)
    ]
    candidates = [
        candidate
        for candidate in candidates
        if candidate.confidence >= min_confidence
    ]
    return _dedupe_candidates(candidates)


def extract_unresolved_questions_from_rows(
    rows: Iterable[dict[str, Any]],
    *,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> list[ClaudeUnresolvedQuestionCandidate]:
    """Extract unresolved question candidates from stored Claude log/message rows."""
    candidates: list[ClaudeUnresolvedQuestionCandidate] = []
    for row in rows:
        text = _row_text(row)
        if not text:
            continue
        candidates.extend(
            extract_unresolved_questions_from_text(
                text,
                session_metadata=_row_metadata(row),
                min_confidence=min_confidence,
            )
        )
    candidates.sort(
        key=lambda candidate: (
            -candidate.confidence,
            str(candidate.timestamp or ""),
            candidate.question_fingerprint,
        )
    )
    return _dedupe_candidates(candidates)


def build_claude_unresolved_question_candidates(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    now: datetime | None = None,
) -> list[ClaudeUnresolvedQuestionCandidate]:
    """Return recent unresolved-question candidates from rows or a database handle."""
    if days <= 0 or (limit is not None and limit <= 0):
        return []
    if not 0 <= min_confidence <= 1:
        raise ValueError("min_confidence must be between 0 and 1")
    now = now or datetime.now(timezone.utc)
    rows = (
        list(db_or_rows)
        if isinstance(db_or_rows, (list, tuple))
        else _recent_claude_message_rows(db_or_rows, days=days, now=now)
    )
    candidates = extract_unresolved_questions_from_rows(
        rows,
        min_confidence=min_confidence,
    )
    return candidates[:limit] if limit is not None else candidates


def mine_claude_unresolved_questions(
    db,
    *,
    days: int = DEFAULT_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    dry_run: bool = False,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    now: datetime | None = None,
) -> list[ClaudeUnresolvedQuestionSeedResult]:
    """Create or preview content ideas from unresolved Claude Code questions."""
    candidates = build_claude_unresolved_question_candidates(
        db,
        days=days,
        limit=limit,
        min_confidence=min_confidence,
        now=now,
    )
    add_idea = getattr(db, "add_content_idea", None) or getattr(db, "insert_content_idea", None)
    find_existing = getattr(db, "find_active_content_idea_for_source_metadata", None)
    if not callable(add_idea):
        return []

    results: list[ClaudeUnresolvedQuestionSeedResult] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.question_fingerprint in seen:
            continue
        seen.add(candidate.question_fingerprint)

        existing = _find_existing_question_idea(db, candidate.question_fingerprint)
        if callable(find_existing):
            existing = existing or find_existing(
                source=SOURCE_NAME,
                source_metadata={"question_fingerprint_id": candidate.question_fingerprint},
            )
        if existing:
            results.append(_result(candidate, "skipped", int(existing["id"]), f"{existing['status']} duplicate"))
            continue

        if dry_run:
            results.append(_result(candidate, "proposed", None, "dry run"))
            continue

        idea_id = add_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority=candidate.priority,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(_result(candidate, "created", int(idea_id), "created"))

    return results


def format_claude_unresolved_question_results_json(
    results: list[ClaudeUnresolvedQuestionSeedResult],
) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def format_claude_unresolved_question_results_text(
    results: list[ClaudeUnresolvedQuestionSeedResult],
) -> str:
    created = sum(1 for result in results if result.status == "created")
    proposed = sum(1 for result in results if result.status == "proposed")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} proposed={proposed} skipped={skipped}"]
    lines.append(f"{'Status':9s}  {'ID':>4s}  {'Conf':>4s}  Question / reason")
    lines.append(f"{'-' * 9:9s}  {'-' * 4:>4s}  {'-' * 4:>4s}  {'-' * 60}")
    if not results:
        lines.append("none       -     -     no unresolved question candidates")
        return "\n".join(lines)
    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        lines.append(
            f"{result.status:9s}  {idea_id:>4s}  {result.confidence:4.2f}  "
            f"{_shorten(result.question, 72)}: {result.reason}"
        )
    return "\n".join(lines)


@dataclass(frozen=True)
class _Signal:
    question: str
    snippet: str
    reason: str
    confidence: float


def _extract_signals(text: str) -> list[_Signal]:
    if not text or not text.strip():
        return []
    lines = [line.strip(" \t-•") for line in text.splitlines()]
    signals: list[_Signal] = []
    for index, line in enumerate(lines):
        if not line:
            continue
        if _looks_resolved(line):
            continue
        if _contains_question(line):
            signals.append(_question_signal(line, lines, index))
            continue
        if _contains_todo_uncertainty(line):
            signals.append(_todo_signal(line, lines, index))
    return [signal for signal in signals if signal.confidence > 0]


def _question_signal(line: str, lines: list[str], index: int) -> _Signal:
    question = _extract_question_text(line)
    snippet = _snippet(lines, index)
    confidence = 0.58
    reasons: list[str] = ["question mark"]
    lowered = line.lower()
    if _contains_uncertainty(line):
        confidence += 0.18
        reasons.append("uncertainty language")
    if re.search(r"\b(open question|unresolved|follow[- ]?up|todo|need to decide|not sure|unclear)\b", lowered):
        confidence += 0.18
        reasons.append("explicit unresolved marker")
    if re.search(r"\b(implementation|design|architecture|migration|test|api|schema|deploy|release)\b", lowered):
        confidence += 0.08
        reasons.append("implementation context")
    if _looks_rhetorical(question):
        confidence -= 0.42
        reasons.append("rhetorical/low-signal phrasing")
    if _looks_resolved(snippet):
        confidence -= 0.35
        reasons.append("nearby resolved language")
    return _Signal(
        question=question,
        snippet=_shorten(snippet),
        reason=", ".join(reasons),
        confidence=round(max(0.0, min(confidence, 0.98)), 2),
    )


def _todo_signal(line: str, lines: list[str], index: int) -> _Signal:
    snippet = _snippet(lines, index)
    question = _statement_to_question(line)
    confidence = 0.7
    reasons = ["TODO/follow-up uncertainty"]
    lowered = line.lower()
    if re.search(r"\b(open question|unresolved|follow[- ]?up|need to decide)\b", lowered):
        confidence += 0.14
        reasons.append("explicit unresolved marker")
    if _looks_resolved(snippet):
        confidence -= 0.35
        reasons.append("nearby resolved language")
    return _Signal(
        question=question,
        snippet=_shorten(snippet),
        reason=", ".join(reasons),
        confidence=round(max(0.0, min(confidence, 0.95)), 2),
    )


def _candidate_from_signal(
    signal: _Signal,
    metadata: dict[str, Any],
) -> ClaudeUnresolvedQuestionCandidate:
    session_id = str(metadata.get("session_id") or metadata.get("sessionId") or "plain-transcript")
    question = _clean_text(signal.question)
    fingerprint = _question_fingerprint(session_id, question)
    message_id = _int_or_none(metadata.get("id") or metadata.get("message_id"))
    message_uuid = _optional_text(metadata.get("message_uuid") or metadata.get("uuid"))
    project_path = _optional_text(metadata.get("project_path") or metadata.get("cwd") or metadata.get("project"))
    timestamp = _optional_text(metadata.get("timestamp"))
    topic = "Claude Code unresolved implementation questions"
    note = (
        f"Unresolved Claude Code question from session {session_id}: {question} "
        f"Reason: {signal.reason}. Confidence: {signal.confidence:.2f}. "
        f"Snippet: {signal.snippet} "
        "Suggested angle: turn the open implementation uncertainty into a practical "
        "post, thread, or newsletter section about how to frame and resolve the tradeoff."
    )
    priority = "high" if signal.confidence >= 0.82 else "normal"
    source_metadata = {
        "source": SOURCE_NAME,
        "question_fingerprint": fingerprint,
        "question_fingerprint_id": fingerprint,
        "question": question,
        "snippet": signal.snippet,
        "reason": signal.reason,
        "confidence": signal.confidence,
        "session_id": session_id,
        "message_id": message_id,
        "message_uuid": message_uuid,
        "project_path": project_path,
        "timestamp": timestamp,
    }
    return ClaudeUnresolvedQuestionCandidate(
        question_fingerprint=fingerprint,
        question=question,
        snippet=signal.snippet,
        reason=signal.reason,
        confidence=signal.confidence,
        session_id=session_id,
        message_id=message_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=timestamp,
        topic=topic,
        note=note,
        priority=priority,
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


def _find_existing_question_idea(db: Any, question_fingerprint: str) -> dict[str, Any] | None:
    if hasattr(db, "conn"):
        rows = db.conn.execute(
            """SELECT * FROM content_ideas
               WHERE status IN ('open', 'promoted')
                 AND source = ?
                 AND source_metadata IS NOT NULL
               ORDER BY created_at ASC, id ASC""",
            (SOURCE_NAME,),
        ).fetchall()
        for row in rows:
            item = dict(row)
            metadata = _decode_metadata(item.get("source_metadata"))
            if metadata.get("question_fingerprint") == question_fingerprint:
                return item
    getter = getattr(db, "get_content_ideas", None)
    if callable(getter):
        for item in getter(status="open", limit=1000, include_snoozed=True):
            if item.get("source") != SOURCE_NAME:
                continue
            metadata = _decode_metadata(item.get("source_metadata"))
            if metadata.get("question_fingerprint") == question_fingerprint:
                return item
    return None


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
    candidate: ClaudeUnresolvedQuestionCandidate,
    status: str,
    idea_id: int | None,
    reason: str,
) -> ClaudeUnresolvedQuestionSeedResult:
    return ClaudeUnresolvedQuestionSeedResult(
        status=status,
        question_fingerprint=candidate.question_fingerprint,
        question=candidate.question,
        confidence=candidate.confidence,
        reason=reason,
        idea_id=idea_id,
        topic=candidate.topic,
        note=candidate.note,
        priority=candidate.priority,
        source_metadata=candidate.source_metadata,
    )


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
            "project_path",
            "project",
            "cwd",
            "timestamp",
        )
        if key in row
    }


def _dedupe_candidates(
    candidates: Iterable[ClaudeUnresolvedQuestionCandidate],
) -> list[ClaudeUnresolvedQuestionCandidate]:
    best: dict[str, ClaudeUnresolvedQuestionCandidate] = {}
    for candidate in candidates:
        existing = best.get(candidate.question_fingerprint)
        if existing is None or candidate.confidence > existing.confidence:
            best[candidate.question_fingerprint] = candidate
    return sorted(
        best.values(),
        key=lambda candidate: (
            -candidate.confidence,
            str(candidate.timestamp or ""),
            candidate.question_fingerprint,
        ),
    )


def _contains_question(line: str) -> bool:
    return "?" in line


def _contains_uncertainty(text: str) -> bool:
    return bool(
        re.search(
            r"\b(not sure|unclear|unknown|open question|unresolved|need to decide|"
            r"should we|could we|do we|can we|follow[- ]?up|todo|figure out)\b",
            text.lower(),
        )
    )


def _contains_todo_uncertainty(line: str) -> bool:
    lowered = line.lower()
    return bool(
        re.search(
            r"\b(todo|follow[- ]?up|open question|unresolved|need to decide|"
            r"not sure|unclear|figure out|investigate whether)\b",
            lowered,
        )
    )


def _looks_resolved(text: str) -> bool:
    return bool(
        re.search(
            r"\b(resolved|answered|fixed|done|closed|we decided|decision:|answer:|"
            r"turns out|no longer needed|not needed)\b",
            text.lower(),
        )
    )


def _looks_rhetorical(question: str) -> bool:
    normalized = _normalize_question(question)
    if len(normalized.split()) <= 3:
        return True
    rhetorical_patterns = (
        r"^(right|ok|okay|clear|makes sense|sound good)\??$",
        r"\b(who knows|what could go wrong|why not|how hard could it be)\b",
        r"\b(isn't it|aren't we|does that make sense)\??$",
    )
    return any(re.search(pattern, normalized) for pattern in rhetorical_patterns)


def _extract_question_text(line: str) -> str:
    stripped = _clean_text(line)
    match = re.search(r"([^.!?\n]{4,}\?)", stripped)
    if match:
        return _clean_text(match.group(1))
    return stripped


def _statement_to_question(line: str) -> str:
    stripped = _clean_text(re.sub(r"^(todo|follow[- ]?up|open question)\s*[:\-]\s*", "", line, flags=re.I))
    if stripped.endswith("?"):
        return stripped
    return f"What should we do about {stripped[0].lower() + stripped[1:] if stripped else 'this uncertainty'}?"


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


def _normalize_question(question: str) -> str:
    value = _clean_text(question).lower()
    value = re.sub(r"`{1,3}", "", value)
    value = re.sub(r"https?://\S+", "<url>", value)
    value = re.sub(r"(/[^\s:]+)+", "<path>", value)
    value = re.sub(r"\b[a-f0-9]{7,40}\b", "<hash>", value)
    value = re.sub(r"\b\d+\b", "<num>", value)
    value = re.sub(r"[^a-z0-9?<> ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _question_fingerprint(session_id: str, question: str) -> str:
    identity = f"{session_id}|{_normalize_question(question)}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"claude_unresolved_{digest}"


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
