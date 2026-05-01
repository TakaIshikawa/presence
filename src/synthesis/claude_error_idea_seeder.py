"""Seed content ideas from repeated Claude Code error patterns."""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


SOURCE_NAME = "claude_error_pattern_seed"
DEFAULT_DAYS = 14
DEFAULT_MIN_COUNT = 2
EXCERPT_CHARS = 220


@dataclass(frozen=True)
class ClaudeErrorIdeaCandidate:
    pattern_id: str
    normalized_phrase: str
    failure_type: str
    occurrence_count: int
    message_ids: list[int]
    message_uuids: list[str]
    session_ids: list[str]
    project_paths: list[str]
    first_seen_at: str
    last_seen_at: str
    examples: list[str]
    topic: str
    note: str
    priority: str
    score: float
    score_reasons: list[str]
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeErrorSeedResult:
    status: str
    pattern_id: str
    normalized_phrase: str
    failure_type: str
    occurrence_count: int
    topic: str
    score: float
    idea_id: int | None
    reason: str
    note: str
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _Signal:
    failure_type: str
    phrase: str
    excerpt: str


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _shorten(text: str | None, width: int = EXCERPT_CHARS) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _normalize_phrase(text: str) -> str:
    value = text.lower()
    value = re.sub(r"`{1,3}", " ", value)
    value = re.sub(r"https?://\S+", "<url>", value)
    value = re.sub(r"(/[^\s:]+)+", "<path>", value)
    value = re.sub(r"\b[\w.-]+(?:/[\w.-]+)+\b", "<path>", value)
    value = re.sub(r"\b[a-f0-9]{7,40}\b", "<hash>", value)
    value = re.sub(r"\b[0-9a-f]{8}-[0-9a-f-]{13,}\b", "<uuid>", value)
    value = re.sub(r"\b\d+\b", "<num>", value)
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"\s+([:,.])", r"\1", value)
    return value.strip(" -:,.")


def _pattern_id(normalized_phrase: str) -> str:
    digest = hashlib.sha256(normalized_phrase.encode("utf-8")).hexdigest()[:12]
    return f"claude_error_{digest}"


def _exception_line(lines: list[str]) -> str | None:
    for line in reversed(lines):
        stripped = line.strip()
        if re.search(r"\b[A-Z][A-Za-z]*(Error|Exception)\b\s*:", stripped):
            return stripped
    return None


def _extract_failure_signal(text: str) -> _Signal | None:
    if not text or not text.strip():
        return None

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    compact = " ".join(lines)
    lowered = compact.lower()

    if "traceback (most recent call last)" in lowered:
        phrase = _exception_line(lines) or "Traceback (most recent call last)"
        return _Signal("traceback", _normalize_phrase(phrase), _shorten(compact))

    patterns: list[tuple[str, str]] = [
        ("tool_error", r"\b(tool error|tool call failed|tool_use failed)\b[:\s-]*(.{0,160})"),
        ("failed_command", r"\b(command failed|exit code \d+|exited with code \d+|non-zero exit)\b[:\s-]*(.{0,160})"),
        ("failed_command", r"\b(npm|pnpm|yarn|uv|pytest|python|ruff|mypy|git)\b[^\n.]{0,80}\b(failed|error)\b[^\n.]{0,120}"),
        ("tool_error", r"\berror:\s*([^\n.]{8,180})"),
        ("tool_error", r"\b[A-Z][A-Za-z]*(Error|Exception):\s*([^\n]{0,180})"),
    ]
    for failure_type, pattern in patterns:
        match = re.search(pattern, compact, flags=re.IGNORECASE)
        if match:
            phrase = match.group(0)
            return _Signal(failure_type, _normalize_phrase(phrase), _shorten(compact))

    correction_patterns = [
        r"\b(still failing|same error|same failure|that did not work|that didn't work)\b[^\n.]{0,120}",
        r"\b(you broke|try again|fix it again|keeps failing|keeps erroring)\b[^\n.]{0,120}",
    ]
    for pattern in correction_patterns:
        match = re.search(pattern, compact, flags=re.IGNORECASE)
        if match:
            return _Signal(
                "correction_loop",
                _normalize_phrase(match.group(0)),
                _shorten(compact),
            )

    return None


def _recent_claude_message_rows(
    db,
    *,
    days: int,
    now: datetime,
) -> list[dict[str, Any]]:
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
        return getter(now - timedelta(days=days), now + timedelta(seconds=1))
    return []


def _score_candidate(
    *,
    occurrence_count: int,
    last_seen_at: str,
    now: datetime,
    failure_type: str,
) -> tuple[float, list[str]]:
    score = 30.0 + min(occurrence_count, 8) * 10
    reasons = [f"occurrences={occurrence_count}+{min(occurrence_count, 8) * 10:g}"]
    last_seen = _parse_datetime(last_seen_at)
    if last_seen is not None:
        age_days = max(0.0, (now - last_seen).total_seconds() / 86400)
        if age_days <= 1:
            score += 20
            reasons.append("last_seen<=1d+20")
        elif age_days <= 3:
            score += 14
            reasons.append("last_seen<=3d+14")
        elif age_days <= 7:
            score += 8
            reasons.append("last_seen<=7d+8")
    if failure_type == "traceback":
        score += 8
        reasons.append("traceback+8")
    elif failure_type == "correction_loop":
        score += 6
        reasons.append("correction-loop+6")
    return round(score, 2), reasons


def _candidate_from_group(
    normalized_phrase: str,
    rows: list[dict[str, Any]],
    *,
    now: datetime,
) -> ClaudeErrorIdeaCandidate:
    rows = sorted(rows, key=lambda row: (str(row.get("timestamp") or ""), int(row.get("id") or 0)))
    first = rows[0]
    last = rows[-1]
    signal = _extract_failure_signal(str(first.get("prompt_text") or ""))
    failure_type = signal.failure_type if signal else "tool_error"
    pattern_id = _pattern_id(normalized_phrase)
    message_ids = [int(row["id"]) for row in rows if row.get("id") is not None]
    message_uuids = sorted({str(row.get("message_uuid") or "") for row in rows if row.get("message_uuid")})
    session_ids = sorted({str(row.get("session_id") or "") for row in rows if row.get("session_id")})
    project_paths = sorted({str(row.get("project_path") or "") for row in rows if row.get("project_path")})
    examples: list[str] = []
    seen_examples: set[str] = set()
    for row in rows:
        row_signal = _extract_failure_signal(str(row.get("prompt_text") or ""))
        excerpt = row_signal.excerpt if row_signal else _shorten(row.get("prompt_text"))
        if excerpt and excerpt not in seen_examples:
            seen_examples.add(excerpt)
            examples.append(excerpt)
        if len(examples) >= 3:
            break

    first_seen_at = str(first.get("timestamp") or "")
    last_seen_at = str(last.get("timestamp") or "")
    score, score_reasons = _score_candidate(
        occurrence_count=len(rows),
        last_seen_at=last_seen_at,
        now=now,
        failure_type=failure_type,
    )
    readable = normalized_phrase.replace("<num>", "N")
    topic = f"Claude Code debugging lesson: {readable[:90]}"
    note = (
        f"Repeated Claude Code {failure_type.replace('_', ' ')} pattern appeared "
        f"{len(rows)} times across {len(session_ids)} session(s). "
        f"Pattern: {normalized_phrase}. "
        f"Message IDs: {', '.join(str(message_id) for message_id in message_ids)}. "
        f"Turn this into a practical lesson about recognizing the failure, narrowing the "
        f"cause, and changing the workflow to avoid the loop."
    )
    priority = "high" if len(rows) >= 4 or score >= 78 else "normal"
    source_metadata = {
        "source": SOURCE_NAME,
        "pattern_id": pattern_id,
        "normalized_phrase": normalized_phrase,
        "failure_type": failure_type,
        "occurrence_count": len(rows),
        "message_ids": message_ids,
        "message_uuids": message_uuids,
        "session_ids": session_ids,
        "project_paths": project_paths,
        "first_seen_at": first_seen_at,
        "last_seen_at": last_seen_at,
        "examples": examples,
        "score": score,
        "score_reasons": score_reasons,
    }
    return ClaudeErrorIdeaCandidate(
        pattern_id=pattern_id,
        normalized_phrase=normalized_phrase,
        failure_type=failure_type,
        occurrence_count=len(rows),
        message_ids=message_ids,
        message_uuids=message_uuids,
        session_ids=session_ids,
        project_paths=project_paths,
        first_seen_at=first_seen_at,
        last_seen_at=last_seen_at,
        examples=examples,
        topic=topic,
        note=note,
        priority=priority,
        score=score,
        score_reasons=score_reasons,
        source_metadata=source_metadata,
    )


def build_claude_error_idea_candidates(
    db,
    *,
    days: int = DEFAULT_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    limit: int | None = None,
    now: datetime | None = None,
) -> list[ClaudeErrorIdeaCandidate]:
    """Return grouped Claude Code failure patterns from recent stored messages."""
    if days <= 0 or min_count <= 0 or (limit is not None and limit <= 0):
        return []
    now = now or datetime.now(timezone.utc)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _recent_claude_message_rows(db, days=days, now=now):
        signal = _extract_failure_signal(str(row.get("prompt_text") or ""))
        if signal is None or not signal.phrase:
            continue
        groups[signal.phrase].append(row)

    candidates = [
        _candidate_from_group(phrase, rows, now=now)
        for phrase, rows in groups.items()
        if len(rows) >= min_count
    ]
    candidates.sort(
        key=lambda candidate: (
            -candidate.occurrence_count,
            -candidate.score,
            candidate.normalized_phrase,
        )
    )
    return candidates[:limit] if limit is not None else candidates


def seed_claude_error_ideas(
    db,
    *,
    days: int = DEFAULT_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    limit: int | None = 10,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[ClaudeErrorSeedResult]:
    """Create content ideas from repeated Claude Code failure patterns."""
    candidates = build_claude_error_idea_candidates(
        db,
        days=days,
        min_count=min_count,
        limit=limit,
        now=now,
    )
    results: list[ClaudeErrorSeedResult] = []
    find_existing = getattr(db, "find_active_content_idea_for_source_metadata", None)
    add_idea = getattr(db, "add_content_idea", None) or getattr(db, "insert_content_idea", None)
    if not callable(add_idea):
        return results

    for candidate in candidates:
        existing = None
        if callable(find_existing):
            existing = find_existing(
                note=candidate.note,
                topic=candidate.topic,
                source=SOURCE_NAME,
                source_metadata={"pattern_id": candidate.pattern_id},
            )
        if existing:
            results.append(
                ClaudeErrorSeedResult(
                    status="skipped",
                    pattern_id=candidate.pattern_id,
                    normalized_phrase=candidate.normalized_phrase,
                    failure_type=candidate.failure_type,
                    occurrence_count=candidate.occurrence_count,
                    topic=candidate.topic,
                    score=candidate.score,
                    idea_id=existing["id"],
                    reason=f"{existing['status']} duplicate",
                    note=candidate.note,
                    source_metadata=candidate.source_metadata,
                )
            )
            continue

        if dry_run:
            results.append(
                ClaudeErrorSeedResult(
                    status="proposed",
                    pattern_id=candidate.pattern_id,
                    normalized_phrase=candidate.normalized_phrase,
                    failure_type=candidate.failure_type,
                    occurrence_count=candidate.occurrence_count,
                    topic=candidate.topic,
                    score=candidate.score,
                    idea_id=None,
                    reason="dry run",
                    note=candidate.note,
                    source_metadata=candidate.source_metadata,
                )
            )
            continue

        idea_id = add_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority=candidate.priority,
            source=SOURCE_NAME,
            source_metadata=candidate.source_metadata,
        )
        results.append(
            ClaudeErrorSeedResult(
                status="created",
                pattern_id=candidate.pattern_id,
                normalized_phrase=candidate.normalized_phrase,
                failure_type=candidate.failure_type,
                occurrence_count=candidate.occurrence_count,
                topic=candidate.topic,
                score=candidate.score,
                idea_id=idea_id,
                reason="created",
                note=candidate.note,
                source_metadata=candidate.source_metadata,
            )
        )

    return results


def format_claude_error_candidates_json(
    candidates: list[ClaudeErrorIdeaCandidate],
    seed_results: list[ClaudeErrorSeedResult] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "candidates": [candidate.to_dict() for candidate in candidates],
    }
    if seed_results is not None:
        payload["seed_results"] = [result.to_dict() for result in seed_results]
    return json.dumps(payload, indent=2, sort_keys=True)
