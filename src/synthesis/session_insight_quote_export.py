"""Extract reusable insight quotes from Claude Code session artifacts."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable


SOURCE_NAME = "session_insight_quote_export"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 25
DEFAULT_MIN_CONFIDENCE = 0.65
EXCERPT_CHARS = 400
MAX_QUOTE_LENGTH = 500
MIN_QUOTE_LENGTH = 20


@dataclass(frozen=True)
class SessionInsightQuote:
    quote_id: str
    quote: str
    confidence: float
    reason: str
    category: str
    session_id: str
    session_path: str | None
    project_path: str | None
    message_id: int | None
    message_uuid: str | None
    timestamp: str | None
    source_metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _QuoteSignal:
    quote: str
    confidence: float
    reason: str
    category: str


def extract_session_insight_quotes_from_text(
    text: str,
    *,
    session_metadata: dict[str, Any] | None = None,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> list[SessionInsightQuote]:
    """Extract insight quote candidates from session text."""
    _validate_confidence(min_confidence)
    metadata = dict(session_metadata or {})
    candidates = [
        _candidate_from_signal(signal, metadata)
        for signal in _extract_quote_signals(text)
        if signal.confidence >= min_confidence
    ]
    return _dedupe_candidates(candidates)


def extract_session_insight_quotes_from_rows(
    rows: Iterable[dict[str, Any]],
    *,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> list[SessionInsightQuote]:
    """Extract insight quotes from stored Claude message/session rows."""
    _validate_confidence(min_confidence)
    candidates: list[SessionInsightQuote] = []
    for row in rows:
        text = _row_text(row)
        if not text:
            continue
        candidates.extend(
            extract_session_insight_quotes_from_text(
                text,
                session_metadata=_row_metadata(row),
                min_confidence=min_confidence,
            )
        )
    return _dedupe_candidates(candidates)


def build_session_insight_quote_exports(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int | None = DEFAULT_LIMIT,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    now: datetime | None = None,
) -> list[SessionInsightQuote]:
    """Return recent session insight quotes from rows or a database handle."""
    if days <= 0 or (limit is not None and limit <= 0):
        return []
    _validate_confidence(min_confidence)
    now = now or datetime.now(timezone.utc)
    rows = (
        list(db_or_rows)
        if isinstance(db_or_rows, (list, tuple))
        else _recent_claude_message_rows(db_or_rows, days=days, now=now)
    )
    candidates = extract_session_insight_quotes_from_rows(
        rows,
        min_confidence=min_confidence,
    )
    return candidates[:limit] if limit is not None else candidates


def format_session_insight_quotes_json(
    quotes: list[SessionInsightQuote],
) -> str:
    return json.dumps([quote.to_dict() for quote in quotes], indent=2, sort_keys=True)


def format_session_insight_quotes_csv(
    quotes: list[SessionInsightQuote],
) -> str:
    if not quotes:
        return "quote_id,quote,confidence,reason,category,session_id,timestamp\n"

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=["quote_id", "quote", "confidence", "reason", "category", "session_id", "timestamp"],
        extrasaction="ignore",
    )
    writer.writeheader()
    for quote in quotes:
        writer.writerow(quote.to_dict())
    return output.getvalue()


def format_session_insight_quotes_text(
    quotes: list[SessionInsightQuote],
) -> str:
    lines = [f"insight_quotes={len(quotes)}"]
    lines.append(f"{'Conf':>4s}  {'Session':16s}  Quote")
    lines.append(f"{'-' * 4:>4s}  {'-' * 16:16s}  {'-' * 60}")
    if not quotes:
        lines.append("   -  -                 no insight quotes found")
        return "\n".join(lines)
    for quote in quotes:
        lines.append(
            f"{quote.confidence:4.2f}  "
            f"{_shorten(quote.session_id, 16):16s}  "
            f"{_shorten(quote.quote, 80)}"
        )
    return "\n".join(lines)


def format_session_insight_quotes_markdown(
    quotes: list[SessionInsightQuote],
) -> str:
    """Format quotes as markdown with grouping by category."""
    if not quotes:
        return "# Session Insight Quotes\n\nNo insight quotes found.\n"

    lines = ["# Session Insight Quotes\n"]

    # Group by category
    by_category: dict[str, list[SessionInsightQuote]] = {}
    for quote in quotes:
        category = quote.category or "general"
        if category not in by_category:
            by_category[category] = []
        by_category[category].append(quote)

    # Output each category
    for category in sorted(by_category.keys()):
        category_quotes = by_category[category]
        lines.append(f"## {category.title()} Insights ({len(category_quotes)})\n")

        for quote in category_quotes:
            lines.append(f"### Quote (confidence: {quote.confidence:.2f})\n")
            lines.append(f"> {quote.quote}\n")
            lines.append(f"- **Session**: `{quote.session_id}`")
            if quote.timestamp:
                lines.append(f"- **Time**: {quote.timestamp}")
            if quote.project_path:
                lines.append(f"- **Project**: `{quote.project_path}`")
            lines.append(f"- **Reason**: {quote.reason}\n")

    return "\n".join(lines)


def format_session_insight_quotes_html(
    quotes: list[SessionInsightQuote],
) -> str:
    """Format quotes as HTML with styling and category grouping."""
    if not quotes:
        return """<!DOCTYPE html>
<html>
<head>
    <title>Session Insight Quotes</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 900px; margin: 40px auto; padding: 20px; }
        h1 { color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }
        h2 { color: #555; margin-top: 30px; }
        .quote { background: #f9f9f9; border-left: 4px solid #4CAF50; padding: 15px; margin: 15px 0; }
        .quote-text { font-size: 1.1em; font-style: italic; margin-bottom: 10px; }
        .metadata { font-size: 0.9em; color: #666; }
        .confidence { font-weight: bold; color: #4CAF50; }
        .category { display: inline-block; background: #4CAF50; color: white; padding: 3px 8px; border-radius: 3px; font-size: 0.8em; }
    </style>
</head>
<body>
    <h1>Session Insight Quotes</h1>
    <p>No insight quotes found.</p>
</body>
</html>"""

    # Group by category
    by_category: dict[str, list[SessionInsightQuote]] = {}
    for quote in quotes:
        category = quote.category or "general"
        if category not in by_category:
            by_category[category] = []
        by_category[category].append(quote)

    lines = ["""<!DOCTYPE html>
<html>
<head>
    <title>Session Insight Quotes</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 900px; margin: 40px auto; padding: 20px; }
        h1 { color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }
        h2 { color: #555; margin-top: 30px; border-bottom: 2px solid #ddd; padding-bottom: 8px; }
        .quote { background: #f9f9f9; border-left: 4px solid #4CAF50; padding: 15px; margin: 15px 0; }
        .quote-text { font-size: 1.1em; font-style: italic; margin-bottom: 10px; }
        .metadata { font-size: 0.9em; color: #666; }
        .metadata-item { margin: 3px 0; }
        .confidence { font-weight: bold; color: #4CAF50; }
        .category { display: inline-block; background: #4CAF50; color: white; padding: 3px 8px; border-radius: 3px; font-size: 0.8em; }
        .technical { border-left-color: #2196F3; }
        .workflow { border-left-color: #FF9800; }
        .debugging { border-left-color: #F44336; }
        code { background: #eee; padding: 2px 5px; border-radius: 3px; font-family: monospace; }
    </style>
</head>
<body>
    <h1>Session Insight Quotes</h1>"""]

    for category in sorted(by_category.keys()):
        category_quotes = by_category[category]
        lines.append(f"    <h2>{category.title()} Insights ({len(category_quotes)})</h2>")

        for quote in category_quotes:
            css_class = f"quote {category}"
            lines.append(f'    <div class="{css_class}">')
            lines.append(f'        <div class="quote-text">"{quote.quote}"</div>')
            lines.append(f'        <div class="metadata">')
            lines.append(f'            <div class="metadata-item"><span class="confidence">Confidence: {quote.confidence:.2f}</span> &bull; <span class="category">{quote.category}</span></div>')
            lines.append(f'            <div class="metadata-item">Session: <code>{_shorten(quote.session_id, 50)}</code></div>')
            if quote.timestamp:
                lines.append(f'            <div class="metadata-item">Time: {quote.timestamp}</div>')
            if quote.project_path:
                lines.append(f'            <div class="metadata-item">Project: <code>{quote.project_path}</code></div>')
            lines.append(f'            <div class="metadata-item">Reason: {quote.reason}</div>')
            lines.append(f'        </div>')
            lines.append(f'    </div>')

    lines.append("""</body>
</html>""")

    return "\n".join(lines)


def _extract_quote_signals(text: str) -> list[_QuoteSignal]:
    """Extract potential insight quotes from text."""
    if not text or not text.strip():
        return []

    # Split into sentences/lines and clean
    sentences = _split_into_sentences(text)
    signals: list[_QuoteSignal] = []

    for sentence in sentences:
        if not sentence:
            continue

        # Filter out noise before scoring
        if _is_noise(sentence):
            continue

        # Check if it contains secrets or overly long paths
        if _contains_secrets_or_long_paths(sentence):
            continue

        # Score the sentence as a potential insight quote
        signal = _score_quote_candidate(sentence)
        if signal is not None:
            signals.append(signal)

    return signals


def _split_into_sentences(text: str) -> list[str]:
    """Split text into sentence-like chunks."""
    # Split on common sentence boundaries
    lines = text.split('\n')
    sentences = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Split on sentence boundaries (with or without capital after)
        # First try splitting with capital letter requirement
        parts = re.split(r'(?<=[.!?])\s+(?=[A-Z])', line)
        if len(parts) == 1:
            # If no splits, try splitting on any sentence boundary
            parts = re.split(r'(?<=[.!?])\s+', line)

        for part in parts:
            cleaned = _clean_text(part)
            if MIN_QUOTE_LENGTH <= len(cleaned) <= MAX_QUOTE_LENGTH:
                sentences.append(cleaned)

    return sentences


def _is_noise(sentence: str) -> bool:
    """Check if a sentence is likely noise (command output, generic status)."""
    lowered = sentence.lower()

    # Command-like output
    if re.match(r'^[$>#]\s+', sentence):
        return True

    # Generic status messages
    noise_patterns = [
        r'^(done|ok|success|failed|error|warning)\b',
        r'^\d+\s+(passed|failed|error|warning)',
        r'^(created|updated|deleted|modified)\s+\d+\s+(file|line)',
        r'^running\s+test',
        r'^[\w/.-]+:\d+:\d+:',  # file:line:col format
        r'^\s*at\s+[\w.]+\s+\(',  # stack trace
        r'^\s*file\s+["\']',  # Python traceback (lowercase to match lowered)
    ]

    for pattern in noise_patterns:
        if re.search(pattern, lowered):
            return True

    # Very short generic phrases
    if len(sentence.split()) <= 3 and not re.search(r'\b(discovered|noticed|realized|found that|learned)\b', lowered):
        return True

    return False


def _contains_secrets_or_long_paths(sentence: str) -> bool:
    """Check if sentence contains secrets or very long absolute paths."""
    # Check for secret-like tokens
    secret_patterns = [
        r'\b[A-Za-z0-9]{32,}\b',  # Long alphanumeric strings (likely tokens)
        r'(?:api[_-]?key|token|secret|password)\s*[:=]\s*\S+',
        r'\bBearer\s+[A-Za-z0-9._~+/=-]+',
        r'\b(?:gh[opsu]_|sk-ant-|xox[baprs]-)[A-Za-z0-9_-]+',
    ]

    for pattern in secret_patterns:
        if re.search(pattern, sentence, re.IGNORECASE):
            return True

    # Check for long absolute paths (more than 4 path segments)
    path_segments = re.findall(r'/[^/\s]{1,30}(?:/[^/\s]{1,30}){4,}', sentence)
    if path_segments:
        return True

    return False


def _score_quote_candidate(sentence: str) -> _QuoteSignal | None:
    """Score a sentence as an insight quote candidate."""
    lowered = sentence.lower()
    confidence = 0.5
    reasons: list[str] = []
    category_scores = {"technical": 0.0, "workflow": 0.0, "debugging": 0.0}

    # First-person technical observation markers (strongest signal)
    first_person_patterns = [
        (r'\bi (noticed|discovered|realized|found|learned)', 0.25, "first-person observation"),
        (r'\bwe (noticed|discovered|realized|found|learned)', 0.22, "collaborative observation"),
        (r'\b(turns out|it turns out)\b', 0.18, "discovery language"),
        (r'\b(interesting|surprisingly|unexpectedly)\b', 0.15, "notable finding"),
    ]

    for pattern, boost, reason in first_person_patterns:
        if re.search(pattern, lowered):
            confidence += boost
            reasons.append(reason)

    # Technical substance indicators with category hints
    technical_patterns = [
        (r'\b(pattern|approach|strategy|technique|method|implementation|architecture|logic)\b', 0.12, "technical concept", "technical"),
        (r'\b(performance|optimization|efficiency|bottleneck|latency)\b', 0.12, "performance insight", "technical"),
        (r'\b(bug|issue|problem|error|failure|edge case|flaw|flawed)\b', 0.10, "problem identification", "debugging"),
        (r'\b(solution|fix|workaround|alternative)\b', 0.10, "solution insight", "debugging"),
        (r'\b(trade-?off|balance|compromise)\b', 0.14, "tradeoff awareness", "technical"),
        (r'\b(cache|caching|database|query|api|algorithm)\b', 0.08, "technical domain", "technical"),
    ]

    for pattern, boost, reason, cat in technical_patterns:
        if re.search(pattern, lowered):
            confidence += boost
            reasons.append(reason)
            category_scores[cat] += boost

    # Workflow indicators
    workflow_patterns = [
        (r'\b(workflow|process|habit|routine|practice)\b', 0.12, "workflow insight", "workflow"),
        (r'\b(always|never|usually|typically|generally)\s+(use|do|check|verify|test)', 0.10, "habit pattern", "workflow"),
        (r'\b(before|after|when)\s+\w+ing', 0.08, "sequential practice", "workflow"),
        (r'\b(prefer|recommend|suggest|advise)\b', 0.09, "recommendation", "workflow"),
    ]

    for pattern, boost, reason, cat in workflow_patterns:
        if re.search(pattern, lowered):
            confidence += boost
            reasons.append(reason)
            category_scores[cat] += boost

    # Debugging indicators
    debugging_patterns = [
        (r'\b(debug|debugging|troubleshoot|investigate|diagnose)\b', 0.15, "debugging activity", "debugging"),
        (r'\b(trace|stack trace|backtrace|log|logging)\b', 0.10, "debugging technique", "debugging"),
        (r'\b(breakpoint|step through|inspect|examine)\b', 0.12, "debugging method", "debugging"),
        (r'\b(root cause|underlying|actual issue|real problem)\b', 0.13, "root cause analysis", "debugging"),
    ]

    for pattern, boost, reason, cat in debugging_patterns:
        if re.search(pattern, lowered):
            confidence += boost
            reasons.append(reason)
            category_scores[cat] += boost

    # Deductions for generic/low-value content
    if re.search(r'\b(just|simply|basically|essentially)\b', lowered):
        confidence -= 0.08
        reasons.append("filler language")

    if re.search(r'\b(obviously|clearly|of course)\b', lowered):
        confidence -= 0.10
        reasons.append("obviousness marker")

    if sentence.endswith('?'):
        confidence -= 0.20
        reasons.append("question form")

    # Check for code-like syntax (neutral, not necessarily bad)
    if re.search(r'[{}()\[\]<>]', sentence) and len(re.findall(r'[{}()\[\]<>]', sentence)) > 3:
        confidence -= 0.12
        reasons.append("code-heavy syntax")

    confidence = round(max(0.0, min(confidence, 0.98)), 2)

    if confidence <= 0 or not reasons:
        return None

    # Determine primary category based on scores
    primary_category = max(category_scores.items(), key=lambda x: x[1])
    category = primary_category[0] if primary_category[1] > 0 else "general"

    return _QuoteSignal(
        quote=sentence,
        confidence=confidence,
        reason=", ".join(reasons),
        category=category,
    )


def _candidate_from_signal(
    signal: _QuoteSignal,
    metadata: dict[str, Any],
) -> SessionInsightQuote:
    session_id = str(metadata.get("session_id") or metadata.get("sessionId") or "plain-transcript")
    quote = _clean_text(signal.quote)
    quote_id = _quote_id(session_id, quote)
    message_id = _int_or_none(metadata.get("id") or metadata.get("message_id"))
    message_uuid = _optional_text(metadata.get("message_uuid") or metadata.get("uuid"))
    project_path = _optional_text(metadata.get("project_path") or metadata.get("cwd") or metadata.get("project"))
    session_path = _optional_text(
        metadata.get("session_path") or metadata.get("path") or metadata.get("artifact_path")
    )
    timestamp = _optional_text(metadata.get("timestamp"))

    source_metadata = {
        "source": SOURCE_NAME,
        "quote_id": quote_id,
        "quote": quote,
        "confidence": signal.confidence,
        "reason": signal.reason,
        "category": signal.category,
        "session_id": session_id,
        "session_path": session_path,
        "project_path": project_path,
        "message_id": message_id,
        "message_uuid": message_uuid,
        "timestamp": timestamp,
    }

    return SessionInsightQuote(
        quote_id=quote_id,
        quote=quote,
        confidence=signal.confidence,
        reason=signal.reason,
        category=signal.category,
        session_id=session_id,
        session_path=session_path,
        project_path=project_path,
        message_id=message_id,
        message_uuid=message_uuid,
        timestamp=timestamp,
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
    candidates: Iterable[SessionInsightQuote],
) -> list[SessionInsightQuote]:
    best: dict[str, SessionInsightQuote] = {}
    for candidate in candidates:
        existing = best.get(candidate.quote_id)
        if existing is None or candidate.confidence > existing.confidence:
            best[candidate.quote_id] = candidate
    return sorted(
        best.values(),
        key=lambda candidate: (
            -candidate.confidence,
            str(candidate.timestamp or ""),
            candidate.quote_id,
        ),
    )


def _shorten(text: str | None, width: int = EXCERPT_CHARS) -> str:
    value = _clean_text(text or "")
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _normalize_quote(quote: str) -> str:
    value = _clean_text(quote).lower()
    value = re.sub(r"`{1,3}", "", value)
    value = re.sub(r"https?://\S+", "<url>", value)
    value = re.sub(r"(/[^\s:]+)+", "<path>", value)
    value = re.sub(r"\b[a-f0-9]{7,40}\b", "<hash>", value)
    value = re.sub(r"\b\d+\b", "<num>", value)
    value = re.sub(r"[^a-z0-9<> ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _quote_id(session_id: str, quote: str) -> str:
    identity = f"{session_id}|{_normalize_quote(quote)}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"quote_{digest}"


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
