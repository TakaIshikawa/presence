"""Deterministic scoring for concrete evidence in generated drafts."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


NUMERIC_SPECIFIC_RE = re.compile(
    r"""
    (?:
        \$\s*\d+(?:[,.]\d+)*(?:\.\d+)?
        |
        \b\d+(?:[,.]\d+)*(?:\.\d+)?\s*(?:%|x)(?=\W|$)
        |
        \b\d+(?:[,.]\d+)*(?:\.\d+)?\s*
        (?:ms|s|sec|secs|seconds?|minutes?|hours?|days?|weeks?|months?|years?|
           repos?|repositories|commits?|prs?|pull\s+requests?|issues?|files?|
           tests?|errors?|requests?|users?|tokens?|chars?|characters?|lines?|runs?)\b
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

SOURCE_REFERENCE_RE = re.compile(
    r"""
    (?:
        \b(?:commit|sha)\s+[0-9a-f]{7,40}\b
        |
        \b[0-9a-f]{12,40}\b
        |
        \b(?:pr|pull\s+request|issue|ticket|activity)\s*\#?\d+\b
        |
        \b(?:source|message|session|content)[_-]?id\s*[:\#]?\s*[a-z0-9][a-z0-9_.:-]{5,}\b
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

QUOTED_TECHNICAL_NOUN_RE = re.compile(
    r"(?:`([^`]{2,80})`|\"([A-Za-z0-9_.:/# -]{3,80})\"|'([A-Za-z0-9_.:/# -]{3,80})')"
)

IMPLEMENTATION_TERM_RE = re.compile(
    r"""
    \b(?:
        api|cli|cache|queue|worker|serializer|parser|schema|migration|index|query|
        sqlite|postgres|redis|http|webhook|endpoint|token|embedding|dedup|regex|
        retry|rollback|fixture|pytest|snapshot|pipeline|scheduler|backfill|lockfile|
        config|database|transaction|rate\s+limit|feature\s+flag|adapter|validator
    )s?\b
    |
    \b[a-z]+_[a-z0-9_]+\b
    |
    \b[A-Za-z]+(?:[A-Z][a-z0-9]+){1,}\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

VAGUE_FILLER_RE = re.compile(
    r"\b(?:stuff|things|something|somehow|basically|really|very|just|kind of|sort of|"
    r"a lot|many people|everyone says|it turns out|at the end of the day|"
    r"move fast|do better work|make it better|best practices?)\b",
    re.IGNORECASE,
)

ABSOLUTE_CLAIM_RE = re.compile(
    r"\b(?:always|never|everyone|nobody|no one|all|none|guaranteed|impossible|"
    r"only way|best way|worst|perfect|must|can't fail|cannot fail)\b",
    re.IGNORECASE,
)


@dataclass
class EvidenceSignal:
    """One evidence-density signal found in the draft."""

    name: str
    count: int
    weight: int
    examples: list[str] = field(default_factory=list)


@dataclass
class EvidenceDensityReport:
    """Evidence-density score and review guidance for generated content."""

    score: int
    status: str
    positive_signals: list[EvidenceSignal]
    negative_signals: list[EvidenceSignal]
    recommendations: list[str]
    content_id: int | None = None


def score_evidence_density(
    text: str,
    *,
    content_id: int | None = None,
    source_commits: list[str] | None = None,
    source_messages: list[str] | None = None,
    source_activity_ids: list[str] | None = None,
) -> EvidenceDensityReport:
    """Score whether a draft contains concrete supporting detail."""

    positives = _positive_signals(
        text,
        source_commits=source_commits or [],
        source_messages=source_messages or [],
        source_activity_ids=source_activity_ids or [],
    )
    negatives = _negative_signals(text, positives)
    score = _clamp_score(35 + sum(s.weight for s in positives) - sum(s.weight for s in negatives))
    status = _status(score)
    return EvidenceDensityReport(
        score=score,
        status=status,
        positive_signals=positives,
        negative_signals=negatives,
        recommendations=_recommendations(positives, negatives, score),
        content_id=content_id,
    )


def _positive_signals(
    text: str,
    *,
    source_commits: list[str],
    source_messages: list[str],
    source_activity_ids: list[str],
) -> list[EvidenceSignal]:
    signals = []
    numeric = _unique_matches(NUMERIC_SPECIFIC_RE, text)
    if numeric:
        signals.append(
            EvidenceSignal(
                "numeric_specifics",
                len(numeric),
                min(24, 8 * len(numeric)),
                numeric[:5],
            )
        )

    source_refs = _unique_matches(SOURCE_REFERENCE_RE, text)
    linked_sources = [str(ref) for ref in source_commits + source_messages + source_activity_ids if ref]
    source_count = len(source_refs) + len(linked_sources)
    if source_count:
        examples = (source_refs + linked_sources)[:5]
        signals.append(
            EvidenceSignal(
                "source_references",
                source_count,
                min(28, 10 * source_count),
                examples,
            )
        )

    quoted = [
        match
        for match in _unique_matches(QUOTED_TECHNICAL_NOUN_RE, text)
        if _looks_technical(match)
    ]
    if quoted:
        signals.append(
            EvidenceSignal(
                "quoted_technical_nouns",
                len(quoted),
                min(18, 6 * len(quoted)),
                quoted[:5],
            )
        )

    implementation_terms = _unique_matches(IMPLEMENTATION_TERM_RE, text)
    if implementation_terms:
        signals.append(
            EvidenceSignal(
                "implementation_terms",
                len(implementation_terms),
                min(24, 4 * len(implementation_terms)),
                implementation_terms[:8],
            )
        )

    return signals


def _negative_signals(text: str, positives: list[EvidenceSignal]) -> list[EvidenceSignal]:
    signals = []
    vague = _unique_matches(VAGUE_FILLER_RE, text)
    if vague:
        signals.append(
            EvidenceSignal(
                "vague_filler_phrases",
                len(vague),
                min(24, 6 * len(vague)),
                vague[:6],
            )
        )

    absolutes = _unique_matches(ABSOLUTE_CLAIM_RE, text)
    concrete_support = {
        signal.name
        for signal in positives
        if signal.name in {"numeric_specifics", "source_references", "quoted_technical_nouns"}
    }
    if absolutes and len(concrete_support) < 2:
        signals.append(
            EvidenceSignal(
                "unsupported_absolute_claims",
                len(absolutes),
                min(28, 9 * len(absolutes)),
                absolutes[:6],
            )
        )
    return signals


def _recommendations(
    positives: list[EvidenceSignal],
    negatives: list[EvidenceSignal],
    score: int,
) -> list[str]:
    positive_names = {signal.name for signal in positives}
    negative_names = {signal.name for signal in negatives}
    recommendations = []
    if "numeric_specifics" not in positive_names:
        recommendations.append("Add a concrete number, duration, count, percentage, or before/after measurement.")
    if "source_references" not in positive_names:
        recommendations.append("Tie the draft to a source commit, message, activity id, PR, or issue reference.")
    if "implementation_terms" not in positive_names:
        recommendations.append("Name the implementation surface involved, such as the API, migration, queue, parser, or test.")
    if "vague_filler_phrases" in negative_names:
        recommendations.append("Replace vague filler with the exact behavior, file, subsystem, or result.")
    if "unsupported_absolute_claims" in negative_names:
        recommendations.append("Qualify absolute claims or support them with a source reference and specific evidence.")
    if score >= 70 and not recommendations:
        recommendations.append("Evidence density is strong; keep the concrete details intact during editing.")
    return recommendations


def _unique_matches(pattern: re.Pattern[str], text: str) -> list[str]:
    seen = set()
    values = []
    for match in pattern.finditer(text):
        groups = [group for group in match.groups() if group]
        value = groups[0] if groups else match.group(0)
        normalized = re.sub(r"\s+", " ", value.strip())
        key = normalized.lower()
        if normalized and key not in seen:
            values.append(normalized)
            seen.add(key)
    return values


def _looks_technical(value: str) -> bool:
    lowered = value.lower()
    if re.search(r"[_.:/#-]|[a-z]+[A-Z]|\b(api|cli|db|sql|json|yaml|pytest|uv|id)\b", value):
        return True
    return bool(IMPLEMENTATION_TERM_RE.search(lowered))


def _clamp_score(score: int) -> int:
    return max(0, min(100, int(score)))


def _status(score: int) -> str:
    if score >= 70:
        return "grounded"
    if score >= 45:
        return "needs_detail"
    return "thin"
