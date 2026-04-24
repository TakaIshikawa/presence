"""Deterministic contradiction checks between drafts and linked knowledge."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_WORD_RE = re.compile(r"[a-z0-9][a-z0-9.+#-]*")
_NUMBER_RE = re.compile(
    r"""
    (?P<value>
        \$\s*\d+(?:[,.]\d+)*(?:\.\d+)?
        |
        \b\d+(?:[,.]\d+)*(?:\.\d+)?
        |
        \b(?:zero|one|two|three|four|five|six|seven|eight|nine|ten)\b
    )
    \s*
    (?P<unit>
        %|x|percent|percentage|times|ms|s|sec|secs|seconds?|minutes?|hours?|
        days?|weeks?|months?|years?|repos?|repositories|commits?|files?|tests?|
        errors?|requests?|users?|tokens?|chars?|characters?|lines?
    )?
    """,
    re.IGNORECASE | re.VERBOSE,
)
_ISO_DATE_RE = re.compile(r"\b(?P<year>20\d{2}|19\d{2})-(?P<month>\d{1,2})(?:-(?P<day>\d{1,2}))?\b")
_MONTH_DATE_RE = re.compile(
    r"\b(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s+(?:(?P<day>\d{1,2})(?:st|nd|rd|th)?,?\s+)?(?P<year>20\d{2}|19\d{2})\b",
    re.IGNORECASE,
)
_VERSION_RE = re.compile(
    r"\b(?P<name>[A-Z][A-Za-z0-9.+#-]{1,30})\s+"
    r"(?P<version>v?\d+(?:\.\d+){1,3}(?:-[A-Za-z0-9.]+)?)\b"
)

_NUMBER_WORDS = {
    "zero": Decimal("0"),
    "one": Decimal("1"),
    "two": Decimal("2"),
    "three": Decimal("3"),
    "four": Decimal("4"),
    "five": Decimal("5"),
    "six": Decimal("6"),
    "seven": Decimal("7"),
    "eight": Decimal("8"),
    "nine": Decimal("9"),
    "ten": Decimal("10"),
}
_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_STOPWORDS = {
    "about",
    "after",
    "also",
    "and",
    "are",
    "because",
    "before",
    "being",
    "between",
    "build",
    "built",
    "but",
    "can",
    "content",
    "draft",
    "from",
    "has",
    "have",
    "into",
    "just",
    "knowledge",
    "more",
    "new",
    "not",
    "now",
    "only",
    "over",
    "post",
    "release",
    "released",
    "same",
    "says",
    "ship",
    "shipped",
    "source",
    "that",
    "the",
    "this",
    "through",
    "today",
    "using",
    "was",
    "were",
    "when",
    "with",
    "work",
}


@dataclass(frozen=True)
class KnowledgeSnippet:
    """Linked knowledge text used as contradiction evidence."""

    knowledge_id: int
    content: str
    insight: str | None = None
    source_type: str | None = None
    source_url: str | None = None
    author: str | None = None


@dataclass(frozen=True)
class ExtractedClaim:
    """A simple factual value extracted from one sentence."""

    kind: str
    value: str
    text: str
    terms: list[str]
    unit: str | None = None
    label: str | None = None


@dataclass(frozen=True)
class ContradictionWarning:
    """An obvious mismatch between generated draft text and linked knowledge."""

    content_id: int
    kind: str
    claim: str
    claim_value: str
    evidence_value: str
    evidence: str
    knowledge_id: int
    reason: str
    source_url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def scan_content_id(db: Any, content_id: int) -> list[ContradictionWarning]:
    """Scan one generated content row against its linked knowledge."""

    content = db.get_generated_content(content_id)
    if content is None:
        raise ValueError(f"Content ID {content_id} not found")
    return scan_text_against_linked_knowledge(
        content_id=content_id,
        draft_text=content.get("content") or "",
        linked_knowledge=_load_linked_knowledge(db, content_id),
    )


def scan_recent_unpublished(db: Any, recent_days: int = 7) -> dict[int, list[ContradictionWarning]]:
    """Scan recent unpublished generated content rows."""

    rows = db.conn.execute(
        """SELECT id, content
           FROM generated_content
           WHERE COALESCE(published, 0) = 0
             AND created_at >= datetime('now', ?)
           ORDER BY created_at DESC, id DESC""",
        (f"-{recent_days} days",),
    ).fetchall()
    results: dict[int, list[ContradictionWarning]] = {}
    for row in rows:
        content_id = int(row["id"])
        warnings = scan_text_against_linked_knowledge(
            content_id=content_id,
            draft_text=row["content"] or "",
            linked_knowledge=_load_linked_knowledge(db, content_id),
        )
        if warnings:
            results[content_id] = warnings
    return results


def scan_text_against_linked_knowledge(
    *,
    content_id: int,
    draft_text: str,
    linked_knowledge: list[KnowledgeSnippet],
) -> list[ContradictionWarning]:
    """Return conservative contradiction warnings for linked knowledge."""

    if not linked_knowledge:
        return []

    draft_claims = extract_claims(draft_text)
    if not draft_claims:
        return []

    warnings: list[ContradictionWarning] = []
    seen: set[tuple[str, str, int, str]] = set()
    for knowledge in linked_knowledge:
        for evidence_sentence in _knowledge_sentences(knowledge):
            evidence_claims = extract_claims(evidence_sentence)
            if not evidence_claims:
                continue
            for claim in draft_claims:
                for evidence_claim in evidence_claims:
                    reason = _contradiction_reason(claim, evidence_claim)
                    if not reason:
                        continue
                    key = (claim.kind, claim.text, knowledge.knowledge_id, evidence_sentence)
                    if key in seen:
                        continue
                    seen.add(key)
                    warnings.append(
                        ContradictionWarning(
                            content_id=content_id,
                            kind=claim.kind,
                            claim=claim.text,
                            claim_value=claim.value,
                            evidence_value=evidence_claim.value,
                            evidence=_shorten(evidence_sentence, 220),
                            knowledge_id=knowledge.knowledge_id,
                            reason=reason,
                            source_url=knowledge.source_url,
                        )
                    )
                    break
    return warnings


def extract_claims(text: str) -> list[ExtractedClaim]:
    """Extract simple numeric, date, and named-version claims."""

    claims: list[ExtractedClaim] = []
    for sentence in _sentences(text):
        terms = _keywords(sentence)
        date_claims, date_spans = _date_claims(sentence, terms)
        version_claims, version_spans = _version_claims(sentence, terms)
        claims.extend(_numeric_claims(sentence, terms, skip_spans=date_spans + version_spans))
        claims.extend(date_claims)
        claims.extend(version_claims)
    return claims


def _load_linked_knowledge(db: Any, content_id: int) -> list[KnowledgeSnippet]:
    links = db.get_content_lineage(content_id)
    return [
        KnowledgeSnippet(
            knowledge_id=int(link["id"]),
            content=link.get("content") or "",
            insight=link.get("insight"),
            source_type=link.get("source_type"),
            source_url=link.get("source_url"),
            author=link.get("author"),
        )
        for link in links
    ]


def _sentences(text: str) -> list[str]:
    clean = re.sub(r"^TWEET\s+\d+:\s*", "", text or "", flags=re.IGNORECASE | re.MULTILINE)
    return [part.strip(" -\t") for part in _SENTENCE_RE.split(clean) if part.strip(" -\t")]


def _knowledge_sentences(knowledge: KnowledgeSnippet) -> list[str]:
    text = "\n".join(part for part in [knowledge.content, knowledge.insight or ""] if part)
    return _sentences(text)


def _numeric_claims(
    sentence: str,
    terms: list[str],
    *,
    skip_spans: list[tuple[int, int]] | None = None,
) -> list[ExtractedClaim]:
    claims = []
    skip_spans = skip_spans or []
    for match in _NUMBER_RE.finditer(sentence):
        if any(start <= match.start() < end for start, end in skip_spans):
            continue
        value = _normalize_number_value(match.group("value"))
        if value is None:
            continue
        unit = _normalize_unit(match.group("unit"))
        value_text = f"{value}{unit}" if unit == "%" else str(value)
        if unit and unit != "%":
            value_text = f"{value} {unit}"
        claims.append(
            ExtractedClaim(
                kind="numeric",
                value=value_text,
                text=sentence,
                terms=terms,
                unit=unit,
            )
        )
    return claims


def _date_claims(sentence: str, terms: list[str]) -> tuple[list[ExtractedClaim], list[tuple[int, int]]]:
    claims = []
    occupied: list[tuple[int, int]] = []
    for match in _ISO_DATE_RE.finditer(sentence):
        value = _date_value(match.group("year"), match.group("month"), match.group("day"))
        claims.append(ExtractedClaim(kind="date", value=value, text=sentence, terms=terms))
        occupied.append(match.span())

    for match in _MONTH_DATE_RE.finditer(sentence):
        if any(start <= match.start() < end for start, end in occupied):
            continue
        value = _date_value(
            match.group("year"),
            str(_MONTHS[match.group("month").lower()]),
            match.group("day"),
        )
        claims.append(ExtractedClaim(kind="date", value=value, text=sentence, terms=terms))
        occupied.append(match.span())
    return claims, occupied


def _version_claims(sentence: str, terms: list[str]) -> tuple[list[ExtractedClaim], list[tuple[int, int]]]:
    claims = []
    spans = []
    for match in _VERSION_RE.finditer(sentence):
        name = match.group("name").lower()
        version = match.group("version").lower()
        claims.append(
            ExtractedClaim(
                kind="version",
                value=f"{name} {version}",
                text=sentence,
                terms=terms + [name],
                label=name,
            )
        )
        spans.append(match.span())
    return claims, spans


def _contradiction_reason(claim: ExtractedClaim, evidence: ExtractedClaim) -> str | None:
    if claim.kind != evidence.kind:
        return None

    if claim.kind == "numeric":
        if claim.unit != evidence.unit:
            return None
        if claim.value == evidence.value:
            return None
        if not _related_context(claim, evidence):
            return None
        return "numeric value conflicts with linked knowledge"

    if claim.kind == "date":
        if claim.value == evidence.value:
            return None
        if not _same_date_precision(claim.value, evidence.value):
            return None
        if not _related_context(claim, evidence):
            return None
        return "date conflicts with linked knowledge"

    if claim.kind == "version":
        if claim.label != evidence.label:
            return None
        if claim.value == evidence.value:
            return None
        return "named version conflicts with linked knowledge"

    return None


def _related_context(claim: ExtractedClaim, evidence: ExtractedClaim) -> bool:
    claim_terms = set(claim.terms)
    evidence_terms = set(evidence.terms)
    overlap = claim_terms & evidence_terms
    if len(overlap) >= 2:
        return True
    if claim.label and claim.label == evidence.label:
        return True
    distinctive = {term for term in overlap if len(term) >= 6}
    return bool(distinctive and (len(claim_terms) <= 3 or len(evidence_terms) <= 3))


def _same_date_precision(left: str, right: str) -> bool:
    return left.count("-") == right.count("-")


def _keywords(sentence: str) -> list[str]:
    keywords = []
    for word in _WORD_RE.findall(sentence.lower()):
        normalized = word.strip(".+-#")
        if (
            len(normalized) >= 4
            and normalized not in _STOPWORDS
            and normalized not in _MONTHS
            and not normalized.isdigit()
            and normalized not in keywords
        ):
            keywords.append(normalized)
    return keywords[:10]


def _normalize_number_value(value: str) -> Decimal | None:
    text = value.lower().replace(",", "").replace("$", "").strip()
    if text in _NUMBER_WORDS:
        return _NUMBER_WORDS[text]
    try:
        number = Decimal(text)
    except InvalidOperation:
        return None
    return number.normalize()


def _normalize_unit(unit: str | None) -> str | None:
    if not unit:
        return None
    normalized = unit.lower()
    if normalized in {"percent", "percentage"}:
        return "%"
    if normalized in {"sec", "secs"}:
        return "seconds"
    if normalized.endswith("s") and normalized not in {"ms"}:
        normalized = normalized[:-1]
    if normalized == "repositories":
        return "repo"
    if normalized == "chars":
        return "character"
    return normalized


def _date_value(year: str, month: str, day: str | None) -> str:
    if day:
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    return f"{int(year):04d}-{int(month):02d}"


def _shorten(value: str, limit: int) -> str:
    text = re.sub(r"\s+", " ", value).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."
