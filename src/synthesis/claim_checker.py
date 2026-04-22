"""Deterministic checks for risky generated claims."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


_NUMBER_RE = re.compile(
    r"""
    (?:
        \$\s*\d+(?:[,.]\d+)*(?:\.\d+)?
        |
        \b\d+(?:[,.]\d+)*(?:\.\d+)?\s*(?:%|x)
        |
        \b\d+(?:[,.]\d+)*(?:\.\d+)?\s*
        (?:percent|percentage|times|ms|s|sec|secs|seconds?|minutes?|hours?|
           days?|weeks?|months?|years?|repos?|repositories|commits?|files?|tests?|
           errors?|requests?|users?|tokens?|chars?|characters?|lines?)\b
        |
        \b(?:zero|one|two|three|four|five|six|seven|eight|nine|ten)\s+
        (?:percent|times|seconds?|minutes?|hours?|days?|weeks?|months?|years?|
           repos?|commits?|files?|tests?|errors?|requests?|users?|tokens?|lines?)\b
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

_FACTUAL_VERB_RE = re.compile(
    r"\b(?:is|are|was|were|uses?|requires?|supports?|ships?|adds?|added|removes?|"
    r"removed|changed|changes|launched|released|deprecated|replaced|introduced|"
    r"enables?|blocks?|fails?|fixed|fixes)\b",
    re.IGNORECASE,
)

_PROPER_TERM_RE = re.compile(
    r"\b(?:[A-Z][A-Za-z0-9.+#-]{2,}|[A-Z]{2,}|[a-z]+[A-Z][A-Za-z0-9.+#-]*)\b"
)

_WORD_RE = re.compile(r"[a-z0-9][a-z0-9.+#-]*")

_STOPWORDS = {
    "about",
    "after",
    "again",
    "against",
    "also",
    "and",
    "any",
    "are",
    "because",
    "been",
    "before",
    "being",
    "between",
    "built",
    "but",
    "can",
    "changed",
    "code",
    "commit",
    "commits",
    "day",
    "debugging",
    "does",
    "done",
    "during",
    "each",
    "everything",
    "from",
    "had",
    "has",
    "have",
    "hours",
    "into",
    "just",
    "less",
    "more",
    "most",
    "much",
    "need",
    "new",
    "not",
    "now",
    "only",
    "over",
    "post",
    "same",
    "than",
    "that",
    "the",
    "then",
    "there",
    "this",
    "through",
    "today",
    "using",
    "when",
    "where",
    "with",
    "work",
    "worked",
    "without",
    "you",
}


@dataclass
class Claim:
    """A risky claim extracted from generated text."""

    text: str
    kind: str
    terms: list[str]
    matched_terms: list[str] = field(default_factory=list)
    reason: str = ""


@dataclass
class ClaimCheckResult:
    """Result of checking generated text against source evidence."""

    claims: list[Claim]
    unsupported_claims: list[Claim]
    annotations: list[str]

    @property
    def supported(self) -> bool:
        return not self.unsupported_claims


class ClaimChecker:
    """Detect risky quantitative or factual claims and verify source support."""

    def check(
        self,
        text: str,
        source_prompts: list[str] | None = None,
        source_commits: list[str] | None = None,
        linked_knowledge: list[str] | None = None,
    ) -> ClaimCheckResult:
        evidence_parts = (source_prompts or []) + (source_commits or []) + (linked_knowledge or [])
        evidence = "\n".join(str(part) for part in evidence_parts if part)
        evidence_norm = self._normalize(evidence)

        claims = self.extract_claims(text)
        unsupported = []
        annotations = []
        for claim in claims:
            supported, matched_terms, reason = self._claim_supported(claim, evidence_norm)
            claim.matched_terms = matched_terms
            claim.reason = reason
            if not supported:
                unsupported.append(claim)
                annotations.append(f"{claim.kind}: {claim.text} ({reason})")

        return ClaimCheckResult(
            claims=claims,
            unsupported_claims=unsupported,
            annotations=annotations,
        )

    def extract_claims(self, text: str) -> list[Claim]:
        claims = []
        for sentence in self._sentences(text):
            number_terms = self._numeric_terms(sentence)
            if number_terms:
                claims.append(
                    Claim(
                        text=sentence,
                        kind="metric",
                        terms=number_terms + self._keywords(sentence, exclude=number_terms),
                    )
                )
                continue

            factual_terms = self._factual_terms(sentence)
            if factual_terms:
                claims.append(
                    Claim(
                        text=sentence,
                        kind="factual",
                        terms=factual_terms,
                    )
                )
        return claims

    def _claim_supported(
        self, claim: Claim, evidence_norm: str
    ) -> tuple[bool, list[str], str]:
        if not evidence_norm:
            return False, [], "no source evidence"

        matched_terms = [term for term in claim.terms if self._term_in_evidence(term, evidence_norm)]

        if claim.kind == "metric":
            metric_terms = [term for term in claim.terms if self._looks_numeric(term)]
            matched_metrics = [
                term for term in metric_terms if self._term_in_evidence(term, evidence_norm)
            ]
            support_terms = [
                term for term in matched_terms if term not in metric_terms and len(term) >= 4
            ]
            if matched_metrics and support_terms:
                return True, matched_terms, ""
            if not matched_metrics:
                return False, matched_terms, "metric value not found in sources"
            return False, matched_terms, "metric context not found in sources"

        if len(claim.terms) == 1:
            if matched_terms:
                return True, matched_terms, ""
            return False, matched_terms, "factual term not found in sources"

        required = max(1, min(len(claim.terms), 2))
        if len(matched_terms) >= required:
            return True, matched_terms, ""
        return False, matched_terms, "factual terms not found in sources"

    def _sentences(self, text: str) -> list[str]:
        clean = re.sub(r"^TWEET\s+\d+:\s*", "", text, flags=re.IGNORECASE | re.MULTILINE)
        parts = re.split(r"(?<=[.!?])\s+|\n+", clean)
        return [part.strip(" -\t") for part in parts if part.strip(" -\t")]

    def _numeric_terms(self, sentence: str) -> list[str]:
        return [self._normalize_term(m.group(0)) for m in _NUMBER_RE.finditer(sentence)]

    def _factual_terms(self, sentence: str) -> list[str]:
        if not _FACTUAL_VERB_RE.search(sentence):
            return []

        terms = []
        for match in _PROPER_TERM_RE.finditer(sentence):
            term = match.group(0)
            if match.start() == 0 and term.lower() in {"this", "that", "today"}:
                continue
            normalized = self._normalize_term(term)
            if normalized not in _STOPWORDS and normalized not in terms:
                terms.append(normalized)
        return terms

    def _keywords(self, sentence: str, exclude: list[str]) -> list[str]:
        excluded = {self._normalize_term(term) for term in exclude}
        keywords = []
        for word in _WORD_RE.findall(sentence.lower()):
            normalized = self._normalize_term(word)
            if (
                len(normalized) >= 4
                and normalized not in _STOPWORDS
                and normalized not in excluded
                and normalized not in keywords
                and not normalized.isdigit()
            ):
                keywords.append(normalized)
        return keywords[:6]

    def _term_in_evidence(self, term: str, evidence_norm: str) -> bool:
        normalized = self._normalize_term(term)
        if not normalized:
            return False
        return re.search(rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])", evidence_norm) is not None

    def _looks_numeric(self, term: str) -> bool:
        return bool(re.search(r"\d|zero|one|two|three|four|five|six|seven|eight|nine|ten", term))

    def _normalize(self, text: str) -> str:
        normalized = text.lower()
        normalized = normalized.replace(" percent", "%")
        normalized = normalized.replace(" percentage", "%")
        normalized = re.sub(r"\$\s+", "$", normalized)
        normalized = re.sub(r"[^a-z0-9.+#%$-]+", " ", normalized)
        return re.sub(r"\s+", " ", normalized).strip()

    def _normalize_term(self, term: str) -> str:
        normalized = term.lower().strip()
        normalized = normalized.replace(",", "")
        normalized = re.sub(r"\s+", " ", normalized)
        normalized = normalized.replace(" percent", "%")
        normalized = normalized.replace(" percentage", "%")
        normalized = re.sub(r"\s+%", "%", normalized)
        normalized = re.sub(r"\$\s+", "$", normalized)
        return normalized.strip()
