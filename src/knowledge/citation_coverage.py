"""Score whether generated content claims are grounded in available evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from synthesis.claim_checker import Claim, ClaimChecker


EvidenceBucket = list[tuple[str, str]]


@dataclass
class ClaimCoverage:
    """Citation coverage for one extracted claim."""

    text: str
    kind: str
    status: str
    score: float
    reason: str
    matched_terms: list[str]
    evidence_types: list[str]


@dataclass
class ContentCoverage:
    """Citation coverage score for one generated content item."""

    content_id: int | None
    content_type: str | None
    status: str
    score: float
    claim_count: int
    covered_count: int
    thin_count: int
    missing_count: int
    citation_link_count: int
    missing_traceable_link_count: int
    reasons: list[str]
    claims: list[ClaimCoverage]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["claims"] = [asdict(claim) for claim in self.claims]
        return payload


class CitationCoverageScorer:
    """Deterministically score claim support from knowledge and provenance."""

    def __init__(self, checker: ClaimChecker | None = None) -> None:
        self.checker = checker or ClaimChecker()

    def score_content(self, content: dict[str, Any], provenance: dict[str, Any] | None = None) -> ContentCoverage:
        """Score one generated content row plus optional provenance bundle."""
        provenance = provenance or {}
        content_payload = provenance.get("content") or content
        text = str(content_payload.get("content") or content.get("content") or "")
        claims = self.checker.extract_claims(text)
        evidence = self._build_evidence(provenance)
        knowledge_links = list(provenance.get("knowledge_links") or [])
        missing_traceable = self._missing_traceable_links(knowledge_links)

        if not claims:
            reasons = ["no metric or factual claims detected"]
            if missing_traceable:
                reasons.append(f"{missing_traceable} linked source(s) lack traceable URL")
            score = 0.9 if missing_traceable else 1.0
            status = "thin" if missing_traceable else "covered"
            return ContentCoverage(
                content_id=content_payload.get("id"),
                content_type=content_payload.get("content_type"),
                status=status,
                score=score,
                claim_count=0,
                covered_count=0,
                thin_count=0,
                missing_count=0,
                citation_link_count=len(knowledge_links),
                missing_traceable_link_count=missing_traceable,
                reasons=reasons,
                claims=[],
            )

        claim_results = [self._score_claim(claim, evidence) for claim in claims]
        covered = sum(1 for claim in claim_results if claim.status == "covered")
        thin = sum(1 for claim in claim_results if claim.status == "thin")
        missing = sum(1 for claim in claim_results if claim.status == "missing")
        raw_score = sum(claim.score for claim in claim_results) / len(claim_results)
        if missing_traceable:
            raw_score = max(0.0, raw_score - min(0.15, missing_traceable * 0.05))

        status = self._status_for_counts(covered, thin, missing)
        if status == "covered" and missing_traceable:
            status = "thin"

        reasons = sorted({claim.reason for claim in claim_results if claim.reason})
        if missing_traceable:
            reasons.append(f"{missing_traceable} linked source(s) lack traceable URL")

        return ContentCoverage(
            content_id=content_payload.get("id"),
            content_type=content_payload.get("content_type"),
            status=status,
            score=round(raw_score, 3),
            claim_count=len(claim_results),
            covered_count=covered,
            thin_count=thin,
            missing_count=missing,
            citation_link_count=len(knowledge_links),
            missing_traceable_link_count=missing_traceable,
            reasons=reasons,
            claims=claim_results,
        )

    def score_provenance(self, provenance: dict[str, Any]) -> ContentCoverage:
        """Score a full provenance bundle returned by storage.db."""
        return self.score_content(provenance.get("content") or {}, provenance)

    def _score_claim(self, claim: Claim, evidence: dict[str, EvidenceBucket]) -> ClaimCoverage:
        matches: list[tuple[str, list[str], str]] = []
        partial_terms: set[str] = set()
        partial_types: set[str] = set()

        for evidence_type, bucket in evidence.items():
            evidence_norm = self.checker._normalize("\n".join(text for _label, text in bucket))
            supported, matched_terms, reason = self.checker._claim_supported(claim, evidence_norm)
            if supported:
                matches.append((evidence_type, matched_terms, reason))
            elif matched_terms:
                partial_terms.update(matched_terms)
                partial_types.add(evidence_type)

        if matches:
            matched_terms = sorted({term for _evidence_type, terms, _reason in matches for term in terms})
            evidence_types = sorted({evidence_type for evidence_type, _terms, _reason in matches})
            return ClaimCoverage(
                text=claim.text,
                kind=claim.kind,
                status="covered",
                score=1.0,
                reason="matched source evidence",
                matched_terms=matched_terms,
                evidence_types=evidence_types,
            )

        if partial_terms:
            return ClaimCoverage(
                text=claim.text,
                kind=claim.kind,
                status="thin",
                score=0.5,
                reason="only partial claim terms found in evidence",
                matched_terms=sorted(partial_terms),
                evidence_types=sorted(partial_types),
            )

        has_evidence = any(bucket for bucket in evidence.values())
        return ClaimCoverage(
            text=claim.text,
            kind=claim.kind,
            status="missing",
            score=0.0,
            reason="claim terms not found in evidence" if has_evidence else "no source evidence",
            matched_terms=[],
            evidence_types=[],
        )

    def _build_evidence(self, provenance: dict[str, Any]) -> dict[str, EvidenceBucket]:
        return {
            "curated_knowledge": self._knowledge_evidence(
                provenance.get("knowledge_links") or [],
                curated_only=True,
            ),
            "knowledge": self._knowledge_evidence(
                provenance.get("knowledge_links") or [],
                curated_only=False,
            ),
            "provenance": self._provenance_evidence(provenance),
        }

    def _knowledge_evidence(self, links: list[dict[str, Any]], *, curated_only: bool) -> EvidenceBucket:
        evidence = []
        for link in links:
            source_type = str(link.get("source_type") or "")
            is_curated = source_type.startswith("curated_")
            if curated_only and not is_curated:
                continue
            if not curated_only and is_curated:
                continue
            text = " ".join(
                str(value)
                for value in (
                    link.get("author"),
                    link.get("source_id"),
                    link.get("source_url"),
                    link.get("content"),
                    link.get("insight"),
                )
                if value
            )
            if text:
                evidence.append((f"knowledge:{link.get('id')}", text))
        return evidence

    def _provenance_evidence(self, provenance: dict[str, Any]) -> EvidenceBucket:
        evidence = []
        for commit in provenance.get("source_commits") or []:
            text = " ".join(str(commit.get(key) or "") for key in ("repo_name", "commit_sha", "commit_message"))
            if text.strip():
                evidence.append((f"commit:{commit.get('commit_sha')}", text))
        for message in provenance.get("source_messages") or []:
            text = str(message.get("prompt_text") or "")
            if text.strip():
                evidence.append((f"message:{message.get('message_uuid')}", text))
        for activity in provenance.get("source_activity") or []:
            text = " ".join(
                str(activity.get(key) or "")
                for key in ("repo_name", "activity_type", "number", "title", "body")
            )
            if text.strip():
                evidence.append((f"activity:{activity.get('activity_id')}", text))
        return evidence

    def _missing_traceable_links(self, links: list[dict[str, Any]]) -> int:
        count = 0
        for link in links:
            source_type = str(link.get("source_type") or "")
            if source_type in {"own_post", "own_conversation"}:
                continue
            if not str(link.get("source_url") or "").strip():
                count += 1
        return count

    def _status_for_counts(self, covered: int, thin: int, missing: int) -> str:
        if missing:
            return "missing"
        if thin:
            return "thin"
        if covered:
            return "covered"
        return "missing"

