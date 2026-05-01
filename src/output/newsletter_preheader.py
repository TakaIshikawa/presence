"""Deterministic newsletter preheader generation and selection."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any


DEFAULT_MIN_LENGTH = 45
DEFAULT_MAX_LENGTH = 100

GENERIC_PHRASES = {
    "click here",
    "don't miss",
    "dont miss",
    "exciting update",
    "learn more",
    "latest news",
    "read more",
    "stay tuned",
    "this week",
    "weekly update",
}

CLICK_CLARITY_WORDS = {
    "build",
    "compare",
    "discover",
    "fix",
    "learn",
    "read",
    "review",
    "see",
    "ship",
    "try",
    "use",
    "watch",
}


@dataclass(frozen=True)
class PreheaderCandidate:
    """One generated preheader candidate with scoring diagnostics."""

    text: str
    source: str
    score: float
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable representation."""
        return {
            "diagnostics": self.diagnostics,
            "score": self.score,
            "source": self.source,
            "text": self.text,
        }


@dataclass(frozen=True)
class NewsletterPreheaderSelection:
    """Selected preheader plus deterministic candidate diagnostics."""

    selected: PreheaderCandidate
    candidates: tuple[PreheaderCandidate, ...]
    subject: str
    min_length: int
    max_length: int

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable selection payload."""
        return {
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "max_length": self.max_length,
            "min_length": self.min_length,
            "selected": self.selected.to_dict(),
            "subject": self.subject,
        }


def select_newsletter_preheader(
    payload: dict[str, Any] | str,
    *,
    min_length: int = DEFAULT_MIN_LENGTH,
    max_length: int = DEFAULT_MAX_LENGTH,
) -> NewsletterPreheaderSelection:
    """Generate and select the best newsletter preheader for a payload."""
    candidates = generate_preheader_candidates(
        payload,
        min_length=min_length,
        max_length=max_length,
    )
    if not candidates:
        raise ValueError("no preheader candidates could be generated")
    selected = sorted(
        candidates,
        key=lambda candidate: (
            -candidate.score,
            candidate.diagnostics["length_delta"],
            candidate.text.lower(),
            candidate.source,
        ),
    )[0]
    fields = extract_newsletter_fields(payload)
    return NewsletterPreheaderSelection(
        selected=selected,
        candidates=tuple(candidates),
        subject=fields["subject"],
        min_length=min_length,
        max_length=max_length,
    )


def generate_preheader_candidates(
    payload: dict[str, Any] | str,
    *,
    min_length: int = DEFAULT_MIN_LENGTH,
    max_length: int = DEFAULT_MAX_LENGTH,
) -> list[PreheaderCandidate]:
    """Build scored candidates from structured fields or markdown drafts."""
    _validate_bounds(min_length, max_length)
    fields = extract_newsletter_fields(payload)
    subject = fields["subject"]
    raw_candidates: list[tuple[str, str]] = []

    title = fields["title"]
    sections = fields["sections"]
    links = fields["top_links"]
    cta = fields["cta"]
    freshness = fields["source_freshness"]

    if sections:
        top = sections[0]
        top_text = _first_text(top, "summary", "lede", "body", "text", "content")
        heading = _first_text(top, "title", "heading", "name")
        if top_text and heading:
            raw_candidates.append((f"{heading}: {top_text}", "section_summary"))
        if top_text:
            raw_candidates.append((top_text, "section_body"))

    if len(sections) >= 2:
        headings = [
            _first_text(section, "title", "heading", "name")
            for section in sections[:3]
        ]
        headings = [heading for heading in headings if heading]
        if len(headings) >= 2:
            raw_candidates.append(
                ("Inside: " + ", ".join(headings) + ".", "section_roundup")
            )

    if links:
        labels = [_first_text(link, "label", "title", "text") for link in links[:2]]
        labels = [label for label in labels if label]
        if labels:
            raw_candidates.append(
                ("Read the top link: " + labels[0] + ".", "top_link")
            )
        if len(labels) >= 2:
            raw_candidates.append(
                ("Compare " + labels[0] + " with " + labels[1] + ".", "top_links")
            )

    if cta:
        raw_candidates.append((f"{cta} after the main notes.", "cta"))

    freshness_text = _freshness_candidate(freshness)
    if freshness_text:
        raw_candidates.append((freshness_text, "source_freshness"))

    body = fields["body_markdown"]
    excerpt = _first_markdown_sentence(body)
    if excerpt:
        raw_candidates.append((excerpt, "markdown_excerpt"))

    if title and excerpt and _normalize_text(title) not in _normalize_text(excerpt):
        raw_candidates.append((f"{title}: {excerpt}", "title_excerpt"))

    seen: set[str] = set()
    scored: list[PreheaderCandidate] = []
    for text, source in raw_candidates:
        fitted = _fit_to_max(_clean_text(text), max_length)
        key = _normalize_text(fitted)
        if not fitted or key in seen:
            continue
        seen.add(key)
        candidate = score_preheader_candidate(
            fitted,
            subject=subject,
            source=source,
            min_length=min_length,
            max_length=max_length,
        )
        if len(candidate.text) >= min_length:
            scored.append(candidate)

    return sorted(
        scored,
        key=lambda candidate: (
            -candidate.score,
            candidate.diagnostics["length_delta"],
            candidate.text.lower(),
            candidate.source,
        ),
    )


def score_preheader_candidate(
    text: str,
    *,
    subject: str = "",
    source: str = "manual",
    min_length: int = DEFAULT_MIN_LENGTH,
    max_length: int = DEFAULT_MAX_LENGTH,
) -> PreheaderCandidate:
    """Score one candidate for fit, specificity, repetition, and click clarity."""
    _validate_bounds(min_length, max_length)
    cleaned = _fit_to_max(_clean_text(text), max_length)
    length = len(cleaned)
    target = (min_length + max_length) / 2
    length_delta = abs(length - target)
    length_score = max(0.0, 25.0 - (length_delta / max(target, 1) * 25.0))
    if length < min_length or length > max_length:
        length_score -= 20.0

    words = _tokens(cleaned)
    subject_words = set(_tokens(subject))
    unique_words = {word for word in words if len(word) > 3}
    specificity_score = min(25.0, len(unique_words) * 2.2)
    if re.search(r"\d|%|\$|[A-Z][a-z]+ [A-Z][a-z]+", cleaned):
        specificity_score += 4.0
    specificity_score = min(30.0, specificity_score)

    normalized = _normalize_text(cleaned)
    generic_hits = sorted(phrase for phrase in GENERIC_PHRASES if phrase in normalized)
    repeated_subject_words = sorted(unique_words.intersection(subject_words))
    subject_similarity = _jaccard(set(words), subject_words)
    repetition_penalty = len(repeated_subject_words) * 1.5
    if subject and (
        normalized == _normalize_text(subject)
        or normalized in _normalize_text(subject)
        or _normalize_text(subject) in normalized
    ):
        repetition_penalty += 35.0
    if subject_similarity >= 0.55:
        repetition_penalty += 20.0 * subject_similarity

    generic_penalty = len(generic_hits) * 8.0
    clarity_hits = sorted(set(words).intersection(CLICK_CLARITY_WORDS))
    clarity_score = min(20.0, 8.0 + len(clarity_hits) * 4.0)
    if "?" in cleaned:
        clarity_score += 2.0
    if not clarity_hits:
        clarity_score -= 3.0

    score = round(
        length_score
        + specificity_score
        + clarity_score
        - repetition_penalty
        - generic_penalty,
        2,
    )
    return PreheaderCandidate(
        text=cleaned,
        source=source,
        score=score,
        diagnostics={
            "clarity_hits": clarity_hits,
            "generic_hits": generic_hits,
            "length": length,
            "length_delta": round(length_delta, 2),
            "length_score": round(length_score, 2),
            "repeated_subject_words": repeated_subject_words,
            "repetition_penalty": round(repetition_penalty, 2),
            "specificity_score": round(specificity_score, 2),
        },
    )


def extract_newsletter_fields(payload: dict[str, Any] | str) -> dict[str, Any]:
    """Normalize structured newsletter fields and plain markdown drafts."""
    if isinstance(payload, str):
        return extract_newsletter_fields_from_markdown(payload)
    if not isinstance(payload, dict):
        raise ValueError("newsletter payload must be an object or markdown string")

    body = str(
        payload.get("body_markdown")
        or payload.get("markdown")
        or payload.get("body")
        or payload.get("content")
        or ""
    )
    sections = _normalize_sections(payload.get("sections") or payload.get("items") or [])
    links = _normalize_links(
        payload.get("top_links")
        or payload.get("outbound_links")
        or payload.get("links")
        or []
    )
    subject = str(
        payload.get("subject")
        or payload.get("selected_subject")
        or payload.get("email_subject")
        or ""
    ).strip()
    title = str(payload.get("title") or payload.get("headline") or "").strip()
    cta = payload.get("cta") or payload.get("call_to_action") or ""
    if isinstance(cta, dict):
        cta = _first_text(cta, "text", "label", "title")

    if body:
        markdown_fields = extract_newsletter_fields_from_markdown(body)
        if not title:
            title = markdown_fields["title"]
        if not subject:
            subject = markdown_fields["subject"]
        if not sections:
            sections = markdown_fields["sections"]
        if not links:
            links = markdown_fields["top_links"]

    return {
        "body_markdown": body,
        "cta": str(cta or "").strip(),
        "sections": sections,
        "source_freshness": payload.get("source_freshness")
        or payload.get("source_freshness_metadata")
        or payload.get("freshness")
        or {},
        "subject": subject,
        "title": title or subject,
        "top_links": links,
    }


def extract_newsletter_fields_from_markdown(markdown: str) -> dict[str, Any]:
    """Extract newsletter fields from a plain markdown draft."""
    text = markdown or ""
    subject = ""
    subject_match = re.search(r"(?im)^\s*subject\s*:\s*(.+)$", text)
    if subject_match:
        subject = _clean_text(subject_match.group(1))

    headings = [
        (match.group(1), _clean_text(match.group(2)))
        for match in re.finditer(r"(?m)^(#{1,3})\s+(.+)$", text)
    ]
    title = headings[0][1] if headings else subject
    sections: list[dict[str, str]] = []
    if headings:
        matches = list(re.finditer(r"(?m)^(#{1,3})\s+(.+)$", text))
        for index, match in enumerate(matches):
            start = match.end()
            end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            body = _first_markdown_sentence(text[start:end])
            heading = _clean_text(match.group(2))
            if body:
                sections.append({"heading": heading, "summary": body})

    links = [
        {"label": _clean_text(match.group(1)), "url": match.group(2).strip()}
        for match in re.finditer(r"\[([^\]]+)\]\(([^)\s]+)\)", text)
        if match.group(2).strip() and not match.group(2).strip().startswith("#")
    ]
    cta = ""
    for line in text.splitlines():
        cleaned = _clean_text(line)
        if re.search(r"\b(read|try|join|subscribe|download|watch)\b", cleaned, re.I):
            cta = cleaned
            break

    return {
        "body_markdown": text,
        "cta": cta,
        "sections": sections,
        "source_freshness": {},
        "subject": subject,
        "title": title,
        "top_links": links,
    }


def format_preheader_selection_json(selection: NewsletterPreheaderSelection) -> str:
    """Render a preheader selection as stable JSON."""
    return json.dumps(selection.to_dict(), indent=2, sort_keys=True) + "\n"


def format_preheader_selection_text(selection: NewsletterPreheaderSelection) -> str:
    """Render a compact operator-facing preheader report."""
    selected = selection.selected
    lines = [
        f"Selected preheader: {selected.text}",
        f"Score: {selected.score:.2f}",
        f"Source: {selected.source}",
        f"Length: {len(selected.text)} ({selection.min_length}-{selection.max_length})",
        "Candidates:",
    ]
    for candidate in selection.candidates:
        lines.append(
            f"- {candidate.score:.2f} {candidate.source}: {candidate.text}"
        )
    return "\n".join(lines)


def _normalize_sections(raw_sections: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_sections, list):
        return []
    sections = []
    for raw in raw_sections:
        if isinstance(raw, dict):
            sections.append(dict(raw))
        elif raw:
            sections.append({"text": str(raw)})
    return sections


def _normalize_links(raw_links: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_links, list):
        return []
    links = []
    for raw in raw_links:
        if isinstance(raw, dict):
            links.append(dict(raw))
        elif raw:
            links.append({"label": str(raw)})
    return links


def _freshness_candidate(freshness: Any) -> str:
    if not isinstance(freshness, dict):
        return ""
    fresh_count = freshness.get("fresh_source_count") or freshness.get("fresh_sources")
    newest = freshness.get("newest_source_at") or freshness.get("newest_at")
    stale_count = freshness.get("stale_source_count") or freshness.get("stale_sources")
    parts = []
    if fresh_count:
        parts.append(f"{fresh_count} fresh source notes")
    if newest:
        parts.append(f"newest from {newest}")
    if stale_count:
        parts.append(f"{stale_count} stale source flagged")
    if not parts:
        return ""
    return "Source check: " + ", ".join(parts) + "."


def _first_text(mapping: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, str) and value.strip():
            return _clean_text(value)
    return ""


def _first_markdown_sentence(markdown: str) -> str:
    cleaned_lines = []
    for line in (markdown or "").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.lower().startswith("subject:"):
            continue
        stripped = re.sub(r"^[-*]\s+", "", stripped)
        cleaned_lines.append(stripped)
    text = _clean_text(" ".join(cleaned_lines))
    if not text:
        return ""
    match = re.search(r"^(.+?[.!?])(?:\s|$)", text)
    return match.group(1) if match else text


def _fit_to_max(text: str, max_length: int) -> str:
    if len(text) <= max_length:
        return text
    truncated = text[: max_length + 1]
    if " " in truncated:
        truncated = truncated[: truncated.rfind(" ")]
    return truncated.rstrip(" ,;:-.")


def _clean_text(value: str) -> str:
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", str(value or ""))
    text = re.sub(r"[*_`>#]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" \t\r\n-")


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _tokens(value: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", (value or "").lower())


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left.intersection(right)) / len(left.union(right))


def _validate_bounds(min_length: int, max_length: int) -> None:
    if min_length < 0:
        raise ValueError("min_length must be non-negative")
    if max_length <= 0:
        raise ValueError("max_length must be positive")
    if min_length > max_length:
        raise ValueError("min_length must be less than or equal to max_length")
