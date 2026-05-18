"""Measure whether retrieved knowledge influences generated content."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import re
from typing import Any
from urllib.parse import urlparse


DEFAULT_LIMIT = 50
DEFAULT_MIN_UNUSED_GENERATIONS = 2
_WORD_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = {
    "about",
    "after",
    "also",
    "because",
    "before",
    "being",
    "from",
    "have",
    "into",
    "more",
    "over",
    "that",
    "their",
    "there",
    "these",
    "this",
    "with",
    "would",
}


def build_knowledge_citation_yield_report(
    retrieval_rows: list[dict[str, Any]],
    output_rows: list[dict[str, Any]] | None = None,
    *,
    min_unused_generations: int = DEFAULT_MIN_UNUSED_GENERATIONS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if min_unused_generations <= 0:
        raise ValueError("min_unused_generations must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    outputs = _index_outputs(output_rows or [])
    evaluated = []
    for row in retrieval_rows:
        generation_id = _text(_first(row, "generation_id", "content_id", "candidate_id", "output_id"))
        output_text = _text(_first(row, "output_text", "generated_text", "post_text", "candidate_text"))
        if not output_text and generation_id:
            output_text = outputs.get(generation_id, "")
        item = _evaluate_retrieval(row, generation_id, output_text)
        evaluated.append(item)

    groups = _aggregate(evaluated)
    flagged = _flag_repeated_unused(evaluated, min_unused_generations=min_unused_generations, limit=limit)
    return {
        "artifact_type": "knowledge_citation_yield",
        "generated_at": generated_at.isoformat(),
        "summary": {
            "retrieval_rows": len(evaluated),
            "used_count": sum(1 for item in evaluated if item["used"]),
            "unused_count": sum(1 for item in evaluated if not item["used"]),
            "citation_yield_rate": _rate(sum(1 for item in evaluated if item["used"]), len(evaluated)),
            "output_rows": len(output_rows or []),
        },
        "by_domain_and_format": groups,
        "flagged_repeatedly_unused": flagged,
    }


def format_knowledge_citation_yield_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_citation_yield_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Knowledge Citation Yield",
        f"Generated: {report['generated_at']}",
        f"Totals: retrievals={summary['retrieval_rows']} used={summary['used_count']} unused={summary['unused_count']} yield={summary['citation_yield_rate']}",
    ]
    if report["by_domain_and_format"]:
        lines.extend(["", "Yield by domain and format:"])
        for group in report["by_domain_and_format"]:
            lines.append(
                f"  - domain={group['domain']} format={group['format']} "
                f"used={group['used_count']} unused={group['unused_count']} yield={group['citation_yield_rate']}"
            )
    if report["flagged_repeatedly_unused"]:
        lines.extend(["", "Repeatedly unused retrievals:"])
        for item in report["flagged_repeatedly_unused"]:
            lines.append(
                f"  - domain={item['domain']} generations={item['unused_generations']} "
                f"source={item['source_url'] or item['citation'] or item['snippet_preview']}"
            )
    return "\n".join(lines)


def _evaluate_retrieval(row: dict[str, Any], generation_id: str, output_text: str) -> dict[str, Any]:
    source_url = _text(_first(row, "source_url", "url", "canonical_url", "citation_url"))
    domain = _domain(_first(row, "source_domain", "domain", "host", "source_url", "url", "canonical_url", "citation_url"))
    fmt = _text(_first(row, "format", "content_format", "output_format", "channel")) or "unknown"
    snippet = _text(_first(row, "snippet", "text", "excerpt", "quote", "source_text"))
    citation = _text(_first(row, "citation", "citation_label", "source_title", "title"))
    used, match_reason = _matches_output(output_text, source_url=source_url, domain=domain, snippet=snippet, citation=citation)
    return {
        "generation_id": generation_id or "unknown",
        "retrieval_id": _text(_first(row, "retrieval_id", "knowledge_id", "source_id", "id")) or _identity(source_url, snippet, citation),
        "domain": domain or "unknown",
        "format": fmt,
        "source_url": source_url,
        "citation": citation,
        "snippet_preview": snippet[:120],
        "used": used,
        "match_reason": match_reason,
    }


def _matches_output(output_text: str, *, source_url: str, domain: str, snippet: str, citation: str) -> tuple[bool, str | None]:
    haystack = output_text.lower()
    if not haystack:
        return False, None
    if source_url and source_url.lower() in haystack:
        return True, "url"
    if domain and domain != "unknown" and domain.lower() in haystack:
        return True, "domain"
    if citation and len(citation) >= 6 and citation.lower() in haystack:
        return True, "citation"
    if snippet and _snippet_overlap(snippet, output_text):
        return True, "snippet_overlap"
    return False, None


def _snippet_overlap(snippet: str, output_text: str) -> bool:
    snippet_tokens = _tokens(snippet)
    output_tokens = set(_tokens(output_text))
    if len(snippet_tokens) < 6:
        return False
    shared = sum(1 for token in set(snippet_tokens) if token in output_tokens)
    return shared >= 5 and shared / max(1, len(set(snippet_tokens))) >= 0.45


def _aggregate(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[(item["domain"], item["format"])].append(item)
    groups = []
    for (domain, fmt), group_items in grouped.items():
        used = sum(1 for item in group_items if item["used"])
        groups.append(
            {
                "domain": domain,
                "format": fmt,
                "used_count": used,
                "unused_count": len(group_items) - used,
                "citation_yield_rate": _rate(used, len(group_items)),
            }
        )
    groups.sort(key=lambda group: (-group["unused_count"], group["domain"], group["format"]))
    return groups


def _flag_repeated_unused(items: list[dict[str, Any]], *, min_unused_generations: int, limit: int) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        if not item["used"]:
            grouped[_identity(item["source_url"], item["snippet_preview"], item["citation"])].append(item)
    flagged = []
    for identity, group_items in grouped.items():
        generation_ids = sorted({item["generation_id"] for item in group_items})
        if len(generation_ids) >= min_unused_generations:
            first = group_items[0]
            flagged.append(
                {
                    "retrieval_key": identity,
                    "domain": first["domain"],
                    "format_counts": dict(sorted(_count(item["format"] for item in group_items).items())),
                    "unused_count": len(group_items),
                    "unused_generations": generation_ids,
                    "source_url": first["source_url"],
                    "citation": first["citation"],
                    "snippet_preview": first["snippet_preview"],
                }
            )
    flagged.sort(key=lambda item: (-item["unused_count"], item["domain"], item["retrieval_key"]))
    return flagged[:limit]


def _index_outputs(rows: list[dict[str, Any]]) -> dict[str, str]:
    outputs = {}
    for row in rows:
        generation_id = _text(_first(row, "generation_id", "content_id", "candidate_id", "output_id", "id"))
        text = _text(_first(row, "output_text", "generated_text", "post_text", "candidate_text", "text", "body"))
        if generation_id:
            outputs[generation_id] = text
    return outputs


def _tokens(text: str) -> list[str]:
    return [token for token in _WORD_RE.findall(text.lower()) if len(token) > 3 and token not in _STOPWORDS]


def _domain(value: Any) -> str:
    text = _text(value)
    if not text:
        return "unknown"
    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = parsed.netloc or parsed.path.split("/")[0]
    return host.lower().removeprefix("www.") or "unknown"


def _identity(source_url: str, snippet: str, citation: str) -> str:
    return source_url or citation or " ".join(_tokens(snippet)[:12]) or "unknown"


def _count(values: Any) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for value in values:
        counts[value] += 1
    return counts


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""
