"""Flag generated content with dense factual-claim-like sentences."""
from __future__ import annotations

import re
from typing import Any

from ._batch_report_common import clean, empty_state, json_dumps, now_value, positive

ARTIFACT_TYPE = "generated_content_claim_density"
DEFAULT_LIMIT = 50
DEFAULT_MIN_DENSITY = 4.0
_WORD_RE = re.compile(r"[A-Za-z0-9]+(?:[-'][A-Za-z0-9]+)?")
_SENTENCE_RE = re.compile(r"[^.!?]+[.!?]?")
_CLAIM_RE = re.compile(r"\b(\d+(?:\.\d+)?%?|in \d{4}|because|according to|research|study|data|increased|decreased|launched|released|supports|shows)\b", re.I)


def build_generated_content_claim_density_report(rows: list[dict[str, Any]], *, min_density: float = DEFAULT_MIN_DENSITY, limit: int = DEFAULT_LIMIT, now: Any = None) -> dict[str, Any]:
    positive("limit", limit)
    findings = []
    for row in rows:
        text = clean(row.get("content") or row.get("body") or row.get("text") or row.get("draft_text"))
        words = _WORD_RE.findall(text)
        claims = [s.strip() for s in _SENTENCE_RE.findall(text) if _CLAIM_RE.search(s)]
        density = round(len(claims) / max(1, len(words)) * 100, 4) if words else 0.0
        if words and density >= min_density:
            findings.append({"content_id": clean(row.get("content_id") or row.get("id")), "content_type": clean(row.get("content_type") or row.get("type"), "unknown"), "claim_count": len(claims), "word_count": len(words), "claim_density": density, "sample_claims": claims[:3]})
    findings.sort(key=lambda f: (-f["claim_density"], f["content_id"]))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_value(now).isoformat(), "filters": {"min_density": min_density, "limit": limit}, "summary": {"content_count": len(rows), "finding_count": len(findings), "shown_count": len(shown)}, "findings": shown, "empty_state": empty_state(findings, "No high claim-density content found.")}


def format_generated_content_claim_density_json(report: dict[str, Any]) -> str:
    return json_dumps(report)
