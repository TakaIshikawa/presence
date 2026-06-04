"""Flag reply drafts that overuse relationship-context details."""
from __future__ import annotations

import re
from typing import Any

from ._batch_report_common import clean, empty_state, json_dumps, now_value, positive

ARTIFACT_TYPE = "reply_draft_relationship_context_overuse"
DEFAULT_LIMIT = 50
DEFAULT_MAX_CONTEXT_REFERENCES = 1


def build_reply_draft_relationship_context_overuse_report(rows: list[dict[str, Any]], *, max_context_references: int = DEFAULT_MAX_CONTEXT_REFERENCES, limit: int = DEFAULT_LIMIT, now: Any = None) -> dict[str, Any]:
    positive("limit", limit)
    findings = []
    for row in rows:
        text = clean(row.get("draft_text") or row.get("text") or row.get("body"))
        terms = _terms(row.get("relationship_context_terms") or row.get("context_terms") or row.get("terms"))
        matched = []
        count = 0
        for term in terms:
            matches = re.findall(rf"\b{re.escape(term)}\b", text, flags=re.I)
            if matches:
                matched.append(term)
                count += len(matches)
        if count > max_context_references:
            findings.append({"draft_id": clean(row.get("draft_id") or row.get("id")), "matched_context_terms": matched, "context_reference_count": count, "excerpt": text[:180]})
    findings.sort(key=lambda f: (-f["context_reference_count"], f["draft_id"]))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_value(now).isoformat(), "filters": {"max_context_references": max_context_references, "limit": limit}, "summary": {"draft_count": len(rows), "finding_count": len(findings)}, "findings": findings[:limit], "empty_state": empty_state(findings, "No reply draft relationship-context overuse found.")}


def format_reply_draft_relationship_context_overuse_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def _terms(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [clean(v).lower() for v in value if clean(v)]
    return [part.strip().lower() for part in re.split(r"[,|]", clean(value)) if part.strip()]
