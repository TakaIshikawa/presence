"""Flag blog drafts with generic or weak opening hooks."""
from __future__ import annotations

import re
from typing import Any

from ._batch_report_common import clean, empty_state, json_dumps, now_value, positive

ARTIFACT_TYPE = "blog_draft_intro_hook_weakness"
DEFAULT_LIMIT = 50
_GENERIC = (("generic_framing", re.compile(r"^(in today's|in this post|this article|as we all know|recently,?\s+we)", re.I)), ("abstract_opening", re.compile(r"\b(important|exciting|interesting|ever-changing|fast-paced)\b", re.I)))
_CONCRETE = re.compile(r"\b(api|prototype|dashboard|migration|release|bug|incident|dataset|benchmark|workflow|component|report|artifact|problem|customer|latency|error)\b", re.I)


def build_blog_draft_intro_hook_weakness_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, now: Any = None) -> dict[str, Any]:
    positive("limit", limit)
    findings = []
    for row in rows:
        intro = _intro(clean(row.get("intro") or row.get("body") or row.get("content") or row.get("draft_text")))
        reasons = [code for code, pattern in _GENERIC if pattern.search(intro)]
        if intro and not _CONCRETE.search(intro):
            reasons.append("missing_concrete_artifact")
        if not intro:
            reasons.append("missing_intro")
        if intro and len(intro.split()) < 8:
            reasons.append("too_short")
        if reasons:
            severity = "high" if "missing_intro" in reasons or len(reasons) > 1 else "medium"
            findings.append({"draft_id": clean(row.get("draft_id") or row.get("id")), "reason_codes": sorted(set(reasons)), "intro_excerpt": intro[:180], "severity": severity})
    findings.sort(key=lambda f: ({"high": 0, "medium": 1}.get(f["severity"], 2), f["draft_id"]))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_value(now).isoformat(), "summary": {"draft_count": len(rows), "finding_count": len(findings)}, "findings": findings[:limit], "empty_state": empty_state(findings, "No weak blog intro hooks found.")}


def format_blog_draft_intro_hook_weakness_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def _intro(text: str) -> str:
    return next((p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()), "")
