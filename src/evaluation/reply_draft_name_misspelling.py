"""Flag likely misspelled recipient names in reply drafts."""
from __future__ import annotations

import re
from typing import Any

from ._batch_report_common import clean, empty_state, json_dumps, now_value, positive

ARTIFACT_TYPE = "reply_draft_name_misspelling"
DEFAULT_LIMIT = 50


def build_reply_draft_name_misspelling_report(rows: list[dict[str, Any]], *, max_edit_distance: int = 2, limit: int = DEFAULT_LIMIT, now: Any = None) -> dict[str, Any]:
    positive("limit", limit)
    findings = []
    for row in rows:
        name = clean(row.get("recipient_name") or row.get("expected_name") or row.get("name"))
        handle = clean(row.get("handle") or row.get("recipient_handle")).lstrip("@")
        text = clean(row.get("draft_text") or row.get("text") or row.get("body"))
        expected = name or handle
        if not expected or not text:
            continue
        exact_terms = {name.lower(), handle.lower(), ("@" + handle).lower(), (name.split()[0].lower() if name else "")} - {""}
        if any(term and term in text.lower() for term in exact_terms):
            continue
        first = name.split()[0] if name else handle
        best = None
        for token in re.findall(r"@?[A-Z][A-Za-z]{1,}|@[A-Za-z0-9_]+", text):
            candidate = token.lstrip("@")
            dist = _edit(first.lower(), candidate.lower())
            if dist <= max_edit_distance and (best is None or dist < best[1]):
                best = (token, dist)
        if best:
            findings.append({"draft_id": clean(row.get("draft_id") or row.get("id")), "expected_name": expected, "matched_text": best[0], "edit_distance": best[1], "excerpt": _excerpt(text, best[0])})
    findings.sort(key=lambda f: (f["edit_distance"], f["draft_id"]))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_value(now).isoformat(), "filters": {"max_edit_distance": max_edit_distance, "limit": limit}, "summary": {"draft_count": len(rows), "finding_count": len(findings)}, "findings": findings[:limit], "empty_state": empty_state(findings, "No reply draft name misspellings found.")}


def format_reply_draft_name_misspelling_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def _edit(a: str, b: str) -> int:
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[-1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def _excerpt(text: str, match: str) -> str:
    idx = text.find(match)
    return text[max(0, idx - 30): idx + len(match) + 30] if idx >= 0 else text[:80]
