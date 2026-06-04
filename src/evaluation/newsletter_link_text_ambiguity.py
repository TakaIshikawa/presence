"""Flag ambiguous newsletter link anchor text."""
from __future__ import annotations

import re
from typing import Any

from ._batch_report_common import clean, empty_state, json_dumps, now_value, positive

ARTIFACT_TYPE = "newsletter_link_text_ambiguity"
DEFAULT_LIMIT = 50
GENERIC = {"here", "click here", "read more", "learn more", "link", "this"}
_A_RE = re.compile(r"<a\s+[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.I | re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_URL_RE = re.compile(r"^https?://", re.I)


def build_newsletter_link_text_ambiguity_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, now: Any = None) -> dict[str, Any]:
    positive("limit", limit)
    links = []
    for row in rows:
        newsletter_id = clean(row.get("newsletter_id") or row.get("issue_id"), "unknown")
        if clean(row.get("html")):
            links.extend({"newsletter_id": newsletter_id, "anchor_text": _strip(a), "destination_url": u} for u, a in _A_RE.findall(clean(row.get("html"))))
        else:
            links.append({"newsletter_id": newsletter_id, "anchor_text": clean(row.get("anchor_text") or row.get("text")), "destination_url": clean(row.get("destination_url") or row.get("url") or row.get("href"))})
    by_anchor: dict[tuple[str, str], set[str]] = {}
    counts: dict[tuple[str, str, str], int] = {}
    for link in links:
        anchor = clean(link["anchor_text"]).lower()
        by_anchor.setdefault((link["newsletter_id"], anchor), set()).add(link["destination_url"])
        key = (link["newsletter_id"], anchor, link["destination_url"])
        counts[key] = counts.get(key, 0) + 1
    findings = []
    for link in links:
        anchor = clean(link["anchor_text"])
        norm = anchor.lower()
        reason = "generic_anchor" if norm in GENERIC else "bare_url" if _URL_RE.match(anchor) else "duplicated_anchor_different_urls" if len(by_anchor.get((link["newsletter_id"], norm), set())) > 1 else ""
        if reason:
            findings.append({"newsletter_id": link["newsletter_id"], "anchor_text": anchor, "destination_url": link["destination_url"], "reason_code": reason, "occurrence_count": counts[(link["newsletter_id"], norm, link["destination_url"])]})
    findings.sort(key=lambda f: (f["newsletter_id"], f["reason_code"], f["anchor_text"], f["destination_url"]))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_value(now).isoformat(), "summary": {"link_count": len(links), "finding_count": len(findings)}, "findings": findings[:limit], "empty_state": empty_state(findings, "No ambiguous newsletter link text found.")}


def format_newsletter_link_text_ambiguity_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def _strip(text: str) -> str:
    return clean(_TAG_RE.sub("", text))
