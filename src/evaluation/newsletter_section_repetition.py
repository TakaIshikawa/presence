"""Flag repeated newsletter section titles and near-identical bodies."""
from __future__ import annotations

from itertools import combinations
from typing import Any

from ._batch_report_common import bounded_share, clean, empty_state, jaccard, json_dumps, now_value, positive, tokens

ARTIFACT_TYPE = "newsletter_section_repetition"
DEFAULT_LIMIT = 50
DEFAULT_SIMILARITY_THRESHOLD = 0.82


def build_newsletter_section_repetition_report(
    rows: list[dict[str, Any]],
    *,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: Any = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    bounded_share("similarity_threshold", similarity_threshold)
    positive("limit", limit)
    issues: list[dict[str, Any]] = []
    by_issue: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_issue.setdefault(clean(row.get("issue_id") or row.get("newsletter_id"), "unknown"), []).append(row)
    for issue_id, sections in by_issue.items():
        title_counts: dict[str, int] = {}
        title_display: dict[str, str] = {}
        max_similarity = 0.0
        representative_ids: set[str] = set()
        repeated_titles: set[str] = set()
        for section in sections:
            title = clean(section.get("section_title") or section.get("title") or section.get("heading"))
            norm = _norm(title)
            if norm:
                title_counts[norm] = title_counts.get(norm, 0) + 1
                title_display.setdefault(norm, title)
        for norm, count in title_counts.items():
            if count > 1:
                repeated_titles.add(title_display[norm])
                representative_ids.update(_section_id(s) for s in sections if _norm(clean(s.get("section_title") or s.get("title") or s.get("heading"))) == norm)
        for left, right in combinations(sections, 2):
            sim = _similarity(_body(left), _body(right))
            max_similarity = max(max_similarity, sim)
            if sim >= similarity_threshold:
                representative_ids.update([_section_id(left), _section_id(right)])
        if repeated_titles or max_similarity >= similarity_threshold:
            issues.append(
                {
                    "issue_id": issue_id,
                    "repeated_section_title": sorted(repeated_titles)[0] if repeated_titles else "",
                    "section_count": len(sections),
                    "max_similarity": round(max_similarity, 4),
                    "representative_section_ids": sorted(i for i in representative_ids if i),
                }
            )
    issues.sort(key=lambda i: (-i["max_similarity"], i["issue_id"]))
    shown = issues[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": now_value(now).isoformat(),
        "filters": {"similarity_threshold": similarity_threshold, "limit": limit},
        "summary": {"issue_count": len(by_issue), "flagged_issue_count": len(issues), "shown_count": len(shown)},
        "issues": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v},
        "empty_state": empty_state(issues, "No newsletter section repetition found.", schema_gap=bool(missing_tables or missing_columns)),
    }


def format_newsletter_section_repetition_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def _body(row: dict[str, Any]) -> str:
    return clean(row.get("section_body") or row.get("body") or row.get("content") or row.get("text"))


def _norm(text: str) -> str:
    return " ".join(clean(text).lower().split())


def _section_id(row: dict[str, Any]) -> str:
    return clean(row.get("section_id") or row.get("id") or row.get("position"))


def _similarity(a: str, b: str) -> float:
    if not clean(a) or not clean(b):
        return 0.0
    if _norm(a) == _norm(b):
        return 1.0
    return jaccard(tokens(a), tokens(b))
