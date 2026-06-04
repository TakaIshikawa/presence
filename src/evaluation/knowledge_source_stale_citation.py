"""Flag stale knowledge-source citations for freshness-sensitive topics."""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from ._batch_report_common import clean, dt, empty_state, json_dumps, now_value, positive

ARTIFACT_TYPE = "knowledge_source_stale_citation"
DEFAULT_LIMIT = 50
DEFAULT_MAX_ALLOWED_AGE_DAYS = 180
FRESH_TOPICS = {"news", "pricing", "legal", "regulation", "api", "software", "security", "market", "medical", "financial"}


def build_knowledge_source_stale_citation_report(rows: list[dict[str, Any]], *, max_allowed_age_days: int = DEFAULT_MAX_ALLOWED_AGE_DAYS, limit: int = DEFAULT_LIMIT, now: Any = None) -> dict[str, Any]:
    positive("max_allowed_age_days", max_allowed_age_days)
    positive("limit", limit)
    gen = now_value(now)
    findings = []
    for row in rows:
        topic = clean(row.get("topic") or row.get("topic_label")).lower()
        sensitive = topic in FRESH_TOPICS or clean(row.get("freshness_sensitive")).lower() in {"1", "true", "yes"}
        source_date = dt(row.get("source_date") or row.get("published_at") or row.get("created_at"))
        if not sensitive or source_date is None:
            continue
        age_days = max(0, (gen.date() - source_date.date()).days)
        allowed = int(row.get("max_allowed_age_days") or max_allowed_age_days)
        if age_days > allowed:
            findings.append({"content_id": clean(row.get("content_id")), "source_id": clean(row.get("source_id") or row.get("id")), "source_date": source_date.date().isoformat(), "age_days": age_days, "topic": topic, "max_allowed_age_days": allowed})
    findings.sort(key=lambda f: (-f["age_days"], f["content_id"], f["source_id"]))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "summary": {"citation_count": len(rows), "finding_count": len(findings)}, "findings": findings[:limit], "empty_state": empty_state(findings, "No stale freshness-sensitive citations found.")}


def format_knowledge_source_stale_citation_json(report: dict[str, Any]) -> str:
    return json_dumps(report)
