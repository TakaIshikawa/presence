"""Flag generation batches whose candidates are too similar."""
from __future__ import annotations

from itertools import combinations
from typing import Any

from ._batch_report_common import bounded_share, clean, empty_state, jaccard, json_dumps, now_value, positive, tokens

ARTIFACT_TYPE = "pipeline_candidate_novelty_spread"
DEFAULT_LIMIT = 50
DEFAULT_SIMILARITY_THRESHOLD = 0.85


def build_pipeline_candidate_novelty_spread_report(rows: list[dict[str, Any]], *, similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD, limit: int = DEFAULT_LIMIT, now: Any = None) -> dict[str, Any]:
    bounded_share("similarity_threshold", similarity_threshold)
    positive("limit", limit)
    by_batch: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_batch.setdefault(clean(row.get("batch_id") or row.get("source_item_id"), "unknown"), []).append(row)
    findings = []
    for batch_id, items in by_batch.items():
        if len(items) < 2:
            continue
        sims = []
        low_pairs = []
        for left, right in combinations(items, 2):
            sim = _similarity(clean(left.get("candidate_text") or left.get("text")), clean(right.get("candidate_text") or right.get("text")))
            sims.append(sim)
            if sim >= similarity_threshold:
                low_pairs.append([clean(left.get("candidate_id") or left.get("id")), clean(right.get("candidate_id") or right.get("id"))])
        avg = round(sum(sims) / len(sims), 4)
        mx = max(sims)
        if low_pairs and avg >= similarity_threshold:
            findings.append({"batch_id": batch_id, "candidate_count": len(items), "average_similarity": avg, "max_similarity": mx, "low_novelty_pair_ids": low_pairs})
    findings.sort(key=lambda f: (-f["average_similarity"], f["batch_id"]))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_value(now).isoformat(), "filters": {"similarity_threshold": similarity_threshold, "limit": limit}, "summary": {"batch_count": len(by_batch), "finding_count": len(findings)}, "findings": findings[:limit], "empty_state": empty_state(findings, "No low-novelty candidate batches found.")}


def format_pipeline_candidate_novelty_spread_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if " ".join(a.lower().split()) == " ".join(b.lower().split()):
        return 1.0
    return jaccard(tokens(a), tokens(b))
